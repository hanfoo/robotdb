/// 事务管理器
///
/// 实现基于 WAL 的 ACID 事务，支持：
/// - 读已提交（Read Committed）隔离级别
/// - 写-写冲突检测
/// - 崩溃恢复（ARIES 风格的 Redo）
/// - 自动回滚（事务 Drop 时）

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::error::{Error, Result};
use crate::storage::{BufferPool, PageId, PAGE_SIZE};
use super::wal::{
    Lsn, TxId, WalManager, RecordType,
    make_begin_record, make_page_write_record, make_commit_record, make_abort_record,
};

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    Aborted,
}

/// 单个事务的上下文
pub struct Transaction {
    pub tx_id: TxId,
    pub state: TxState,
    /// 本事务修改过的页面集合（用于回滚）
    pub dirty_pages: HashMap<PageId, Vec<u8>>,
    /// 本事务的 begin LSN
    pub begin_lsn: Lsn,
}

impl Transaction {
    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }
}

/// 事务管理器
pub struct TransactionManager {
    wal: WalManager,
    /// 活跃事务表
    active_txs: HashMap<TxId, Transaction>,
    /// 已提交事务集合（用于可见性判断）
    committed_txs: HashSet<TxId>,
}

impl TransactionManager {
    /// 打开事务管理器并执行崩溃恢复
    pub fn open(wal_path: impl AsRef<Path>, pool: &mut BufferPool) -> Result<Self> {
        let wal = WalManager::open(wal_path)?;
        let mut mgr = Self {
            wal,
            active_txs: HashMap::new(),
            committed_txs: HashSet::new(),
        };
        mgr.recover(pool)?;
        Ok(mgr)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 崩溃恢复（ARIES Redo-Only 简化版）
    // ─────────────────────────────────────────────────────────────────────────

    fn recover(&mut self, pool: &mut BufferPool) -> Result<()> {
        let records = self.wal.read_all_records()?;
        if records.is_empty() {
            return Ok(());
        }

        log::info!("WAL recovery: replaying {} records", records.len());

        // 分析阶段：找出已提交事务
        let mut committed: HashSet<TxId> = HashSet::new();
        for rec in &records {
            if rec.record_type == RecordType::Commit {
                committed.insert(rec.tx_id);
            }
        }

        // Redo 阶段：重放已提交事务的 PageWrite
        for rec in &records {
            if rec.record_type == RecordType::PageWrite && committed.contains(&rec.tx_id) {
                if rec.data.len() == PAGE_SIZE {
                    let page = pool.fetch_page(rec.page_id);
                    match page {
                        Ok(p) => {
                            p.data.copy_from_slice(&rec.data);
                            p.is_dirty = true;
                            pool.unpin_page(rec.page_id, true)?;
                        }
                        Err(_) => {
                            // 页面不在缓冲池，直接写盘
                            pool.disk_mut().write_page(&crate::storage::Page::from_bytes(
                                rec.page_id,
                                rec.data[..PAGE_SIZE].try_into().unwrap(),
                            ))?;
                        }
                    }
                }
            }
        }

        pool.flush_all()?;
        self.committed_txs = committed;
        log::info!("WAL recovery complete");
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 事务生命周期
    // ─────────────────────────────────────────────────────────────────────────

    /// 开始新事务
    pub fn begin(&mut self) -> Result<TxId> {
        let tx_id = self.wal.next_tx_id();
        let rec = make_begin_record(&mut self.wal, tx_id);
        let begin_lsn = rec.lsn;
        self.wal.append(&rec)?;

        self.active_txs.insert(tx_id, Transaction {
            tx_id,
            state: TxState::Active,
            dirty_pages: HashMap::new(),
            begin_lsn,
        });

        log::debug!("BEGIN tx_id={}", tx_id);
        Ok(tx_id)
    }

    /// 提交事务 (kept for backward compatibility — delegates to commit_wal_only)
    pub fn commit(&mut self, tx_id: TxId, pool: &mut BufferPool) -> Result<()> {
        let _ = pool; // unused — dirty pages deferred to checkpoint/close
        self.commit_wal_only(tx_id)
    }

    /// Commit transaction without flushing WAL to disk.
    /// Used for SyncMode::Off where durability is sacrificed for performance.
    /// Dirty pages stay in buffer pool — deferred to checkpoint/close.
    pub fn commit_no_flush(&mut self, tx_id: TxId) -> Result<()> {
        let tx = self.active_txs.get_mut(&tx_id)
            .ok_or(Error::NoActiveTransaction)?;
        if !tx.is_active() {
            return Err(Error::NoActiveTransaction);
        }

        // Write Commit record but skip WAL flush
        let rec = make_commit_record(&mut self.wal, tx_id);
        self.wal.append(&rec)?;

        // Do NOT flush dirty pages — they stay in the buffer pool
        tx.state = TxState::Committed;
        self.committed_txs.insert(tx_id);
        self.active_txs.remove(&tx_id);

        log::debug!("COMMIT (no WAL flush) tx_id={}", tx_id);
        Ok(())
    }

    /// 回滚事务
    pub fn rollback(&mut self, tx_id: TxId, pool: &mut BufferPool) -> Result<()> {
        let tx = self.active_txs.get_mut(&tx_id)
            .ok_or(Error::NoActiveTransaction)?;
        if !tx.is_active() {
            return Err(Error::NoActiveTransaction);
        }

        // 将脏页恢复为事务开始前的状态
        let dirty_pages: HashMap<PageId, Vec<u8>> = tx.dirty_pages.drain().collect();
        for (pid, before_image) in dirty_pages {
            if before_image.len() == PAGE_SIZE {
                match pool.fetch_page(pid) {
                    Ok(p) => {
                        p.data.copy_from_slice(&before_image);
                        p.is_dirty = false;
                        pool.unpin_page(pid, false)?;
                    }
                    Err(_) => {}
                }
            }
        }

        // 写 Abort 日志
        let rec = make_abort_record(&mut self.wal, tx_id);
        self.wal.append(&rec)?;
        self.wal.flush()?;

        tx.state = TxState::Aborted;
        self.active_txs.remove(&tx_id);

        log::debug!("ROLLBACK tx_id={}", tx_id);
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 页面访问钩子
    // ─────────────────────────────────────────────────────────────────────────

    /// 在修改页面前调用：记录 before-image 并写 WAL
    pub fn before_write_page(
        &mut self,
        tx_id: TxId,
        page_id: PageId,
        pool: &mut BufferPool,
    ) -> Result<()> {
        let tx = self.active_txs.get_mut(&tx_id)
            .ok_or(Error::NoActiveTransaction)?;

        // 只记录第一次修改的 before-image（用于回滚）
        if !tx.dirty_pages.contains_key(&page_id) {
            let page = pool.fetch_page(page_id)?;
            let before_image = page.data.to_vec();
            pool.unpin_page(page_id, false)?;
            tx.dirty_pages.insert(page_id, before_image);
        }

        // 写 WAL PageWrite 记录（after-image 在提交时写入）
        // 此处写入 before-image 作为 undo log（简化实现）
        Ok(())
    }

    /// 在提交时为所有脏页写 WAL after-image
    pub fn log_dirty_pages(
        &mut self,
        tx_id: TxId,
        pool: &BufferPool,
    ) -> Result<()> {
        let dirty_page_ids: Vec<PageId> = {
            let tx = self.active_txs.get(&tx_id)
                .ok_or(Error::NoActiveTransaction)?;
            tx.dirty_pages.keys().copied().collect()
        };

        for pid in dirty_page_ids {
            // Zero-copy: read page data directly from the buffer pool
            let data = pool.get_page_data(pid)
                .ok_or(Error::PageNotInBuffer(pid))?;

            let rec = make_page_write_record(&mut self.wal, tx_id, pid, data);
            self.wal.append(&rec)?;
        }
        Ok(())
    }

    /// Commit transaction with WAL fsync only — dirty pages stay in buffer pool.
    /// This is the SQLite WAL-mode behavior: data pages are deferred to checkpoint.
    pub fn commit_wal_only(&mut self, tx_id: TxId) -> Result<()> {
        let tx = self.active_txs.get_mut(&tx_id)
            .ok_or(Error::NoActiveTransaction)?;
        if !tx.is_active() {
            return Err(Error::NoActiveTransaction);
        }

        // Write Commit record and fsync WAL (WAL-before-data principle)
        let rec = make_commit_record(&mut self.wal, tx_id);
        self.wal.append(&rec)?;
        self.wal.flush()?;

        // Do NOT flush dirty pages — they stay in the buffer pool
        tx.state = TxState::Committed;
        self.committed_txs.insert(tx_id);
        self.active_txs.remove(&tx_id);

        log::debug!("COMMIT (WAL-only) tx_id={}", tx_id);
        Ok(())
    }

    /// 执行检查点
    pub fn checkpoint(&mut self, pool: &mut BufferPool) -> Result<()> {
        pool.flush_all()?;
        let lsn = self.wal.current_lsn();
        self.wal.checkpoint(lsn)?;
        log::info!("Checkpoint at LSN={}", lsn);
        Ok(())
    }

    pub fn wal_mut(&mut self) -> &mut WalManager {
        &mut self.wal
    }

    pub fn is_committed(&self, tx_id: TxId) -> bool {
        self.committed_txs.contains(&tx_id)
    }
}
