/// 故障注入框架（Fault Injection Framework）
///
/// 用于模拟真实生产环境中的各类磁盘和系统故障，以测试数据库的
/// 崩溃恢复能力、数据完整性保障和错误处理正确性。
///
/// # 支持的故障类型
///
/// - **WriteFailure**：模拟磁盘写入失败（返回 I/O 错误）
/// - **TornWrite**：模拟部分写入（只写入页面的一部分，模拟断电场景）
/// - **BitFlip**：模拟磁盘位翻转（静默数据损坏）
/// - **ReadFailure**：模拟磁盘读取失败
/// - **SyncFailure**：模拟 fsync 失败（数据未持久化到磁盘）
///
/// # 使用方式
///
/// ```rust
/// use robotdb::storage::fault::{FaultInjector, FaultConfig, FaultMode};
///
/// let injector = FaultInjector::new();
/// // 设置在第 5 次写入时触发写入失败
/// injector.set_write_failure_after(5);
/// // 将 injector 传入 DiskManager
/// ```

use std::sync::{Arc, Mutex};
use crate::error::{Error, Result};
use crate::storage::PageId;

/// 故障触发模式
#[derive(Debug, Clone, PartialEq)]
pub enum FaultMode {
    /// 不注入故障（正常运行）
    None,
    /// 在第 N 次写操作后触发写入失败
    WriteFailureAfter(u64),
    /// 在第 N 次写操作后触发部分写入（torn write）
    /// 参数：触发后第几次写操作，以及写入的字节比例（0.0~1.0）
    TornWriteAfter { after: u64, fraction: f64 },
    /// 在第 N 次读操作后触发读取失败
    ReadFailureAfter(u64),
    /// 在第 N 次 sync 后触发 sync 失败
    SyncFailureAfter(u64),
    /// 随机位翻转：每次写入后以指定概率翻转一个随机位
    RandomBitFlip { probability: f64 },
}

/// 故障注入配置（可在运行时动态修改）
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// 当前故障模式
    pub mode: FaultMode,
    /// 已执行的写操作计数
    pub write_count: u64,
    /// 已执行的读操作计数
    pub read_count: u64,
    /// 已执行的 sync 操作计数
    pub sync_count: u64,
    /// 是否记录所有 I/O 操作（用于调试）
    pub trace_io: bool,
    /// I/O 操作日志
    pub io_log: Vec<IoEvent>,
    /// 随机数种子（用于确定性测试）
    pub rng_state: u64,
    /// 特定页面的故障：只对指定页面 ID 注入故障
    pub target_pages: Option<Vec<PageId>>,
}

/// I/O 事件记录
#[derive(Debug, Clone)]
pub struct IoEvent {
    pub event_type: IoEventType,
    pub page_id: PageId,
    pub write_count: u64,
    pub injected_fault: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IoEventType {
    Read,
    Write,
    Sync,
    Allocate,
}

impl FaultConfig {
    pub fn new() -> Self {
        Self {
            mode: FaultMode::None,
            write_count: 0,
            read_count: 0,
            sync_count: 0,
            trace_io: false,
            io_log: Vec::new(),
            rng_state: 12345,
            target_pages: None,
        }
    }

    /// 简单 LCG 随机数生成（确定性，无需外部依赖）
    fn next_rand(&mut self) -> f64 {
        self.rng_state = self.rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.rng_state >> 11) as f64 / (1u64 << 53) as f64
    }

    fn next_rand_usize(&mut self, max: usize) -> usize {
        self.rng_state = self.rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.rng_state as usize) % max
    }

    /// 检查当前写操作是否应该注入故障
    /// 返回：None=正常，Some(FaultAction)=注入故障
    pub fn check_write(&mut self, page_id: PageId, data: &mut Vec<u8>) -> WriteAction {
        // 检查是否只对特定页面注入
        if let Some(ref pages) = self.target_pages {
            if !pages.contains(&page_id) {
                self.write_count += 1;
                return WriteAction::Normal;
            }
        }

        self.write_count += 1;
        let wc = self.write_count;

        if self.trace_io {
            self.io_log.push(IoEvent {
                event_type: IoEventType::Write,
                page_id,
                write_count: wc,
                injected_fault: None,
            });
        }

        match self.mode.clone() {
            FaultMode::None => WriteAction::Normal,

            FaultMode::WriteFailureAfter(after) => {
                if wc > after {
                    if self.trace_io {
                        if let Some(last) = self.io_log.last_mut() {
                            last.injected_fault = Some(format!("WriteFailure at wc={}", wc));
                        }
                    }
                    WriteAction::Fail
                } else {
                    WriteAction::Normal
                }
            }

            FaultMode::TornWriteAfter { after, fraction } => {
                if wc > after {
                    let write_bytes = ((data.len() as f64) * fraction) as usize;
                    let write_bytes = write_bytes.max(1).min(data.len());
                    if self.trace_io {
                        if let Some(last) = self.io_log.last_mut() {
                            last.injected_fault = Some(format!(
                                "TornWrite at wc={}: writing {}/{} bytes",
                                wc, write_bytes, data.len()
                            ));
                        }
                    }
                    WriteAction::Partial(write_bytes)
                } else {
                    WriteAction::Normal
                }
            }

            FaultMode::RandomBitFlip { probability } => {
                let r = self.next_rand();
                if r < probability {
                    let bit_pos = self.next_rand_usize(data.len() * 8);
                    let byte_idx = bit_pos / 8;
                    let bit_idx = bit_pos % 8;
                    data[byte_idx] ^= 1 << bit_idx;
                    if self.trace_io {
                        if let Some(last) = self.io_log.last_mut() {
                            last.injected_fault = Some(format!(
                                "BitFlip at byte={} bit={}", byte_idx, bit_idx
                            ));
                        }
                    }
                    WriteAction::BitFlipped
                } else {
                    WriteAction::Normal
                }
            }

            FaultMode::ReadFailureAfter(_) | FaultMode::SyncFailureAfter(_) => {
                WriteAction::Normal
            }
        }
    }

    /// 检查当前读操作是否应该注入故障
    pub fn check_read(&mut self, page_id: PageId) -> bool {
        if let Some(ref pages) = self.target_pages {
            if !pages.contains(&page_id) {
                self.read_count += 1;
                return false; // 不注入
            }
        }

        self.read_count += 1;
        let rc = self.read_count;

        if self.trace_io {
            self.io_log.push(IoEvent {
                event_type: IoEventType::Read,
                page_id,
                write_count: self.write_count,
                injected_fault: None,
            });
        }

        match &self.mode {
            FaultMode::ReadFailureAfter(after) => rc > *after,
            _ => false,
        }
    }

    /// 检查当前 sync 操作是否应该注入故障
    pub fn check_sync(&mut self) -> bool {
        self.sync_count += 1;
        let sc = self.sync_count;

        match &self.mode {
            FaultMode::SyncFailureAfter(after) => sc > *after,
            _ => false,
        }
    }

    /// 重置所有计数器（保留故障配置）
    pub fn reset_counters(&mut self) {
        self.write_count = 0;
        self.read_count = 0;
        self.sync_count = 0;
        self.io_log.clear();
    }

    /// 禁用故障注入
    pub fn disable(&mut self) {
        self.mode = FaultMode::None;
    }
}

/// 写操作的处理结果
#[derive(Debug, PartialEq)]
pub enum WriteAction {
    /// 正常写入
    Normal,
    /// 写入失败（返回 I/O 错误）
    Fail,
    /// 部分写入（只写 N 字节）
    Partial(usize),
    /// 数据已被翻转位（正常写入损坏的数据）
    BitFlipped,
}

/// 故障注入器（线程安全的共享句柄）
#[derive(Clone, Debug)]
pub struct FaultInjector {
    inner: Arc<Mutex<FaultConfig>>,
}

impl FaultInjector {
    /// 创建一个新的故障注入器（默认无故障）
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FaultConfig::new())),
        }
    }

    /// 设置在第 N 次写操作后触发写入失败
    pub fn set_write_failure_after(&self, n: u64) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.mode = FaultMode::WriteFailureAfter(n);
        cfg.reset_counters();
    }

    /// 设置在第 N 次写操作后触发部分写入
    pub fn set_torn_write_after(&self, n: u64, fraction: f64) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.mode = FaultMode::TornWriteAfter { after: n, fraction };
        cfg.reset_counters();
    }

    /// 设置随机位翻转（每次写入以 probability 概率翻转）
    pub fn set_random_bit_flip(&self, probability: f64) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.mode = FaultMode::RandomBitFlip { probability };
        cfg.reset_counters();
    }

    /// 设置在第 N 次读操作后触发读取失败
    pub fn set_read_failure_after(&self, n: u64) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.mode = FaultMode::ReadFailureAfter(n);
        cfg.reset_counters();
    }

    /// 设置在第 N 次 sync 后触发 sync 失败
    pub fn set_sync_failure_after(&self, n: u64) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.mode = FaultMode::SyncFailureAfter(n);
        cfg.reset_counters();
    }

    /// 限制只对特定页面注入故障
    pub fn set_target_pages(&self, pages: Vec<PageId>) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.target_pages = Some(pages);
    }

    /// 清除页面限制（对所有页面注入）
    pub fn clear_target_pages(&self) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.target_pages = None;
    }

    /// 启用 I/O 追踪日志
    pub fn enable_trace(&self) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.trace_io = true;
    }

    /// 禁用故障注入
    pub fn disable(&self) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.disable();
    }

    /// 重置计数器
    pub fn reset(&self) {
        let mut cfg = self.inner.lock().unwrap();
        cfg.reset_counters();
    }

    /// 获取当前写操作计数
    pub fn write_count(&self) -> u64 {
        self.inner.lock().unwrap().write_count
    }

    /// 获取 I/O 日志快照
    pub fn get_io_log(&self) -> Vec<IoEvent> {
        self.inner.lock().unwrap().io_log.clone()
    }

    /// 检查写操作（内部使用）
    pub(crate) fn check_write(&self, page_id: PageId, data: &mut Vec<u8>) -> WriteAction {
        self.inner.lock().unwrap().check_write(page_id, data)
    }

    /// 检查读操作（内部使用）
    pub(crate) fn check_read(&self, page_id: PageId) -> bool {
        self.inner.lock().unwrap().check_read(page_id)
    }

    /// 检查 sync 操作（内部使用）
    pub(crate) fn check_sync(&self) -> bool {
        self.inner.lock().unwrap().check_sync()
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

/// 带故障注入能力的磁盘管理器包装器
///
/// 包装原始 DiskManager，在每次 I/O 操作前检查是否需要注入故障。
/// 这种设计保持了原始 DiskManager 的不变性，故障注入完全在测试层面控制。
pub struct FaultDiskManager {
    inner: crate::storage::DiskManager,
    pub injector: FaultInjector,
}

impl FaultDiskManager {
    pub fn new(disk: crate::storage::DiskManager) -> Self {
        Self {
            inner: disk,
            injector: FaultInjector::new(),
        }
    }

    pub fn with_injector(disk: crate::storage::DiskManager, injector: FaultInjector) -> Self {
        Self { inner: disk, injector }
    }

    /// 读取页面（带故障注入）
    pub fn read_page(&mut self, page_id: PageId) -> Result<crate::storage::Page> {
        if self.injector.check_read(page_id) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Injected read failure for page {}", page_id),
            )));
        }
        self.inner.read_page(page_id)
    }

    /// 写入页面（带故障注入）
    pub fn write_page(&mut self, page: &crate::storage::Page) -> Result<()> {
        let page_id = page.page_id;
        let mut data = page.as_bytes().to_vec();

        match self.injector.check_write(page_id, &mut data) {
            WriteAction::Normal | WriteAction::BitFlipped => {
                // 正常写入（BitFlipped 时写入已损坏的数据）
                self.inner.write_raw_bytes(page_id, &data)
            }
            WriteAction::Fail => {
                Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Injected write failure for page {}", page_id),
                )))
            }
            WriteAction::Partial(bytes) => {
                // 部分写入：只写入前 bytes 字节，然后返回成功
                // 这模拟了断电时的 torn write 场景
                self.inner.write_raw_bytes(page_id, &data[..bytes])
            }
        }
    }

    /// 分配新页面
    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.inner.allocate_page()
    }

    /// sync（带故障注入）
    pub fn sync(&mut self) -> Result<()> {
        if self.injector.check_sync() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Injected sync failure",
            )));
        }
        self.inner.sync()
    }

    /// 读取数据库头部
    pub fn read_header(&mut self) -> Result<crate::storage::DbHeader> {
        self.inner.read_header()
    }

    /// 写入数据库头部
    pub fn write_header(&mut self, header: &crate::storage::DbHeader) -> Result<()> {
        self.inner.write_header(header)
    }

    pub fn num_pages(&self) -> u32 {
        self.inner.num_pages()
    }

    pub fn db_path(&self) -> &str {
        &self.inner.db_path
    }

    /// 获取内部 DiskManager 的可变引用（用于直接访问）
    pub fn inner_mut(&mut self) -> &mut crate::storage::DiskManager {
        &mut self.inner
    }
}

/// 崩溃模拟器
///
/// 通过在特定时间点强制截断数据库文件来模拟崩溃场景。
/// 截断后，数据库应该能够通过 WAL 恢复到一致状态。
pub struct CrashSimulator;

impl CrashSimulator {
    /// 模拟在写入 N 个页面后发生崩溃
    ///
    /// 通过截断数据库文件到指定字节数来模拟崩溃。
    /// WAL 文件保持完整，用于崩溃恢复。
    pub fn crash_after_pages(db_path: &std::path::Path, pages_written: u32) -> Result<()> {
        use std::fs::OpenOptions;
        let file = OpenOptions::new().write(true).open(db_path)?;
        let truncate_size = pages_written as u64 * crate::storage::PAGE_SIZE as u64;
        file.set_len(truncate_size)?;
        Ok(())
    }

    /// 模拟在写入 N 字节后发生崩溃（部分页面写入）
    pub fn crash_at_byte(db_path: &std::path::Path, byte_offset: u64) -> Result<()> {
        use std::fs::OpenOptions;
        let file = OpenOptions::new().write(true).open(db_path)?;
        file.set_len(byte_offset)?;
        Ok(())
    }

    /// 损坏 WAL 文件的最后 N 字节（模拟 WAL 写入中断）
    pub fn corrupt_wal_tail(wal_path: &std::path::Path, corrupt_bytes: u64) -> Result<()> {
        use std::fs::OpenOptions;
        let file = OpenOptions::new().write(true).open(wal_path)?;
        let metadata = file.metadata()?;
        let current_size = metadata.len();
        if current_size > corrupt_bytes {
            file.set_len(current_size - corrupt_bytes)?;
        }
        Ok(())
    }

    /// 用随机数据覆盖数据库文件的指定页面（模拟磁盘损坏）
    pub fn corrupt_page(db_path: &std::path::Path, page_id: PageId, seed: u64) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        use std::fs::OpenOptions;
        let mut file = OpenOptions::new().write(true).open(db_path)?;
        let offset = page_id as u64 * crate::storage::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        // 生成确定性随机数据
        let mut state = seed;
        let mut corrupt_data = vec![0u8; crate::storage::PAGE_SIZE];
        for byte in corrupt_data.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            *byte = (state >> 56) as u8;
        }
        file.write_all(&corrupt_data)?;
        Ok(())
    }
}
