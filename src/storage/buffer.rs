use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::error::{Error, Result};
use super::disk::DiskManager;
use super::page::{Page, PageId, PAGE_SIZE};

// LRU-K 替换策略中的帧描述符
struct Frame {
    page: Page,
    // 最近访问时间戳（逻辑时钟）
    last_access: u64,
}

/// 缓冲池快照：在事务开始时记录每个页面的 before-image
pub struct BufferSnapshot {
    /// page_id -> before-image bytes
    pub before_images: HashMap<PageId, Vec<u8>>,
    /// 快照时已知的所有页面 ID
    pub known_pages: std::collections::HashSet<PageId>,
}

/// 缓冲池管理器
///
/// 采用 LRU 替换策略，维护固定数量的内存帧。
/// 所有页面访问必须经过缓冲池，保证同一时刻每页只有一个内存副本。
pub struct BufferPool {
    /// 帧数组
    frames: Vec<Option<Frame>>,
    /// page_id -> 帧下标
    page_table: HashMap<PageId, usize>,
    /// 空闲帧队列
    free_frames: Vec<usize>,
    /// 逻辑时钟
    clock: u64,
    /// 磁盘管理器
    disk: DiskManager,
}

impl BufferPool {
    /// 创建缓冲池，`capacity` 为最大帧数
    pub fn new(disk: DiskManager, capacity: usize) -> Self {
        let mut frames = Vec::with_capacity(capacity);
        let mut free_frames = Vec::with_capacity(capacity);
        for i in 0..capacity {
            frames.push(None);
            free_frames.push(i);
        }
        Self {
            frames,
            page_table: HashMap::new(),
            free_frames,
            clock: 0,
            disk,
        }
    }

    /// 获取当前逻辑时钟并递增
    fn tick(&mut self) -> u64 {
        let t = self.clock;
        self.clock += 1;
        t
    }

    /// 选择一个可驱逐的帧（LRU）
    fn evict(&mut self) -> Result<usize> {
        let mut lru_frame = None;
        let mut lru_time = u64::MAX;

        for (idx, frame_opt) in self.frames.iter().enumerate() {
            if let Some(frame) = frame_opt {
                if frame.page.pin_count == 0 && frame.last_access < lru_time {
                    lru_time = frame.last_access;
                    lru_frame = Some(idx);
                }
            }
        }

        lru_frame.ok_or(Error::BufferPoolFull)
    }

    /// 将脂帧刷盘并清空帧槽（不把帧索引加入 free_frames）
    fn flush_frame(&mut self, frame_idx: usize) -> Result<()> {
        if let Some(frame) = &mut self.frames[frame_idx] {
            if frame.page.is_dirty {
                frame.page.write_checksum();
                self.disk.write_page(&frame.page)?;
            }
            let pid = frame.page.page_id;
            self.page_table.remove(&pid);
        }
        self.frames[frame_idx] = None;
        Ok(())
    }

    /// 获取一个可用帧（优先使用空闲帧，否则驱逐 LRU 帧）
    /// 注意：驱逐路径不将 victim 帧加入 free_frames，直接返回帧索引供调用方使用
    fn acquire_frame(&mut self) -> Result<usize> {
        if let Some(idx) = self.free_frames.pop() {
            Ok(idx)
        } else {
            let victim = self.evict()?;
            // 仅刷盘并清空帧，不把 victim 加入 free_frames（调用方会直接使用此帧）
            self.flush_frame(victim)?;
            Ok(victim)
        }
    }

    /// 获取页面（从缓冲池或磁盘加载），并钉住（pin）
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut Page> {
        // 命中缓冲池
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let t = self.tick();
            let frame = self.frames[frame_idx].as_mut().unwrap();
            frame.last_access = t;
            frame.page.pin_count += 1;
            return Ok(&mut frame.page);
        }

        // 从磁盘加载
        let page = self.disk.read_page(page_id)?;
        let frame_idx = self.acquire_frame()?;
        let t = self.tick();
        self.frames[frame_idx] = Some(Frame {
            page,
            last_access: t,
        });
        self.frames[frame_idx].as_mut().unwrap().page.pin_count = 1;
        self.page_table.insert(page_id, frame_idx);
        Ok(&mut self.frames[frame_idx].as_mut().unwrap().page)
    }

    /// 分配新页面
    pub fn new_page(&mut self) -> Result<&mut Page> {
        let page_id = self.disk.allocate_page()?;
        let frame_idx = self.acquire_frame()?;
        let t = self.tick();
        let mut page = Page::new(page_id);
        page.pin_count = 1;
        self.frames[frame_idx] = Some(Frame {
            page,
            last_access: t,
        });
        self.page_table.insert(page_id, frame_idx);
        Ok(&mut self.frames[frame_idx].as_mut().unwrap().page)
    }

    /// 释放页面钉住（unpin），`is_dirty` 标记是否被修改
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_idx = *self
            .page_table
            .get(&page_id)
            .ok_or(Error::PageNotInBuffer(page_id))?;
        let frame = self.frames[frame_idx].as_mut().unwrap();
        if frame.page.pin_count == 0 {
            return Err(Error::Internal(format!(
                "Unpin page {} with pin_count=0",
                page_id
            )));
        }
        frame.page.pin_count -= 1;
        if is_dirty {
            frame.page.is_dirty = true;
        }
        Ok(())
    }

    /// 将指定页面强制刷盘
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let frame = self.frames[frame_idx].as_mut().unwrap();
            if frame.page.is_dirty {
                frame.page.write_checksum();
                self.disk.write_page(&frame.page)?;
                frame.page.is_dirty = false;
            }
        }
        Ok(())
    }

    /// 将所有脏页刷盘
    pub fn flush_all(&mut self) -> Result<()> {
        let page_ids: Vec<PageId> = self.page_table.keys().copied().collect();
        for pid in page_ids {
            self.flush_page(pid)?;
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Write dirty pages to the OS buffer without fsync.
    /// Equivalent to SQLite's synchronous=NORMAL: data may be lost on OS crash
    /// but not on process crash.
    pub fn flush_dirty(&mut self) -> Result<()> {
        for frame_opt in &mut self.frames {
            if let Some(frame) = frame_opt {
                if frame.page.is_dirty {
                    frame.page.write_checksum();
                    self.disk.write_page(&frame.page)?;
                    frame.page.is_dirty = false;
                }
            }
        }
        Ok(())
    }

    /// Write dirty pages to the OS buffer and then fsync the data file.
    /// Used for synchronous=FULL mode on auto-commit DML.
    pub fn flush_dirty_and_sync(&mut self) -> Result<()> {
        self.flush_dirty()?;
        self.disk.sync()?;
        Ok(())
    }

    /// 直接访问磁盘管理器（用于头页读写）
    pub fn disk_mut(&mut self) -> &mut DiskManager {
        &mut self.disk
    }

    /// 更新磁盘头中的 schema_root 字段，确保 Catalog 位置在重启后可以找到
    pub fn update_schema_root(&mut self, catalog_page: super::page::PageId) -> Result<()> {
        use super::page::HEADER_PAGE_ID;
        // Read the current header
        let mut header = self.disk.read_header().unwrap_or_else(|_| super::page::DbHeader::new());
        if header.schema_root == catalog_page {
            // Nothing to update
            return Ok(());
        }
        header.schema_root = catalog_page;
        header.change_counter += 1;
        header.page_count = self.disk.num_pages();
        self.disk.write_header(&header)?;
        // Also invalidate the header page in the buffer pool so it gets reloaded fresh
        if let Some(&frame_idx) = self.page_table.get(&HEADER_PAGE_ID) {
            if let Some(frame) = &mut self.frames[frame_idx] {
                frame.page.is_dirty = false; // already written directly
            }
        }
        Ok(())
    }

    pub fn num_disk_pages(&self) -> u32 {
        self.disk.num_pages()
    }

    /// Zero-copy access to page data in the buffer pool.
    /// Returns a reference to the raw page data without pin/unpin overhead.
    /// Used by `log_dirty_pages` to avoid re-fetching pages.
    pub fn get_page_data(&self, page_id: PageId) -> Option<&[u8; PAGE_SIZE]> {
        self.page_table.get(&page_id).and_then(|&frame_idx| {
            self.frames[frame_idx].as_ref().map(|frame| frame.page.data.as_ref())
        })
    }

    /// 拍摄当前缓冲池状态，保存所有已加载页面的 before-image
    pub fn take_snapshot(&mut self) -> BufferSnapshot {
        let mut before_images = HashMap::new();
        let mut known_pages = std::collections::HashSet::new();
        for frame_opt in &self.frames {
            if let Some(frame) = frame_opt {
                let pid = frame.page.page_id;
                known_pages.insert(pid);
                // 保存当前页面内容作为 before-image
                before_images.insert(pid, frame.page.data.to_vec());
            }
        }
        BufferSnapshot { before_images, known_pages }
    }

    /// 将缓冲池恢复到快照状态（用于事务回滚）
    pub fn rollback_to_snapshot(&mut self, snapshot: &BufferSnapshot) -> Result<()> {
        // 1. 将快照中已知页面恢复为 before-image
        for (pid, before_image) in &snapshot.before_images {
            if let Some(&frame_idx) = self.page_table.get(pid) {
                if let Some(frame) = &mut self.frames[frame_idx] {
                    frame.page.data.copy_from_slice(before_image);
                    frame.page.is_dirty = false;
                }
            }
            // 如果页面已被驱逐出缓冲池，则不需要处理（磁盘上的数据未被写入）
        }

        // 2. 移除快照后新分配的页面（即事务期间新建的页面）
        let new_pages: Vec<PageId> = self.page_table.keys()
            .copied()
            .filter(|pid| !snapshot.known_pages.contains(pid))
            .collect();
        for pid in new_pages {
            if let Some(&frame_idx) = self.page_table.get(&pid) {
                // 直接清除帧，不刷盘（这些页面是事务期间创建的，应该被丢弃）
                self.frames[frame_idx] = None;
                self.free_frames.push(frame_idx);
                self.page_table.remove(&pid);
            }
        }

        Ok(())
    }
}

/// 线程安全的缓冲池包装
pub type SharedBufferPool = Arc<Mutex<BufferPool>>;

pub fn new_shared_buffer_pool(disk: DiskManager, capacity: usize) -> SharedBufferPool {
    Arc::new(Mutex::new(BufferPool::new(disk, capacity)))
}
