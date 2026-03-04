use std::fs::{File, OpenOptions};
#[cfg(not(unix))]
use std::io::Read;
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::{Error, Result};
use super::page::{DbHeader, Page, PageId, PAGE_SIZE, HEADER_PAGE_ID};

/// 磁盘管理器：负责页面的物理读写
///
/// 文件布局：页面按顺序连续存储，每页固定 PAGE_SIZE 字节。
/// 第 0 页为数据库头页，后续页面从偏移 PAGE_SIZE 开始。
pub struct DiskManager {
    file: File,
    pub db_path: String,
    /// 当前文件中已分配的页面总数
    pub num_pages: u32,
    /// File capacity in pages (may be > num_pages due to pre-allocation)
    file_capacity: u32,
}

impl DiskManager {
    /// 打开或创建数据库文件
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        let num_pages = if file_size == 0 {
            0
        } else {
            (file_size / PAGE_SIZE) as u32
        };

        Ok(Self {
            file,
            db_path: path_str,
            file_capacity: num_pages,
            num_pages,
        })
    }

    /// 读取指定页面
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id >= self.num_pages {
            return Err(Error::InvalidPageId(page_id));
        }
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        #[cfg(unix)]
        {
            self.file.read_exact_at(&mut buf, offset)?;
        }
        #[cfg(not(unix))]
        {
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut buf)?;
        }
        Ok(Page::from_bytes(page_id, &buf))
    }

    /// 将页面写入磁盘
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let page_id = page.page_id;
        let offset = page_id as u64 * PAGE_SIZE as u64;
        #[cfg(unix)]
        {
            self.file.write_all_at(page.as_bytes(), offset)?;
        }
        #[cfg(not(unix))]
        {
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(page.as_bytes())?;
        }
        if page_id >= self.num_pages {
            self.num_pages = page_id + 1;
        }
        Ok(())
    }

    /// 分配一个新页面（扩展文件）
    ///
    /// Uses ftruncate to batch-extend the file by PREALLOC_CHUNK pages at a time,
    /// reducing the number of syscalls from ~N to ~N/32 for sequential allocations.
    /// The OS fills extended space with zeros, and BufferPool::new_page()
    /// creates a zeroed Page in memory, so we never read the on-disk zeros.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        const PREALLOC_CHUNK: u32 = 32;

        let page_id = self.num_pages;
        self.num_pages += 1;

        // Only call set_len when the logical page count exceeds file capacity.
        // Pre-allocate in chunks to reduce ftruncate syscalls.
        if self.num_pages > self.file_capacity {
            let target = self.file_capacity + PREALLOC_CHUNK;
            let new_size = target as u64 * PAGE_SIZE as u64;
            self.file.set_len(new_size)?;
            self.file_capacity = target;
        }
        Ok(page_id)
    }

    /// 强制将文件缓冲区刷入磁盘
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// 读取数据库头部
    pub fn read_header(&mut self) -> Result<DbHeader> {
        if self.num_pages == 0 {
            return Err(Error::CorruptDatabase("Empty database file".into()));
        }
        let page = self.read_page(HEADER_PAGE_ID)?;
        DbHeader::deserialize(page.payload())
            .ok_or_else(|| Error::CorruptDatabase("Invalid magic number in header".into()))
    }

    /// 写入数据库头部
    pub fn write_header(&mut self, header: &DbHeader) -> Result<()> {
        let mut page = if self.num_pages == 0 {
            let p = Page::new(HEADER_PAGE_ID);
            self.num_pages = 1;
            p
        } else {
            self.read_page(HEADER_PAGE_ID)?
        };

        use super::page::PageType;
        page.set_page_type(PageType::Header);
        let payload = page.payload_mut();
        header.serialize(payload);
        page.write_checksum();
        self.write_page(&page)?;
        Ok(())
    }

    /// 初始化一个新数据库文件
    pub fn initialize(&mut self) -> Result<DbHeader> {
        let header = DbHeader::new();
        self.write_header(&header)?;
        self.sync()?;
        Ok(header)
    }

    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }

    /// 写入原始字节到指定页面偏移处（专为故障注入使用）
    ///
    /// 与 `write_page` 不同，此方法允许写入任意字节数量，用于模拟部分写入场景。
    pub fn write_raw_bytes(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        if page_id >= self.num_pages {
            self.num_pages = page_id + 1;
        }
        Ok(())
    }

    /// 获取文件大小（字节）
    pub fn file_size(&mut self) -> Result<u64> {
        Ok(self.file.seek(SeekFrom::End(0))?)
    }

    /// 截断文件到指定大小（用于崩溃模拟）
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.file.set_len(size)?;
        self.num_pages = (size / PAGE_SIZE as u64) as u32;
        Ok(())
    }
}
