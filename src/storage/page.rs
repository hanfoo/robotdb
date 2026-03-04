/// 数据库页面大小（4 KB，与 SQLite 默认一致）
pub const PAGE_SIZE: usize = 4096;

/// 页面 ID 类型
pub type PageId = u32;

/// 无效页面 ID 哨兵值
pub const INVALID_PAGE_ID: PageId = u32::MAX;

/// 数据库文件头部所在页面
pub const HEADER_PAGE_ID: PageId = 0;

/// 页面类型标识
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// 文件头页（第 0 页）
    Header = 0,
    /// B-Tree 内部节点
    BTreeInternal = 1,
    /// B-Tree 叶子节点
    BTreeLeaf = 2,
    /// 溢出页（存储超大 BLOB/TEXT）
    Overflow = 3,
    /// 空闲列表页
    FreeList = 4,
}

impl PageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Header),
            1 => Some(Self::BTreeInternal),
            2 => Some(Self::BTreeLeaf),
            3 => Some(Self::Overflow),
            4 => Some(Self::FreeList),
            _ => None,
        }
    }
}

/// 内存中的页面表示
///
/// 页面布局（4096 字节）：
/// ```text
/// [0..1]   page_type (u8)
/// [1..5]   page_id   (u32 LE)
/// [5..9]   checksum  (u32 LE, CRC32 of [9..PAGE_SIZE])
/// [9..PAGE_SIZE] payload
/// ```
#[derive(Clone)]
pub struct Page {
    pub data: Box<[u8; PAGE_SIZE]>,
    pub page_id: PageId,
    pub is_dirty: bool,
    pub pin_count: u32,
}

impl Page {
    /// 创建一个全零的新页面
    pub fn new(page_id: PageId) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        // 写入 page_id
        data[1..5].copy_from_slice(&page_id.to_le_bytes());
        Self {
            data,
            page_id,
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// 从磁盘原始字节创建页面
    pub fn from_bytes(page_id: PageId, bytes: &[u8; PAGE_SIZE]) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        data.copy_from_slice(bytes);
        Self {
            data,
            page_id,
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// 获取页面类型
    pub fn page_type(&self) -> Option<PageType> {
        PageType::from_u8(self.data[0])
    }

    /// 设置页面类型
    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[0] = pt as u8;
        self.is_dirty = true;
    }

    /// 获取 payload 区域（可变）
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.is_dirty = true;
        &mut self.data[9..]
    }

    /// 获取 payload 区域（只读）
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.data[9..]
    }

    /// 计算并写入校验和（xxh3 低 32 位）
    pub fn write_checksum(&mut self) {
        use xxhash_rust::xxh3::xxh3_64;
        let hash = xxh3_64(&self.data[9..]) as u32;
        self.data[5..9].copy_from_slice(&hash.to_le_bytes());
    }

    /// 验证校验和
    pub fn verify_checksum(&self) -> bool {
        use xxhash_rust::xxh3::xxh3_64;
        let stored = u32::from_le_bytes(self.data[5..9].try_into().unwrap());
        let computed = xxh3_64(&self.data[9..]) as u32;
        stored == computed
    }

    /// 将页面序列化为字节数组（用于写盘）
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("page_id", &self.page_id)
            .field("page_type", &self.page_type())
            .field("is_dirty", &self.is_dirty)
            .field("pin_count", &self.pin_count)
            .finish()
    }
}

/// 数据库文件头（存储在第 0 页 payload 区域）
#[derive(Debug, Clone)]
pub struct DbHeader {
    /// 魔数："ROBOTDB01"
    pub magic: [u8; 8],
    /// 页面大小
    pub page_size: u16,
    /// 文件格式版本
    pub version: u16,
    /// 数据库总页数
    pub page_count: u32,
    /// 空闲列表首页
    pub free_list_head: PageId,
    /// 空闲页数量
    pub free_page_count: u32,
    /// Schema 根页（存储表定义的 B-Tree 根）
    pub schema_root: PageId,
    /// 文件变更计数（每次写事务提交 +1）
    pub change_counter: u64,
}

impl DbHeader {
    pub const MAGIC: &'static [u8; 8] = b"ROBOTDB01";
    pub const SIZE: usize = 8 + 2 + 2 + 4 + 4 + 4 + 4 + 8; // = 36 bytes

    pub fn new() -> Self {
        Self {
            magic: *Self::MAGIC,
            page_size: PAGE_SIZE as u16,
            version: 1,
            page_count: 1,
            free_list_head: INVALID_PAGE_ID,
            free_page_count: 0,
            schema_root: INVALID_PAGE_ID,
            change_counter: 0,
        }
    }

    /// 序列化到字节切片
    pub fn serialize(&self, buf: &mut [u8]) {
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..10].copy_from_slice(&self.page_size.to_le_bytes());
        buf[10..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        buf[16..20].copy_from_slice(&self.free_list_head.to_le_bytes());
        buf[20..24].copy_from_slice(&self.free_page_count.to_le_bytes());
        buf[24..28].copy_from_slice(&self.schema_root.to_le_bytes());
        buf[28..36].copy_from_slice(&self.change_counter.to_le_bytes());
    }

    /// 从字节切片反序列化
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if &magic != Self::MAGIC {
            return None;
        }
        Some(Self {
            magic,
            page_size: u16::from_le_bytes(buf[8..10].try_into().ok()?),
            version: u16::from_le_bytes(buf[10..12].try_into().ok()?),
            page_count: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            free_list_head: u32::from_le_bytes(buf[16..20].try_into().ok()?),
            free_page_count: u32::from_le_bytes(buf[20..24].try_into().ok()?),
            schema_root: u32::from_le_bytes(buf[24..28].try_into().ok()?),
            change_counter: u64::from_le_bytes(buf[28..36].try_into().ok()?),
        })
    }
}
