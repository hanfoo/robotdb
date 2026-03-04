/// WAL（Write-Ahead Log）实现
///
/// WAL 是保证数据库 ACID 特性的核心机制。每次写操作先写日志，
/// 再修改数据页面，崩溃恢复时通过重放日志恢复一致性状态。
///
/// WAL 文件格式：
/// ```text
/// [WAL Header: 32 bytes]
/// [Record 1][Record 2]...[Record N]
/// ```
///
/// WAL Record 格式：
/// ```text
/// [lsn: u64][tx_id: u64][record_type: u8][page_id: u32]
/// [data_len: u32][data: bytes][checksum: u32]
/// ```

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::storage::{PageId, PAGE_SIZE};

/// 日志序列号（Log Sequence Number）
pub type Lsn = u64;

/// 事务 ID
pub type TxId = u64;

/// WAL 记录类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// 事务开始
    Begin = 1,
    /// 页面写入（包含完整页面数据）
    PageWrite = 2,
    /// 事务提交
    Commit = 3,
    /// 事务回滚
    Abort = 4,
    /// 检查点
    Checkpoint = 5,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Begin),
            2 => Some(Self::PageWrite),
            3 => Some(Self::Commit),
            4 => Some(Self::Abort),
            5 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// WAL 日志记录
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub tx_id: TxId,
    pub record_type: RecordType,
    pub page_id: PageId,
    /// 对于 PageWrite：包含完整页面数据（PAGE_SIZE 字节）
    /// 对于其他类型：空
    pub data: Vec<u8>,
}

impl WalRecord {
    const HEADER_SIZE: usize = 8 + 8 + 1 + 4 + 4; // lsn+tx_id+type+page_id+data_len = 25
    const CHECKSUM_SIZE: usize = 4;

    pub fn serialized_size(&self) -> usize {
        Self::HEADER_SIZE + self.data.len() + Self::CHECKSUM_SIZE
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        // Reserve space upfront to avoid reallocation
        buf.reserve(self.serialized_size());
        let start = buf.len();
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.extend_from_slice(&self.tx_id.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&self.page_id.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);
        // Compute checksum over the header+data bytes just written (excluding data_len)
        // Header layout in buf: lsn(8) + tx_id(8) + type(1) + page_id(4) = 21 bytes,
        // then data_len(4), then data. Checksum covers the 21-byte header + data only.
        let checksum = Self::compute_checksum_from_parts(
            &buf[start..start + 21], // lsn + tx_id + type + page_id
            &self.data,
        );
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    fn compute_checksum_from_parts(header: &[u8], data: &[u8]) -> u32 {
        use xxhash_rust::xxh3::xxh3_64;
        // Use a stack-local buffer for small data to avoid heap allocation
        if data.len() <= 256 {
            let mut tmp = [0u8; 277]; // 21 header + 256 data
            tmp[..header.len()].copy_from_slice(header);
            tmp[header.len()..header.len() + data.len()].copy_from_slice(data);
            xxh3_64(&tmp[..header.len() + data.len()]) as u32
        } else {
            let mut hasher_input = Vec::with_capacity(header.len() + data.len());
            hasher_input.extend_from_slice(header);
            hasher_input.extend_from_slice(data);
            xxh3_64(&hasher_input) as u32
        }
    }

    fn compute_checksum(&self) -> u32 {
        // Build the same header bytes used by compute_checksum_from_parts
        let mut header = [0u8; 21];
        header[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        header[8..16].copy_from_slice(&self.tx_id.to_le_bytes());
        header[16] = self.record_type as u8;
        header[17..21].copy_from_slice(&self.page_id.to_le_bytes());
        Self::compute_checksum_from_parts(&header, &self.data)
    }

    pub fn deserialize(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < Self::HEADER_SIZE + Self::CHECKSUM_SIZE {
            return Err(Error::WalError("Record too short".into()));
        }
        let lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let tx_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let record_type = RecordType::from_u8(buf[16])
            .ok_or_else(|| Error::WalError(format!("Unknown record type: {}", buf[16])))?;
        let page_id = u32::from_le_bytes(buf[17..21].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[21..25].try_into().unwrap()) as usize;

        let total = Self::HEADER_SIZE + data_len + Self::CHECKSUM_SIZE;
        if buf.len() < total {
            return Err(Error::WalError("Incomplete record".into()));
        }

        let data = buf[Self::HEADER_SIZE..Self::HEADER_SIZE + data_len].to_vec();
        let stored_checksum = u32::from_le_bytes(
            buf[Self::HEADER_SIZE + data_len..total].try_into().unwrap(),
        );

        let record = Self { lsn, tx_id, record_type, page_id, data };
        if record.compute_checksum() != stored_checksum {
            return Err(Error::WalChecksumMismatch);
        }

        Ok((record, total))
    }
}

/// WAL 文件头
const WAL_MAGIC: &[u8; 8] = b"RUSTWAL1";

struct WalHeader {
    magic: [u8; 8],
    /// 最新检查点 LSN
    checkpoint_lsn: Lsn,
    /// 下一个可用 LSN
    next_lsn: Lsn,
    /// 下一个可用 TxId
    next_tx_id: TxId,
}

impl WalHeader {
    const SIZE: usize = 8 + 8 + 8 + 8; // 32 bytes

    fn new() -> Self {
        Self {
            magic: *WAL_MAGIC,
            checkpoint_lsn: 0,
            next_lsn: 1,
            next_tx_id: 1,
        }
    }

    fn serialize(&self, buf: &mut [u8]) {
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..16].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[16..24].copy_from_slice(&self.next_lsn.to_le_bytes());
        buf[24..32].copy_from_slice(&self.next_tx_id.to_le_bytes());
    }

    fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if &magic != WAL_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            checkpoint_lsn: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            next_lsn: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            next_tx_id: u64::from_le_bytes(buf[24..32].try_into().ok()?),
        })
    }
}

/// WAL 管理器
pub struct WalManager {
    file: File,
    header: WalHeader,
    /// 内存写缓冲区（批量写入提升性能）
    write_buffer: Vec<u8>,
    /// 写缓冲区大小阈值（超过则自动刷盘）
    buffer_size: usize,
}

impl WalManager {
    /// 打开或创建 WAL 文件
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        let metadata = file.metadata()?;
        let header = if metadata.len() == 0 {
            // 新建 WAL 文件
            let h = WalHeader::new();
            let mut buf = [0u8; WalHeader::SIZE];
            h.serialize(&mut buf);
            file.write_all(&buf)?;
            file.sync_all()?;
            h
        } else {
            // 读取已有头部
            let mut buf = [0u8; WalHeader::SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;
            WalHeader::deserialize(&buf)
                .ok_or_else(|| Error::WalError("Invalid WAL magic".into()))?
        };

        Ok(Self {
            file,
            header,
            write_buffer: Vec::with_capacity(64 * 1024),
            buffer_size: 64 * 1024,
        })
    }

    /// 分配新的 LSN
    pub fn next_lsn(&mut self) -> Lsn {
        let lsn = self.header.next_lsn;
        self.header.next_lsn += 1;
        lsn
    }

    /// 分配新的事务 ID
    pub fn next_tx_id(&mut self) -> TxId {
        let id = self.header.next_tx_id;
        self.header.next_tx_id += 1;
        id
    }

    /// 追加一条日志记录
    pub fn append(&mut self, record: &WalRecord) -> Result<Lsn> {
        record.serialize(&mut self.write_buffer);
        if self.write_buffer.len() >= self.buffer_size {
            self.flush()?;
        }
        Ok(record.lsn)
    }

    /// 将缓冲区刷入磁盘（group commit）
    pub fn flush(&mut self) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&self.write_buffer)?;
        self.file.sync_data()?;
        self.write_buffer.clear();
        // 更新头部
        self.write_header()?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        let mut buf = [0u8; WalHeader::SIZE];
        self.header.serialize(&mut buf);
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&buf)?;
        Ok(())
    }

    /// 读取所有日志记录（用于崩溃恢复）
    pub fn read_all_records(&mut self) -> Result<Vec<WalRecord>> {
        self.file.seek(SeekFrom::Start(WalHeader::SIZE as u64))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;

        let mut records = Vec::new();
        let mut offset = 0;
        while offset < buf.len() {
            match WalRecord::deserialize(&buf[offset..]) {
                Ok((record, size)) => {
                    offset += size;
                    records.push(record);
                }
                Err(Error::WalError(_)) | Err(Error::WalChecksumMismatch) => {
                    // 遇到损坏记录则停止（可能是未完成的写入）
                    log::warn!("WAL: stopping replay at offset {} due to corrupt record", offset);
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(records)
    }

    /// 执行检查点：将已提交事务的脏页刷盘后截断 WAL
    pub fn checkpoint(&mut self, checkpoint_lsn: Lsn) -> Result<()> {
        self.flush()?;
        self.header.checkpoint_lsn = checkpoint_lsn;
        // 写入检查点记录
        let lsn = self.next_lsn();
        let record = WalRecord {
            lsn,
            tx_id: 0,
            record_type: RecordType::Checkpoint,
            page_id: 0,
            data: Vec::new(),
        };
        record.serialize(&mut self.write_buffer);
        self.flush()?;
        Ok(())
    }

    pub fn current_lsn(&self) -> Lsn {
        self.header.next_lsn.saturating_sub(1)
    }
}

/// WAL 辅助：构建 Begin 记录
pub fn make_begin_record(wal: &mut WalManager, tx_id: TxId) -> WalRecord {
    WalRecord {
        lsn: wal.next_lsn(),
        tx_id,
        record_type: RecordType::Begin,
        page_id: 0,
        data: Vec::new(),
    }
}

/// WAL 辅助：构建 PageWrite 记录
pub fn make_page_write_record(
    wal: &mut WalManager,
    tx_id: TxId,
    page_id: PageId,
    page_data: &[u8; PAGE_SIZE],
) -> WalRecord {
    WalRecord {
        lsn: wal.next_lsn(),
        tx_id,
        record_type: RecordType::PageWrite,
        page_id,
        data: page_data.to_vec(),
    }
}

/// WAL 辅助：构建 Commit 记录
pub fn make_commit_record(wal: &mut WalManager, tx_id: TxId) -> WalRecord {
    WalRecord {
        lsn: wal.next_lsn(),
        tx_id,
        record_type: RecordType::Commit,
        page_id: 0,
        data: Vec::new(),
    }
}

/// WAL 辅助：构建 Abort 记录
pub fn make_abort_record(wal: &mut WalManager, tx_id: TxId) -> WalRecord {
    WalRecord {
        lsn: wal.next_lsn(),
        tx_id,
        record_type: RecordType::Abort,
        page_id: 0,
        data: Vec::new(),
    }
}
