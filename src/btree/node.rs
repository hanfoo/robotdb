/// B-Tree 节点编解码
///
/// 节点页面 payload 布局（内部节点与叶子节点共用头部）：
///
/// ```text
/// Header (9 bytes):
///   [0]      node_type: u8  (0=internal, 1=leaf)
///   [1..3]   num_keys: u16
///   [3..7]   parent_id: u32
///   [7..9]   free_space_offset: u16  (从 payload 末尾向前增长)
///
/// Cell Pointer Array (2 bytes * num_keys):
///   每个指针指向 payload 中 cell 的起始偏移（相对于 payload 起始）
///
/// Cell 区域（从 payload 末尾向前增长）：
///   内部节点 cell: [key_len:u16][key:bytes][child_id:u32]
///   叶子节点 cell: [key_len:u16][key:bytes][val_len:u16][val:bytes]
///
/// 最右子指针（仅内部节点，存储在 header 后 4 字节）：
///   [9..13]  rightmost_child: u32
/// ```

use crate::error::{Error, Result};
use crate::storage::PAGE_SIZE;

pub const NODE_HEADER_SIZE: usize = 9;
pub const PAYLOAD_SIZE: usize = PAGE_SIZE - 9; // page payload 大小

/// 节点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

/// B-Tree 节点（内存表示）
#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub node_type: NodeType,
    /// 键列表（已排序）
    pub keys: Vec<Vec<u8>>,
    /// 子节点页 ID（内部节点：len = keys.len() + 1）
    pub children: Vec<u32>,
    /// 值列表（叶子节点：len = keys.len()）
    pub values: Vec<Vec<u8>>,
    /// 父节点页 ID
    pub parent_id: u32,
    /// 叶子链表：下一个叶子页 ID（仅叶子节点）
    pub next_leaf: u32,
}

impl BTreeNode {
    /// 创建空内部节点
    pub fn new_internal() -> Self {
        Self {
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            parent_id: u32::MAX,
            next_leaf: u32::MAX,
        }
    }

    /// 创建空叶子节点
    pub fn new_leaf() -> Self {
        Self {
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            parent_id: u32::MAX,
            next_leaf: u32::MAX,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.node_type == NodeType::Leaf
    }

    /// 估算序列化后的字节大小
    pub fn serialized_size(&self) -> usize {
        let mut size = 13; // header(9) + next_leaf/rightmost(4)
        size += self.keys.len() * 2; // cell pointer array
        for k in &self.keys {
            size += 2 + k.len(); // key_len + key
        }
        if self.is_leaf() {
            for v in &self.values {
                size += 2 + v.len(); // val_len + val
            }
        } else {
            size += self.children.len() * 4; // child page ids
        }
        size
    }

    /// 判断节点是否需要分裂（超过页面容量的 80%）
    pub fn needs_split(&self) -> bool {
        self.serialized_size() > (PAYLOAD_SIZE * 4 / 5)
    }

    /// 序列化节点到字节缓冲区
    pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
        let n = self.keys.len();
        buf[0] = self.node_type as u8;
        buf[1..3].copy_from_slice(&(n as u16).to_le_bytes());
        buf[3..7].copy_from_slice(&self.parent_id.to_le_bytes());

        // next_leaf / rightmost_child 存在 [9..13]
        let extra = if self.is_leaf() {
            self.next_leaf
        } else {
            *self.children.last().unwrap_or(&u32::MAX)
        };
        buf[9..13].copy_from_slice(&extra.to_le_bytes());

        // cell pointer array 起始偏移
        let ptr_base = 13usize;
        let ptr_end = ptr_base + n * 2;

        // cell 区域从 payload 末尾向前增长
        let mut cell_end = PAYLOAD_SIZE;
        let mut ptrs = Vec::with_capacity(n);

        for i in 0..n {
            let key = &self.keys[i];
            let cell_size = if self.is_leaf() {
                2 + key.len() + 2 + self.values[i].len()
            } else {
                2 + key.len() + 4
            };

            if cell_end < ptr_end + cell_size {
                return Err(Error::NodeOverflow);
            }
            cell_end -= cell_size;
            ptrs.push(cell_end as u16);

            let c = &mut buf[cell_end..cell_end + cell_size];
            let kl = key.len() as u16;
            c[0..2].copy_from_slice(&kl.to_le_bytes());
            c[2..2 + key.len()].copy_from_slice(key);
            let after_key = 2 + key.len();

            if self.is_leaf() {
                let val = &self.values[i];
                let vl = val.len() as u16;
                c[after_key..after_key + 2].copy_from_slice(&vl.to_le_bytes());
                c[after_key + 2..after_key + 2 + val.len()].copy_from_slice(val);
            } else {
                let child = self.children[i];
                c[after_key..after_key + 4].copy_from_slice(&child.to_le_bytes());
            }
        }

        // 写入 cell pointer array
        for (i, &ptr) in ptrs.iter().enumerate() {
            let off = ptr_base + i * 2;
            buf[off..off + 2].copy_from_slice(&ptr.to_le_bytes());
        }

        // 写入 free_space_offset
        buf[7..9].copy_from_slice(&(cell_end as u16).to_le_bytes());

        Ok(())
    }

    /// 从字节缓冲区反序列化节点
    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 13 {
            return Err(Error::CorruptPage("Buffer too small".into()));
        }
        let node_type = match buf[0] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            v => {
                return Err(Error::CorruptPage(format!("Unknown node type: {}", v)));
            }
        };
        let n = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;
        let parent_id = u32::from_le_bytes(buf[3..7].try_into().unwrap());
        let extra = u32::from_le_bytes(buf[9..13].try_into().unwrap());

        let ptr_base = 13usize;
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        let mut children = Vec::with_capacity(n + 1);

        for i in 0..n {
            let off = ptr_base + i * 2;
            if off + 2 > buf.len() {
                return Err(Error::CorruptPage("Cell pointer out of bounds".into()));
            }
            let cell_off = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap()) as usize;

            if cell_off + 2 > buf.len() {
                return Err(Error::CorruptPage("Cell offset out of bounds".into()));
            }
            let kl = u16::from_le_bytes(buf[cell_off..cell_off + 2].try_into().unwrap()) as usize;
            let key_start = cell_off + 2;
            if key_start + kl > buf.len() {
                return Err(Error::CorruptPage("Key out of bounds".into()));
            }
            keys.push(buf[key_start..key_start + kl].to_vec());

            let after_key = key_start + kl;
            if node_type == NodeType::Leaf {
                if after_key + 2 > buf.len() {
                    return Err(Error::CorruptPage("Value length out of bounds".into()));
                }
                let vl =
                    u16::from_le_bytes(buf[after_key..after_key + 2].try_into().unwrap()) as usize;
                let val_start = after_key + 2;
                if val_start + vl > buf.len() {
                    return Err(Error::CorruptPage("Value out of bounds".into()));
                }
                values.push(buf[val_start..val_start + vl].to_vec());
            } else {
                if after_key + 4 > buf.len() {
                    return Err(Error::CorruptPage("Child id out of bounds".into()));
                }
                let child =
                    u32::from_le_bytes(buf[after_key..after_key + 4].try_into().unwrap());
                children.push(child);
            }
        }

        let (next_leaf, rightmost) = if node_type == NodeType::Leaf {
            (extra, u32::MAX)
        } else {
            children.push(extra); // rightmost child
            (u32::MAX, extra)
        };
        let _ = rightmost;

        Ok(Self {
            node_type,
            keys,
            children,
            values,
            parent_id,
            next_leaf,
        })
    }
}
