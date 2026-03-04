use std::cmp::Ordering;

use crate::error::{Error, Result};
use crate::storage::{BufferPool, PageId, PageType, INVALID_PAGE_ID};
use super::node::BTreeNode;

/// Result of trying the leaf hint path in insert_with_hint.
enum HintResult {
    /// Successfully inserted in the hinted leaf
    Inserted,
    /// Key already exists
    DuplicateKey,
    /// Leaf is too full, needs split
    NeedsSplit,
    /// Key doesn't belong to this leaf
    WrongLeaf,
    /// Hint is stale (page is no longer a leaf)
    StaleHint,
}

impl From<Result<bool>> for HintResult {
    fn from(r: Result<bool>) -> Self {
        match r {
            Ok(true) => HintResult::Inserted,
            Ok(false) => HintResult::NeedsSplit,
            Err(_) => HintResult::DuplicateKey,
        }
    }
}

/// B+ Tree 实现
///
/// 每棵 B+ Tree 对应一个根页面，支持变长键值对存储。
/// 叶子节点通过 next_leaf 指针形成有序链表，支持范围扫描。
///
/// 插入算法使用"路径追踪"方式（path-tracking），在从根到叶的遍历过程中
/// 记录每层的 (page_id, child_index) 对，避免依赖存储在节点中的 parent_id
/// 字段（该字段在多层分裂时容易失效）。
pub struct BTree {
    /// 根节点页 ID
    pub root_id: PageId,
}

impl BTree {
    /// 创建新的 B+ Tree（分配根叶子节点）
    pub fn create(pool: &mut BufferPool) -> Result<Self> {
        let root_page = pool.new_page()?;
        let root_id = root_page.page_id;
        root_page.set_page_type(PageType::BTreeLeaf);

        let root_node = BTreeNode::new_leaf();
        let payload = root_page.payload_mut();
        root_node.serialize(payload)?;
        pool.unpin_page(root_id, true)?;

        Ok(Self { root_id })
    }

    /// 打开已有 B+ Tree
    pub fn open(root_id: PageId) -> Self {
        Self { root_id }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 内部辅助：读写节点
    // ─────────────────────────────────────────────────────────────────────────

    fn read_node(pool: &mut BufferPool, page_id: PageId) -> Result<BTreeNode> {
        let page = pool.fetch_page(page_id)?;
        let node = BTreeNode::deserialize(page.payload())?;
        pool.unpin_page(page_id, false)?;
        Ok(node)
    }

    fn write_node(pool: &mut BufferPool, page_id: PageId, node: &BTreeNode) -> Result<()> {
        let page = pool.fetch_page(page_id)?;
        debug_assert_eq!(page.page_id, page_id, "Buffer pool returned wrong page");
        let payload = page.payload_mut();
        node.serialize(payload)?;
        let pt = if node.is_leaf() {
            PageType::BTreeLeaf
        } else {
            PageType::BTreeInternal
        };
        page.set_page_type(pt);
        pool.unpin_page(page_id, true)?;
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 零拷贝页面内二分查找辅助函数
    // ─────────────────────────────────────────────────────────────────────────

    /// Read the cell offset for cell `idx` from the cell pointer array.
    #[inline(always)]
    fn cell_offset(payload: &[u8], idx: usize) -> usize {
        let ptr_off = 13 + idx * 2;
        u16::from_le_bytes([payload[ptr_off], payload[ptr_off + 1]]) as usize
    }

    /// Return the key slice from a cell at the given offset (works for both internal and leaf cells).
    #[inline(always)]
    fn cell_key(payload: &[u8], cell_off: usize) -> &[u8] {
        let kl = u16::from_le_bytes([payload[cell_off], payload[cell_off + 1]]) as usize;
        &payload[cell_off + 2..cell_off + 2 + kl]
    }

    /// Read the child page ID from an internal cell (stored right after the key bytes).
    #[inline(always)]
    fn internal_cell_child(payload: &[u8], cell_off: usize) -> PageId {
        let kl = u16::from_le_bytes([payload[cell_off], payload[cell_off + 1]]) as usize;
        let child_off = cell_off + 2 + kl;
        u32::from_le_bytes([
            payload[child_off],
            payload[child_off + 1],
            payload[child_off + 2],
            payload[child_off + 3],
        ])
    }

    /// Binary search on an internal node's page payload without deserialization.
    /// Returns the child page ID to follow for the given key.
    fn find_child_in_page(payload: &[u8], key: &[u8]) -> PageId {
        let num_keys = u16::from_le_bytes([payload[1], payload[2]]) as usize;
        let rightmost = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);

        let mut lo = 0usize;
        let mut hi = num_keys;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell_off = Self::cell_offset(payload, mid);
            let cell_key = Self::cell_key(payload, cell_off);
            match cell_key.cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    // Exact match: follow the right child (index = mid + 1)
                    let child_idx = mid + 1;
                    if child_idx < num_keys {
                        return Self::internal_cell_child(
                            payload,
                            Self::cell_offset(payload, child_idx),
                        );
                    } else {
                        return rightmost;
                    }
                }
            }
        }
        // lo is the insertion point; children[lo] is the child to follow
        if lo < num_keys {
            Self::internal_cell_child(payload, Self::cell_offset(payload, lo))
        } else {
            rightmost
        }
    }

    /// Binary search on a leaf node's page payload without deserialization.
    /// Returns Some(value_bytes) if the key is found, None otherwise.
    fn find_value_in_leaf_page(payload: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let num_keys = u16::from_le_bytes([payload[1], payload[2]]) as usize;

        let mut lo = 0usize;
        let mut hi = num_keys;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell_off = Self::cell_offset(payload, mid);
            let cell_key = Self::cell_key(payload, cell_off);
            match cell_key.cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    // Read value: after key_len(2) + key(kl) comes val_len(2) + val
                    let kl =
                        u16::from_le_bytes([payload[cell_off], payload[cell_off + 1]]) as usize;
                    let val_len_off = cell_off + 2 + kl;
                    let vl = u16::from_le_bytes([
                        payload[val_len_off],
                        payload[val_len_off + 1],
                    ]) as usize;
                    let val_start = val_len_off + 2;
                    return Some(payload[val_start..val_start + vl].to_vec());
                }
            }
        }
        None
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 查找
    // ─────────────────────────────────────────────────────────────────────────

    /// 精确查找键，返回对应的值
    pub fn get(&self, pool: &mut BufferPool, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let leaf_id = self.find_leaf(pool, key)?;
        let page = pool.fetch_page(leaf_id)?;
        let result = Self::find_value_in_leaf_page(page.payload(), key);
        pool.unpin_page(leaf_id, false)?;
        Ok(result)
    }

    /// 找到键所在的叶子节点页 ID（仅用于读操作）
    fn find_leaf(&self, pool: &mut BufferPool, key: &[u8]) -> Result<PageId> {
        let mut cur_id = self.root_id;
        loop {
            let page = pool.fetch_page(cur_id)?;
            let payload = page.payload();
            if payload[0] == 1 {
                // Leaf node
                pool.unpin_page(cur_id, false)?;
                return Ok(cur_id);
            }
            let child_id = Self::find_child_in_page(payload, key);
            pool.unpin_page(cur_id, false)?;
            cur_id = child_id;
        }
    }

    /// 从根到叶遍历，记录路径（每层的 page_id），最后一个元素是叶子节点 ID。
    /// 路径用于插入时的分裂传播，避免依赖可能陈旧的 parent_id 字段。
    fn find_leaf_with_path(
        &self,
        pool: &mut BufferPool,
        key: &[u8],
    ) -> Result<(PageId, Vec<PageId>)> {
        let mut path: Vec<PageId> = Vec::with_capacity(8);
        let mut cur_id = self.root_id;
        loop {
            let page = pool.fetch_page(cur_id)?;
            let payload = page.payload();
            if payload[0] == 1 {
                // Leaf node
                pool.unpin_page(cur_id, false)?;
                return Ok((cur_id, path));
            }
            path.push(cur_id);
            let child_id = Self::find_child_in_page(payload, key);
            pool.unpin_page(cur_id, false)?;
            cur_id = child_id;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 零分配叶子节点内就地插入（快速路径）
    // ─────────────────────────────────────────────────────────────────────────

    /// Try to insert a key-value pair directly into the leaf page payload
    /// without deserializing the node. Returns:
    /// - `Ok(true)` if insertion succeeded (fast path)
    /// - `Ok(false)` if the leaf needs a split (caller should fall through to slow path)
    /// - `Err(DuplicateKey)` if the key already exists
    fn try_insert_in_leaf_page(
        payload: &mut [u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        let num_keys = u16::from_le_bytes([payload[1], payload[2]]) as usize;
        let free_space_offset = u16::from_le_bytes([payload[7], payload[8]]) as usize;

        // Cell size: key_len(2) + key + val_len(2) + value
        let cell_size = 2 + key.len() + 2 + value.len();
        // New cell pointer entry: 2 bytes
        let ptr_array_end = 13 + (num_keys + 1) * 2;

        // Check if there's enough space
        if free_space_offset < cell_size || free_space_offset - cell_size < ptr_array_end {
            return Ok(false);
        }

        // Check 80% threshold: estimate total serialized size after insertion
        // total_size = 13 (header) + (num_keys+1)*2 (ptrs) + existing_cells + cell_size
        let existing_cells_size = super::node::PAYLOAD_SIZE - free_space_offset;
        let total_size = 13 + (num_keys + 1) * 2 + existing_cells_size + cell_size;
        if total_size > super::node::PAYLOAD_SIZE * 4 / 5 {
            return Ok(false);
        }

        // Binary search for insertion position
        let mut lo = 0usize;
        let mut hi = num_keys;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell_off = Self::cell_offset(payload, mid);
            let cell_key = Self::cell_key(payload, cell_off);
            match cell_key.cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Err(Error::DuplicateKey),
            }
        }
        let pos = lo;

        // Write the new cell at free_space_offset - cell_size
        let cell_off = free_space_offset - cell_size;
        let kl = key.len() as u16;
        let vl = value.len() as u16;
        payload[cell_off..cell_off + 2].copy_from_slice(&kl.to_le_bytes());
        payload[cell_off + 2..cell_off + 2 + key.len()].copy_from_slice(key);
        let after_key = cell_off + 2 + key.len();
        payload[after_key..after_key + 2].copy_from_slice(&vl.to_le_bytes());
        payload[after_key + 2..after_key + 2 + value.len()].copy_from_slice(value);

        // Shift cell pointers right by 2 bytes to make room at position `pos`
        let ptr_base = 13;
        // Shift from pos..num_keys to pos+1..num_keys+1
        if pos < num_keys {
            let src_start = ptr_base + pos * 2;
            let src_end = ptr_base + num_keys * 2;
            // copy_within handles overlapping regions
            payload.copy_within(src_start..src_end, src_start + 2);
        }

        // Write the new cell pointer at position `pos`
        let ptr_off = ptr_base + pos * 2;
        payload[ptr_off..ptr_off + 2].copy_from_slice(&(cell_off as u16).to_le_bytes());

        // Update header: num_keys += 1, free_space_offset = cell_off
        let new_num_keys = (num_keys + 1) as u16;
        payload[1..3].copy_from_slice(&new_num_keys.to_le_bytes());
        payload[7..9].copy_from_slice(&(cell_off as u16).to_le_bytes());

        Ok(true)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 插入（路径追踪版，彻底修复多层分裂时 parent_id 陈旧的问题）
    // ─────────────────────────────────────────────────────────────────────────

    /// 插入键值对（若键已存在则返回 DuplicateKey 错误）
    /// Accepts slices to avoid forcing callers to allocate Vecs.
    /// Only clones into Vec on the rare slow (split) path.
    pub fn insert(&mut self, pool: &mut BufferPool, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.insert_with_hint(pool, key, value, &mut None)
    }

    /// Insert with an optional leaf page hint to skip find_leaf on sequential inserts.
    /// The hint is updated on success so subsequent calls can reuse it.
    pub fn insert_with_hint(
        &mut self,
        pool: &mut BufferPool,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        leaf_hint: &mut Option<PageId>,
    ) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        // Try the hinted leaf first (avoids find_leaf for sequential inserts)
        if let Some(hint_id) = *leaf_hint {
            // Determine if the key belongs to the hinted leaf by reading page metadata.
            // We must release the page borrow before calling unpin_page.
            let hint_result = {
                let page = pool.fetch_page(hint_id)?;
                let payload = page.payload_mut();
                if payload[0] != 1 {
                    // Not a leaf anymore — stale hint
                    HintResult::StaleHint
                } else {
                    let next_leaf = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);
                    let num_keys = u16::from_le_bytes([payload[1], payload[2]]) as usize;

                    // Check if key is before the first key (belongs to earlier leaf)
                    if num_keys > 0 {
                        let first_cell_off = Self::cell_offset(payload, 0);
                        let first_key = Self::cell_key(payload, first_cell_off);
                        if key < first_key {
                            HintResult::WrongLeaf
                        } else if next_leaf != INVALID_PAGE_ID {
                            let last_cell_off = Self::cell_offset(payload, num_keys - 1);
                            let last_key = Self::cell_key(payload, last_cell_off);
                            if key > last_key {
                                // Key past end of this leaf and there's a next leaf
                                HintResult::WrongLeaf
                            } else {
                                Self::try_insert_in_leaf_page(payload, key, value)
                                    .into()
                            }
                        } else {
                            // Rightmost leaf — key definitely belongs here
                            Self::try_insert_in_leaf_page(payload, key, value)
                                .into()
                        }
                    } else {
                        // Empty leaf — key belongs here
                        Self::try_insert_in_leaf_page(payload, key, value)
                            .into()
                    }
                }
            };
            // page borrow is released here

            match hint_result {
                HintResult::Inserted => {
                    pool.unpin_page(hint_id, true)?;
                    return Ok(());
                }
                HintResult::DuplicateKey => {
                    pool.unpin_page(hint_id, false)?;
                    return Err(Error::DuplicateKey);
                }
                HintResult::NeedsSplit => {
                    pool.unpin_page(hint_id, false)?;
                    *leaf_hint = None;
                    // Fall through to slow path
                }
                HintResult::WrongLeaf | HintResult::StaleHint => {
                    pool.unpin_page(hint_id, false)?;
                    *leaf_hint = None;
                    // Fall through to standard find_leaf path
                }
            }
        }

        // Standard path: find_leaf + try in-place insert
        let leaf_id = self.find_leaf(pool, key)?;
        let page = pool.fetch_page(leaf_id)?;
        let payload = page.payload_mut();
        match Self::try_insert_in_leaf_page(payload, key, value) {
            Ok(true) => {
                pool.unpin_page(leaf_id, true)?;
                *leaf_hint = Some(leaf_id);
                return Ok(());
            }
            Err(e) => {
                pool.unpin_page(leaf_id, false)?;
                return Err(e);
            }
            Ok(false) => {
                pool.unpin_page(leaf_id, false)?;
            }
        }

        // Slow path: full deserialization + split (allocates Vecs for key/value)
        let (leaf_id, path) = self.find_leaf_with_path(pool, key)?;
        let mut leaf = Self::read_node(pool, leaf_id)?;

        // 检查重复键
        if leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)).is_ok() {
            return Err(Error::DuplicateKey);
        }

        // 插入到叶子节点
        let pos = leaf
            .keys
            .binary_search_by(|k| k.as_slice().cmp(key))
            .unwrap_or_else(|i| i);
        leaf.keys.insert(pos, key.to_vec());
        leaf.values.insert(pos, value.to_vec());

        if !leaf.needs_split() {
            Self::write_node(pool, leaf_id, &leaf)?;
            *leaf_hint = Some(leaf_id);
            return Ok(());
        }

        // 叶子需要分裂，沿路径向上传播
        let push_up = self.split_leaf_node(pool, leaf_id, leaf)?;
        self.propagate_split(pool, push_up, path)?;
        // After split, the new right leaf is likely where the next key goes
        // Clear hint — next insert will re-find
        *leaf_hint = None;
        Ok(())
    }

    /// 插入或更新键值对
    pub fn upsert(&mut self, pool: &mut BufferPool, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (leaf_id, path) = self.find_leaf_with_path(pool, &key)?;
        let mut leaf = Self::read_node(pool, leaf_id)?;

        match leaf.keys.binary_search_by(|k| k.as_slice().cmp(&key)) {
            Ok(idx) => {
                // 更新已有值
                leaf.values[idx] = value;
                Self::write_node(pool, leaf_id, &leaf)?;
            }
            Err(pos) => {
                leaf.keys.insert(pos, key);
                leaf.values.insert(pos, value);
                if !leaf.needs_split() {
                    Self::write_node(pool, leaf_id, &leaf)?;
                } else {
                    let push_up = self.split_leaf_node(pool, leaf_id, leaf)?;
                    self.propagate_split(pool, push_up, path)?;
                }
            }
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 分裂辅助
    // ─────────────────────────────────────────────────────────────────────────

    /// 分裂叶子节点，返回需要向上传播的 (push_up_key, left_id, right_id)
    fn split_leaf_node(
        &mut self,
        pool: &mut BufferPool,
        leaf_id: PageId,
        mut leaf: BTreeNode,
    ) -> Result<(Vec<u8>, PageId, PageId)> {
        let mid = leaf.keys.len() / 2;

        let new_page = pool.new_page()?;
        let new_id = new_page.page_id;
        pool.unpin_page(new_id, false)?;

        let mut new_leaf = BTreeNode::new_leaf();
        new_leaf.keys = leaf.keys.split_off(mid);
        new_leaf.values = leaf.values.split_off(mid);
        new_leaf.next_leaf = leaf.next_leaf;
        leaf.next_leaf = new_id;

        let push_up_key = new_leaf.keys[0].clone();

        Self::write_node(pool, leaf_id, &leaf)?;
        Self::write_node(pool, new_id, &new_leaf)?;

        Ok((push_up_key, leaf_id, new_id))
    }

    /// 分裂内部节点，返回需要向上传播的 (push_up_key, left_id, right_id)
    fn split_internal_node(
        &mut self,
        pool: &mut BufferPool,
        node_id: PageId,
        mut node: BTreeNode,
    ) -> Result<(Vec<u8>, PageId, PageId)> {
        let mid = node.keys.len() / 2;
        let push_up_key = node.keys[mid].clone();

        let new_page = pool.new_page()?;
        let new_id = new_page.page_id;
        pool.unpin_page(new_id, false)?;

        let mut new_node = BTreeNode::new_internal();
        new_node.keys = node.keys.split_off(mid + 1);
        new_node.children = node.children.split_off(mid + 1);
        node.keys.pop(); // 移除中间键（它被提升）

        // 更新新节点子节点的 parent_id（仅用于兼容旧读路径，不影响插入正确性）
        for &child_id in &new_node.children {
            let mut child = Self::read_node(pool, child_id)?;
            child.parent_id = new_id;
            Self::write_node(pool, child_id, &child)?;
        }

        Self::write_node(pool, node_id, &node)?;
        Self::write_node(pool, new_id, &new_node)?;

        Ok((push_up_key, node_id, new_id))
    }

    /// 沿路径向上传播分裂，直到不再需要分裂或到达根节点。
    ///
    /// `path` 是从根到叶的内部节点 ID 列表（不含叶子本身）。
    /// `push_up` 是 (分隔键, 左子 ID, 右子 ID)。
    fn propagate_split(
        &mut self,
        pool: &mut BufferPool,
        mut push_up: (Vec<u8>, PageId, PageId),
        mut path: Vec<PageId>,
    ) -> Result<()> {
        loop {
            let (key, left_id, right_id) = push_up;

            match path.pop() {
                None => {
                    // 已到达根节点，需要创建新根
                    let new_root_page = pool.new_page()?;
                    let new_root_id = new_root_page.page_id;
                    pool.unpin_page(new_root_id, false)?;

                    let mut new_root = BTreeNode::new_internal();
                    new_root.keys.push(key);
                    new_root.children.push(left_id);
                    new_root.children.push(right_id);
                    Self::write_node(pool, new_root_id, &new_root)?;

                    // 更新子节点的 parent_id（兼容性）
                    let mut left = Self::read_node(pool, left_id)?;
                    left.parent_id = new_root_id;
                    Self::write_node(pool, left_id, &left)?;

                    let mut right = Self::read_node(pool, right_id)?;
                    right.parent_id = new_root_id;
                    Self::write_node(pool, right_id, &right)?;

                    self.root_id = new_root_id;
                    return Ok(());
                }
                Some(parent_id) => {
                    // 将分隔键和右子节点插入父节点
                    let mut parent = Self::read_node(pool, parent_id)?;
                    debug_assert!(!parent.is_leaf(), "path contains leaf node id={}", parent_id);
                    debug_assert!(!parent.children.is_empty(),
                        "internal node id={} has 0 children", parent_id);
                    let pos = parent
                        .keys
                        .binary_search_by(|k| k.as_slice().cmp(&key))
                        .unwrap_or_else(|i| i);
                    parent.keys.insert(pos, key);
                    parent.children.insert(pos + 1, right_id);
                    // 更新右子节点的 parent_id（左子节点的 parent_id 已经正确）
                    let mut right_node = Self::read_node(pool, right_id)?;
                    right_node.parent_id = parent_id;
                    Self::write_node(pool, right_id, &right_node)?;
                    if !parent.needs_split() {
                        Self::write_node(pool, parent_id, &parent)?;
                        return Ok(());
                    }

                    // 父节点也需要分裂，继续向上传播
                    push_up = self.split_internal_node(pool, parent_id, parent)?;
                    // path 已经 pop 了 parent_id，继续循环
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 删除
    // ─────────────────────────────────────────────────────────────────────────

    /// 删除键（若不存在则返回 KeyNotFound）
    pub fn delete(&mut self, pool: &mut BufferPool, key: &[u8]) -> Result<()> {
        let leaf_id = self.find_leaf(pool, key)?;
        let mut leaf = Self::read_node(pool, leaf_id)?;

        match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Err(_) => Err(Error::KeyNotFound),
            Ok(idx) => {
                leaf.keys.remove(idx);
                leaf.values.remove(idx);
                Self::write_node(pool, leaf_id, &leaf)?;
                // 简化实现：不做再平衡（生产级实现需要合并下溢节点）
                Ok(())
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 范围扫描
    // ─────────────────────────────────────────────────────────────────────────

    /// 范围扫描：返回 [start_key, end_key) 范围内所有键值对
    pub fn range_scan(
        &self,
        pool: &mut BufferPool,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_leaf_id = if let Some(sk) = start_key {
            self.find_leaf(pool, sk)?
        } else {
            self.leftmost_leaf(pool)?
        };

        let mut result = Vec::new();
        let mut cur_id = start_leaf_id;

        loop {
            if cur_id == INVALID_PAGE_ID {
                break;
            }
            let node = Self::read_node(pool, cur_id)?;
            for (k, v) in node.keys.iter().zip(node.values.iter()) {
                if let Some(sk) = start_key {
                    if k.as_slice() < sk {
                        continue;
                    }
                }
                if let Some(ek) = end_key {
                    if k.as_slice() >= ek {
                        return Ok(result);
                    }
                }
                result.push((k.clone(), v.clone()));
            }
            cur_id = node.next_leaf;
        }

        Ok(result)
    }

    /// 全表扫描（返回所有键值对）
    pub fn scan_all(&self, pool: &mut BufferPool) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_scan(pool, None, None)
    }

    /// Walk the leaf chain and call `f` with a reference to each value's bytes,
    /// reading directly from page payloads without deserializing BTreeNode.
    /// Zero heap allocation in the scan loop itself.
    pub fn for_each_leaf_value<F>(&self, pool: &mut BufferPool, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]),
    {
        let mut cur_id = self.leftmost_leaf(pool)?;
        let payload_end = super::node::PAYLOAD_SIZE;
        while cur_id != INVALID_PAGE_ID {
            let page = pool.fetch_page(cur_id)?;
            let p = page.payload();
            let num_keys = u16::from_le_bytes([p[1], p[2]]) as usize;
            let next_leaf = u32::from_le_bytes([p[9], p[10], p[11], p[12]]);
            // Walk cells contiguously from free_space_offset
            let fso = u16::from_le_bytes([p[7], p[8]]) as usize;
            let mut pos = fso;
            for _ in 0..num_keys {
                if pos + 4 > payload_end { break; }
                let kl = u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
                let vlo = pos + 2 + kl;
                if vlo + 2 > payload_end { break; }
                let vl = u16::from_le_bytes([p[vlo], p[vlo + 1]]) as usize;
                let vs = vlo + 2;
                f(&p[vs..vs + vl]);
                pos = vs + vl;
            }
            pool.unpin_page(cur_id, false)?;
            cur_id = next_leaf;
        }
        Ok(())
    }

    /// Walk the leaf chain and accumulate a numeric aggregate directly from raw page data.
    /// Combines leaf walk + column extraction + numeric accumulation in one tight loop.
    /// `col_idx` is the column index to aggregate.
    /// `skip_bytes`: pre-computed byte offset from row value start to the target column's
    /// length field. Pass 0 to compute dynamically (required if variable-width columns
    /// precede the target).
    /// Returns (sum_int, sum_float, has_float, count).
    pub fn sum_column_raw(
        &self,
        pool: &mut BufferPool,
        col_idx: usize,
        skip_bytes: usize,
    ) -> Result<(i64, f64, bool, usize)> {
        let mut sum_int: i64 = 0;
        let mut sum_float: f64 = 0.0;
        let mut has_float = false;
        let mut count: usize = 0;
        let mut cur_id = self.leftmost_leaf(pool)?;
        while cur_id != INVALID_PAGE_ID {
            let page = pool.fetch_page(cur_id)?;
            let p = page.payload();
            let num_keys = u16::from_le_bytes([p[1], p[2]]) as usize;
            let next_leaf = u32::from_le_bytes([p[9], p[10], p[11], p[12]]);

            // Walk cells contiguously from free_space_offset to end of payload,
            // avoiding the cell pointer array indirection for better cache locality.
            let fso = u16::from_le_bytes([p[7], p[8]]) as usize;
            let payload_end = super::node::PAYLOAD_SIZE;
            let mut cell_pos = fso;
            for _ in 0..num_keys {
                if cell_pos + 4 > payload_end { break; }
                let kl = u16::from_le_bytes([p[cell_pos], p[cell_pos + 1]]) as usize;
                let vlo = cell_pos + 2 + kl;
                if vlo + 2 > payload_end { break; }
                let vl = u16::from_le_bytes([p[vlo], p[vlo + 1]]) as usize;
                let vs = vlo + 2;
                let end = vs + vl;
                cell_pos = end; // advance to next cell

                // Navigate to target column
                let off = if skip_bytes > 0 {
                    vs + skip_bytes
                } else {
                    let mut o = vs + 4; // skip num_cols header
                    for _ in 0..col_idx {
                        if o + 4 > end { break; }
                        let cl = u32::from_le_bytes([p[o], p[o+1], p[o+2], p[o+3]]) as usize;
                        o += 4 + cl;
                    }
                    o
                };

                // Need 4 (col_len) + 1 (tag) + 8 (data) = 13 bytes
                if off + 13 > end { continue; }
                let off = off + 4; // skip col_len
                match p[off] {
                    2 => { // Integer
                        let n = i64::from_le_bytes(p[off+1..off+9].try_into().unwrap());
                        sum_int = sum_int.wrapping_add(n);
                        count += 1;
                    }
                    3 => { // Real
                        let f = f64::from_le_bytes(p[off+1..off+9].try_into().unwrap());
                        sum_float += f;
                        has_float = true;
                        count += 1;
                    }
                    _ => {}
                }
            }
            pool.unpin_page(cur_id, false)?;
            cur_id = next_leaf;
        }
        Ok((sum_int, sum_float, has_float, count))
    }

    /// Fused filter+aggregate on raw page bytes.
    /// Walks the leaf chain, reads the filter column and (optionally) agg column directly,
    /// applies the integer comparison, and accumulates the result.
    ///
    /// `filter_col_idx`: column to compare in WHERE
    /// `filter_skip`: precomputed byte offset to filter column (0 = dynamic skip)
    /// `filter_op`: 0=Gt, 1=Ge, 2=Lt, 3=Le, 4=Eq, 5=NotEq
    /// `filter_lit`: the integer literal to compare against
    /// `agg_col_idx`: column to aggregate (None for COUNT(*))
    /// `agg_skip`: precomputed byte offset to agg column (0 = dynamic skip)
    pub fn count_filtered_raw(
        &self,
        pool: &mut BufferPool,
        filter_col_idx: usize,
        filter_skip: usize,
        filter_op: u8,
        filter_lit: i64,
        agg_col_idx: Option<usize>,
        agg_skip: usize,
    ) -> Result<(i64, f64, bool, usize)> {
        let mut sum_int: i64 = 0;
        let mut sum_float: f64 = 0.0;
        let mut has_float = false;
        let mut count: usize = 0;
        let mut cur_id = self.leftmost_leaf(pool)?;
        let payload_end = super::node::PAYLOAD_SIZE;
        while cur_id != INVALID_PAGE_ID {
            let page = pool.fetch_page(cur_id)?;
            let p = page.payload();
            let num_keys = u16::from_le_bytes([p[1], p[2]]) as usize;
            let next_leaf = u32::from_le_bytes([p[9], p[10], p[11], p[12]]);
            let fso = u16::from_le_bytes([p[7], p[8]]) as usize;
            let mut cell_pos = fso;

            for _ in 0..num_keys {
                if cell_pos + 4 > payload_end { break; }
                let kl = u16::from_le_bytes([p[cell_pos], p[cell_pos + 1]]) as usize;
                let vlo = cell_pos + 2 + kl;
                if vlo + 2 > payload_end { break; }
                let vl = u16::from_le_bytes([p[vlo], p[vlo + 1]]) as usize;
                let vs = vlo + 2;
                let end = vs + vl;
                cell_pos = end;

                // Navigate to filter column
                let foff = if filter_skip > 0 {
                    vs + filter_skip
                } else {
                    let mut o = vs + 4;
                    for _ in 0..filter_col_idx {
                        if o + 4 > end { break; }
                        let cl = u32::from_le_bytes([p[o], p[o+1], p[o+2], p[o+3]]) as usize;
                        o += 4 + cl;
                    }
                    o
                };
                // Read filter value: need 4 (len) + 1 (tag) + 8 (i64) = 13 bytes
                if foff + 13 > end { continue; }
                let ftag = p[foff + 4];
                if ftag != 2 { continue; } // Only integer filter supported
                let fval = i64::from_le_bytes(p[foff+5..foff+13].try_into().unwrap());

                // Apply filter
                let pass = match filter_op {
                    0 => fval > filter_lit,  // Gt
                    1 => fval >= filter_lit, // Ge
                    2 => fval < filter_lit,  // Lt
                    3 => fval <= filter_lit, // Le
                    4 => fval == filter_lit, // Eq
                    5 => fval != filter_lit, // NotEq
                    _ => false,
                };
                if !pass { continue; }

                // Accumulate
                match agg_col_idx {
                    None => {
                        // COUNT(*)
                        count += 1;
                    }
                    Some(aci) => {
                        // Read agg column
                        let aoff = if aci == filter_col_idx {
                            foff // same column, reuse offset
                        } else if agg_skip > 0 {
                            vs + agg_skip
                        } else {
                            let mut o = vs + 4;
                            for _ in 0..aci {
                                if o + 4 > end { break; }
                                let cl = u32::from_le_bytes([p[o], p[o+1], p[o+2], p[o+3]]) as usize;
                                o += 4 + cl;
                            }
                            o
                        };
                        if aoff + 13 > end { continue; }
                        let atag = p[aoff + 4];
                        match atag {
                            2 => {
                                let n = i64::from_le_bytes(p[aoff+5..aoff+13].try_into().unwrap());
                                sum_int = sum_int.wrapping_add(n);
                                count += 1;
                            }
                            3 => {
                                let f = f64::from_le_bytes(p[aoff+5..aoff+13].try_into().unwrap());
                                sum_float += f;
                                has_float = true;
                                count += 1;
                            }
                            _ => {}
                        }
                    }
                }
            }
            pool.unpin_page(cur_id, false)?;
            cur_id = next_leaf;
        }
        Ok((sum_int, sum_float, has_float, count))
    }

    /// Count all entries by walking the leaf chain without deserializing values.
    /// Much faster than scan_all for COUNT(*) queries.
    pub fn count_entries(&self, pool: &mut BufferPool) -> Result<u64> {
        let mut count = 0u64;
        let mut cur_id = self.leftmost_leaf(pool)?;
        loop {
            if cur_id == INVALID_PAGE_ID {
                break;
            }
            let (num_keys, next_leaf) = Self::count_leaf_entries_fast(pool, cur_id)?;
            count += num_keys as u64;
            cur_id = next_leaf;
        }
        Ok(count)
    }

    /// Read only num_keys and next_leaf from a leaf page's header bytes,
    /// avoiding full deserialization of keys and values.
    fn count_leaf_entries_fast(pool: &mut BufferPool, page_id: PageId) -> Result<(u16, PageId)> {
        let page = pool.fetch_page(page_id)?;
        let payload = page.payload();
        // Node header layout: [0] node_type, [1..3] num_keys (u16 LE), [9..13] next_leaf (u32 LE)
        let num_keys = u16::from_le_bytes([payload[1], payload[2]]);
        let next_leaf = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);
        pool.unpin_page(page_id, false)?;
        Ok((num_keys, next_leaf))
    }

    /// 找到最左叶子节点（zero-alloc: reads page payload directly）
    fn leftmost_leaf(&self, pool: &mut BufferPool) -> Result<PageId> {
        let mut cur_id = self.root_id;
        loop {
            let page = pool.fetch_page(cur_id)?;
            let payload = page.payload();
            if payload[0] == 1 {
                // Leaf node
                pool.unpin_page(cur_id, false)?;
                return Ok(cur_id);
            }
            // Internal node: follow children[0]
            let num_keys = u16::from_le_bytes([payload[1], payload[2]]) as usize;
            let child_id = if num_keys > 0 {
                let cell_off = Self::cell_offset(payload, 0);
                // Bounds check to handle corrupt pages gracefully
                if cell_off + 6 > payload.len() {
                    pool.unpin_page(cur_id, false)?;
                    // Fall back to full deserialization for corrupt pages
                    let node = Self::read_node(pool, cur_id)?;
                    if node.is_leaf() { return Ok(cur_id); }
                    cur_id = node.children[0];
                    continue;
                }
                Self::internal_cell_child(payload, cell_off)
            } else {
                u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]])
            };
            pool.unpin_page(cur_id, false)?;
            cur_id = child_id;
        }
    }
}
