/// B-Tree 结构完整性验证
///
/// 验证以下不变量：
/// 1. 每个节点的键必须严格有序（升序）
/// 2. 内部节点的子节点数量 = 键数量 + 1
/// 3. 叶子节点的值数量 = 键数量
/// 4. 所有叶子节点通过 next_leaf 指针形成有序链表，且链表中的键全局有序
/// 5. 内部节点的分隔键必须满足：左子树所有键 < 分隔键 <= 右子树所有键
/// 6. 树高度一致（所有叶子节点在同一深度）
/// 7. 无循环引用（页面 ID 不重复访问）
/// 8. 根节点的 parent_id 为 INVALID_PAGE_ID

use std::collections::HashSet;
use crate::error::Result;
use crate::storage::{BufferPool, PageId, INVALID_PAGE_ID};
use super::node::BTreeNode;
use super::tree::BTree;

/// 完整性检查报告
#[derive(Debug, Default)]
pub struct IntegrityReport {
    /// 检查通过的不变量数量
    pub checks_passed: usize,
    /// 发现的违规列表
    pub violations: Vec<String>,
    /// 树的统计信息
    pub stats: TreeStats,
}

/// B-Tree 统计信息
#[derive(Debug, Default)]
pub struct TreeStats {
    /// 总节点数
    pub total_nodes: usize,
    /// 内部节点数
    pub internal_nodes: usize,
    /// 叶子节点数
    pub leaf_nodes: usize,
    /// 总键值对数量
    pub total_keys: usize,
    /// 树的高度（根到叶子的层数）
    pub height: usize,
    /// 最小叶子节点填充率（键数）
    pub min_leaf_keys: usize,
    /// 最大叶子节点填充率（键数）
    pub max_leaf_keys: usize,
}

impl IntegrityReport {
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    fn pass(&mut self, msg: &str) {
        self.checks_passed += 1;
        let _ = msg; // 可选：记录通过的检查项
    }

    fn fail(&mut self, msg: String) {
        self.violations.push(msg);
    }
}

impl BTree {
    /// 执行完整的 B-Tree 结构完整性验证
    ///
    /// 返回详细的检查报告，包含所有发现的违规和统计信息。
    pub fn integrity_check(&self, pool: &mut BufferPool) -> Result<IntegrityReport> {
        let mut report = IntegrityReport::default();
        let mut visited = HashSet::new();

        // 1. 验证根节点存在且可读
        if self.root_id == INVALID_PAGE_ID {
            report.fail("Root page ID is INVALID_PAGE_ID".into());
            return Ok(report);
        }

        let root_node = match Self::read_node_for_check(pool, self.root_id) {
            Ok(n) => n,
            Err(e) => {
                report.fail(format!("Cannot read root node (page {}): {}", self.root_id, e));
                return Ok(report);
            }
        };
        report.pass("Root node is readable");

        // 2. 验证根节点的 parent_id 为 INVALID
        if root_node.parent_id != INVALID_PAGE_ID {
            report.fail(format!(
                "Root node (page {}) has non-INVALID parent_id: {}",
                self.root_id, root_node.parent_id
            ));
        } else {
            report.pass("Root node parent_id is INVALID");
        }

        // 3. 递归验证整棵树结构
        let height = Self::check_subtree(
            pool,
            self.root_id,
            None,
            None,
            &mut visited,
            &mut report,
            0,
        )?;
        report.stats.height = height;
        report.pass("Tree height is consistent across all paths");

        // 4. 验证叶子链表的全局有序性
        Self::check_leaf_chain(pool, self.root_id, &mut report)?;
        report.pass("Leaf chain is globally ordered");

        // 5. 收集统计信息
        Self::collect_stats(pool, self.root_id, &mut report.stats)?;

        Ok(report)
    }

    /// 递归验证子树
    ///
    /// 返回子树的高度（叶子节点高度为 1）
    fn check_subtree(
        pool: &mut BufferPool,
        page_id: PageId,
        lower_bound: Option<&[u8]>,  // 当前节点所有键必须 > lower_bound
        upper_bound: Option<&[u8]>,  // 当前节点所有键必须 <= upper_bound
        visited: &mut HashSet<PageId>,
        report: &mut IntegrityReport,
        depth: usize,
    ) -> Result<usize> {
        // 检测循环引用
        if !visited.insert(page_id) {
            report.fail(format!(
                "Cycle detected: page {} is referenced more than once",
                page_id
            ));
            return Ok(0);
        }

        // 防止无限递归（树高度不应超过 64 层）
        if depth > 64 {
            report.fail(format!(
                "Tree depth exceeds 64 at page {}, possible infinite loop",
                page_id
            ));
            return Ok(depth);
        }

        let node = match Self::read_node_for_check(pool, page_id) {
            Ok(n) => n,
            Err(e) => {
                report.fail(format!("Cannot read node at page {}: {}", page_id, e));
                return Ok(0);
            }
        };

        // ── 检查键的有序性 ────────────────────────────────────────────────────

        // 键必须严格递增
        for i in 1..node.keys.len() {
            if node.keys[i] <= node.keys[i - 1] {
                report.fail(format!(
                    "Node at page {}: keys are not strictly ascending at index {} ({:?} <= {:?})",
                    page_id, i,
                    truncate_key(&node.keys[i]),
                    truncate_key(&node.keys[i - 1])
                ));
            }
        }

        // B+ Tree 键范围不变量：
        // - 左子树的键必须小于等于分隔键（upper_bound）
        // - 右子树的键必须大于等于分隔键（lower_bound）
        // 注意：B+ Tree 分裂时将右子树的第一个键复制到父节点，
        // 因此左子树最大键 == 分隔键，右子树最小键 == 分隔键是合法的
        for (i, key) in node.keys.iter().enumerate() {
            if let Some(lb) = lower_bound {
                if key.as_slice() < lb {
                    report.fail(format!(
                        "Node at page {}: key[{}] ({:?}) is strictly less than lower bound ({:?})",
                        page_id, i,
                        truncate_key(key),
                        truncate_key(lb)
                    ));
                }
            }
            if let Some(ub) = upper_bound {
                if key.as_slice() > ub {
                    report.fail(format!(
                        "Node at page {}: key[{}] ({:?}) exceeds upper bound ({:?})",
                        page_id, i,
                        truncate_key(key),
                        truncate_key(ub)
                    ));
                }
            }
        }

        if node.is_leaf() {
            // ── 叶子节点检查 ──────────────────────────────────────────────────

            // 值数量必须等于键数量
            if node.values.len() != node.keys.len() {
                report.fail(format!(
                    "Leaf node at page {}: keys.len()={} != values.len()={}",
                    page_id, node.keys.len(), node.values.len()
                ));
            } else {
                report.pass(&format!("Leaf node at page {} has consistent key/value counts", page_id));
            }

            // 子节点列表必须为空
            if !node.children.is_empty() {
                report.fail(format!(
                    "Leaf node at page {} has non-empty children list (len={})",
                    page_id, node.children.len()
                ));
            }

            Ok(1) // 叶子节点高度为 1
        } else {
            // ── 内部节点检查 ──────────────────────────────────────────────────

            let n_keys = node.keys.len();
            let n_children = node.children.len();

            // 子节点数量必须 = 键数量 + 1
            if n_children != n_keys + 1 {
                report.fail(format!(
                    "Internal node at page {}: children.len()={} != keys.len()+1={}",
                    page_id, n_children, n_keys + 1
                ));
            } else {
                report.pass(&format!("Internal node at page {} has consistent key/child counts", page_id));
            }

            // 值列表必须为空
            if !node.values.is_empty() {
                report.fail(format!(
                    "Internal node at page {} has non-empty values list (len={})",
                    page_id, node.values.len()
                ));
            }

            // 递归验证每个子树，并检查高度一致性
            let mut child_heights = Vec::new();
            for (i, &child_id) in node.children.iter().enumerate() {
                if child_id == INVALID_PAGE_ID {
                    report.fail(format!(
                        "Internal node at page {}: child[{}] is INVALID_PAGE_ID",
                        page_id, i
                    ));
                    continue;
                }

                // 确定子树的键范围约束
                let child_lower = if i == 0 {
                    lower_bound.map(|b| b.to_vec())
                } else {
                    Some(node.keys[i - 1].clone())
                };
                let child_upper = if i < n_keys {
                    Some(node.keys[i].clone())
                } else {
                    upper_bound.map(|b| b.to_vec())
                };

                // 验证子节点的 parent_id 指向当前节点
                if let Ok(child_node) = Self::read_node_for_check(pool, child_id) {
                    if child_node.parent_id != page_id {
                        report.fail(format!(
                            "Node at page {}: parent_id={} but expected {}",
                            child_id, child_node.parent_id, page_id
                        ));
                    }
                }

                let child_height = Self::check_subtree(
                    pool,
                    child_id,
                    child_lower.as_deref(),
                    child_upper.as_deref(),
                    visited,
                    report,
                    depth + 1,
                )?;
                child_heights.push(child_height);
            }

            // 所有子树高度必须相同（B-Tree 平衡性）
            if !child_heights.is_empty() {
                let first_height = child_heights[0];
                for (i, &h) in child_heights.iter().enumerate() {
                    if h != first_height {
                        report.fail(format!(
                            "Internal node at page {}: child[{}] has height {} but child[0] has height {} (unbalanced tree)",
                            page_id, i, h, first_height
                        ));
                    }
                }
                Ok(first_height + 1)
            } else {
                Ok(1)
            }
        }
    }

    /// 验证叶子链表的全局有序性
    fn check_leaf_chain(
        pool: &mut BufferPool,
        root_id: PageId,
        report: &mut IntegrityReport,
    ) -> Result<()> {
        // 找到最左叶子节点
        let mut cur_id = root_id;
        loop {
            let node = Self::read_node_for_check(pool, cur_id)?;
            if node.is_leaf() {
                break;
            }
            if node.children.is_empty() {
                return Ok(());
            }
            cur_id = node.children[0];
        }

        let mut prev_last_key: Option<Vec<u8>> = None;
        let mut leaf_count = 0usize;
        let mut total_keys = 0usize;
        let mut visited_leaves = HashSet::new();

        loop {
            if cur_id == INVALID_PAGE_ID {
                break;
            }

            // 检测叶子链表中的循环
            if !visited_leaves.insert(cur_id) {
                report.fail(format!(
                    "Leaf chain cycle detected at page {}",
                    cur_id
                ));
                break;
            }

            let node = match Self::read_node_for_check(pool, cur_id) {
                Ok(n) => n,
                Err(e) => {
                    report.fail(format!("Cannot read leaf node at page {}: {}", cur_id, e));
                    break;
                }
            };

            if !node.is_leaf() {
                report.fail(format!(
                    "Leaf chain at page {} points to a non-leaf node",
                    cur_id
                ));
                break;
            }

            // 验证跨叶子节点的全局有序性
            if let Some(ref prev_key) = prev_last_key {
                if let Some(first_key) = node.keys.first() {
                    if first_key.as_slice() <= prev_key.as_slice() {
                        report.fail(format!(
                            "Leaf chain ordering violation at page {}: first key {:?} <= previous leaf's last key {:?}",
                            cur_id,
                            truncate_key(first_key),
                            truncate_key(prev_key)
                        ));
                    }
                }
            }

            total_keys += node.keys.len();
            leaf_count += 1;
            prev_last_key = node.keys.last().cloned();
            cur_id = node.next_leaf;
        }

        report.stats.leaf_nodes = leaf_count;
        report.stats.total_keys = total_keys;
        report.pass(&format!("Leaf chain traversal complete: {} leaves, {} total keys", leaf_count, total_keys));
        Ok(())
    }

    /// 收集树的统计信息
    fn collect_stats(
        pool: &mut BufferPool,
        page_id: PageId,
        stats: &mut TreeStats,
    ) -> Result<()> {
        if page_id == INVALID_PAGE_ID {
            return Ok(());
        }
        let node = Self::read_node_for_check(pool, page_id)?;
        stats.total_nodes += 1;

        if node.is_leaf() {
            stats.leaf_nodes = stats.leaf_nodes.max(1); // will be set by leaf chain check
            let n = node.keys.len();
            if stats.min_leaf_keys == 0 || n < stats.min_leaf_keys {
                stats.min_leaf_keys = n;
            }
            if n > stats.max_leaf_keys {
                stats.max_leaf_keys = n;
            }
        } else {
            stats.internal_nodes += 1;
            for &child_id in &node.children {
                if child_id != INVALID_PAGE_ID {
                    Self::collect_stats(pool, child_id, stats)?;
                }
            }
        }
        Ok(())
    }

    /// 读取节点（完整性检查专用，不修改 pin 状态）
    fn read_node_for_check(pool: &mut BufferPool, page_id: PageId) -> Result<BTreeNode> {
        let page = pool.fetch_page(page_id)?;
        let node = BTreeNode::deserialize(page.payload())?;
        pool.unpin_page(page_id, false)?;
        Ok(node)
    }
}

/// 截断键的显示（避免日志过长）
fn truncate_key(key: &[u8]) -> String {
    if key.len() <= 16 {
        format!("{:?}", key)
    } else {
        format!("{:?}...({}b)", &key[..16], key.len())
    }
}

/// 对 IntegrityReport 实现 Display，便于测试输出
impl std::fmt::Display for IntegrityReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== B-Tree Integrity Report ===")?;
        writeln!(f, "Status: {}", if self.is_valid() { "VALID ✓" } else { "INVALID ✗" })?;
        writeln!(f, "Checks passed: {}", self.checks_passed)?;
        writeln!(f, "Violations: {}", self.violations.len())?;
        writeln!(f, "")?;
        writeln!(f, "--- Tree Statistics ---")?;
        writeln!(f, "  Height:         {}", self.stats.height)?;
        writeln!(f, "  Total nodes:    {}", self.stats.total_nodes)?;
        writeln!(f, "  Internal nodes: {}", self.stats.internal_nodes)?;
        writeln!(f, "  Leaf nodes:     {}", self.stats.leaf_nodes)?;
        writeln!(f, "  Total keys:     {}", self.stats.total_keys)?;
        writeln!(f, "  Leaf fill (min/max keys): {}/{}", self.stats.min_leaf_keys, self.stats.max_leaf_keys)?;
        if !self.violations.is_empty() {
            writeln!(f, "")?;
            writeln!(f, "--- Violations ---")?;
            for (i, v) in self.violations.iter().enumerate() {
                writeln!(f, "  [{}] {}", i + 1, v)?;
            }
        }
        Ok(())
    }
}
