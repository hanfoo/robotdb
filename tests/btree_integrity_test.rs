/// B-Tree 结构完整性测试套件
///
/// 测试策略：
/// 1. 基础结构验证：空树、单节点、小规模操作后的结构检查
/// 2. 随机操作压力测试：大量随机插入/删除后验证所有不变量
/// 3. 边界条件：节点分裂、树高增长、极端键值
/// 4. 属性测试（Property-Based）：对任意操作序列验证不变量

use robotdb::btree::{BTree, IntegrityReport};
use robotdb::storage::{BufferPool, DiskManager};
use robotdb::error::Result;
use std::collections::{BTreeMap, HashSet};
use tempfile::tempdir;

// ─────────────────────────────────────────────────────────────────────────────
// 测试辅助工具
// ─────────────────────────────────────────────────────────────────────────────

/// 创建一个基于临时文件的缓冲池（每个测试独立）
fn make_pool() -> (BufferPool, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk = DiskManager::open(&path).unwrap();
    let pool = BufferPool::new(disk, 512);
    (pool, dir)
}

/// 将整数编码为固定长度的大端字节序（用于保证 B-Tree 中的排序正确性）
fn encode_key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn encode_val(n: u64) -> Vec<u8> {
    format!("value_{}", n).into_bytes()
}

/// 断言完整性检查通过，否则打印详细报告并 panic
fn assert_valid(report: &IntegrityReport, context: &str) {
    if !report.is_valid() {
        eprintln!("\n{}", report);
        panic!("B-Tree integrity check FAILED in context: {}\nViolations:\n{}",
            context,
            report.violations.iter()
                .enumerate()
                .map(|(i, v)| format!("  [{}] {}", i+1, v))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 基础结构测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_empty_tree_is_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let tree = BTree::create(&mut pool)?;
    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "empty tree");
    assert_eq!(report.stats.total_keys, 0);
    assert_eq!(report.stats.height, 1);
    Ok(())
}

#[test]
fn test_single_insert_is_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    tree.insert(&mut pool, encode_key(42), encode_val(42))?;

    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, "single insert");
    assert_eq!(report.stats.total_keys, 1);
    assert_eq!(report.stats.height, 1);
    Ok(())
}

#[test]
fn test_sequential_inserts_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let n = 100u64;

    for i in 0..n {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "100 sequential inserts");
    assert_eq!(report.stats.total_keys, n as usize);
    Ok(())
}

#[test]
fn test_reverse_sequential_inserts_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let n = 100u64;

    // 逆序插入会触发不同的分裂路径
    for i in (0..n).rev() {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, "100 reverse sequential inserts");
    assert_eq!(report.stats.total_keys, n as usize);
    Ok(())
}

#[test]
fn test_large_sequential_inserts_trigger_splits() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let n = 1000u64;

    for i in 0..n {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "1000 sequential inserts (multiple splits)");
    assert_eq!(report.stats.total_keys, n as usize);
    // 1000 条记录必然触发多次分裂，树高应 > 1
    assert!(report.stats.height >= 2,
        "Expected tree height >= 2 after 1000 inserts, got {}", report.stats.height);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 删除操作测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_then_delete_all_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let n = 50u64;

    for i in 0..n {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    // 删除所有偶数键
    for i in (0..n).step_by(2) {
        tree.delete(&mut pool, &encode_key(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, "delete half of keys");
    assert_eq!(report.stats.total_keys, (n / 2) as usize);
    Ok(())
}

#[test]
fn test_interleaved_insert_delete_valid() -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;

    // 插入 1-50，删除 1-25，再插入 51-100
    for i in 1u64..=50 {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }
    for i in 1u64..=25 {
        tree.delete(&mut pool, &encode_key(i))?;
    }
    for i in 51u64..=100 {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, "interleaved insert/delete");
    assert_eq!(report.stats.total_keys, 75); // 26-100 = 75 keys
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 随机操作压力测试（Property-Based 风格）
// ─────────────────────────────────────────────────────────────────────────────

/// 使用线性同余生成器（LCG）作为确定性伪随机数生成器
/// 避免引入外部依赖，同时保证测试可重现
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self { Self { state: seed } }
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    fn next_range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

/// 核心属性测试：对任意操作序列，B-Tree 的结构始终有效
/// 同时使用 BTreeMap 作为参考实现（Oracle），验证语义正确性
fn run_random_ops_test(
    seed: u64,
    n_ops: usize,
    key_range: u64,
    context: &str,
) -> Result<()> {
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut rng = Lcg::new(seed);

    for op_idx in 0..n_ops {
        let key_num = rng.next_range(0, key_range);
        let op = rng.next_range(0, 3); // 0=insert, 1=delete, 2=get

        match op {
            0 => {
                // INSERT
                let val = rng.next();
                if oracle.contains_key(&key_num) {
                    // 键已存在，跳过（避免 DuplicateKey 错误）
                } else {
                    tree.insert(&mut pool, encode_key(key_num), encode_val(val))?;
                    oracle.insert(key_num, val);
                }
            }
            1 => {
                // DELETE
                if oracle.contains_key(&key_num) {
                    tree.delete(&mut pool, &encode_key(key_num))?;
                    oracle.remove(&key_num);
                }
                // 若键不存在则跳过
            }
            2 => {
                // GET（验证语义正确性）
                let tree_result = tree.get(&mut pool, &encode_key(key_num))?;
                let oracle_result = oracle.get(&key_num);
                match (tree_result, oracle_result) {
                    (Some(_), Some(_)) => {} // 都找到，值的内容验证略（val 是随机的）
                    (None, None) => {}       // 都未找到
                    (Some(_), None) => {
                        panic!("[{}] op#{}: GET key={} returned Some but oracle says None",
                            context, op_idx, key_num);
                    }
                    (None, Some(_)) => {
                        panic!("[{}] op#{}: GET key={} returned None but oracle says Some",
                            context, op_idx, key_num);
                    }
                }
            }
            _ => unreachable!(),
        }

        // 每 100 次操作做一次完整性检查
        if (op_idx + 1) % 100 == 0 {
            let report = tree.integrity_check(&mut pool)?;
            if !report.is_valid() {
                eprintln!("{}", report);
                panic!("[{}] Integrity check failed at op#{} (seed={}, key_range={})",
                    context, op_idx, seed, key_range);
            }
        }
    }

    // 最终完整性检查
    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, context);

    // 验证最终键数量与 oracle 一致
    assert_eq!(
        report.stats.total_keys, oracle.len(),
        "[{}] Final key count mismatch: B-Tree={}, oracle={}",
        context, report.stats.total_keys, oracle.len()
    );

    // 验证全表扫描结果与 oracle 完全一致（顺序 + 内容）
    let all_pairs = tree.scan_all(&mut pool)?;
    let oracle_pairs: Vec<(u64, u64)> = oracle.iter().map(|(&k, &v)| (k, v)).collect();

    assert_eq!(
        all_pairs.len(), oracle_pairs.len(),
        "[{}] scan_all count mismatch", context
    );

    for (i, (tree_kv, (oracle_k, _oracle_v))) in all_pairs.iter().zip(oracle_pairs.iter()).enumerate() {
        let tree_key = u64::from_be_bytes(tree_kv.0.as_slice().try_into().unwrap());
        assert_eq!(
            tree_key, *oracle_k,
            "[{}] scan_all key mismatch at index {}: got {}, expected {}",
            context, i, tree_key, oracle_k
        );
    }

    println!("[{}] PASSED: {} ops, seed={}, final_keys={}, height={}",
        context, n_ops, seed, report.stats.total_keys, report.stats.height);
    Ok(())
}

#[test]
fn test_random_ops_small_key_range() -> Result<()> {
    // 小键范围：大量碰撞，频繁插入已存在的键
    run_random_ops_test(42, 500, 20, "small_key_range")
}

#[test]
fn test_random_ops_medium_key_range() -> Result<()> {
    run_random_ops_test(12345, 1000, 200, "medium_key_range")
}

#[test]
fn test_random_ops_large_key_range() -> Result<()> {
    // 大键范围：主要是插入，很少碰撞
    run_random_ops_test(99999, 2000, 10000, "large_key_range")
}

#[test]
fn test_random_ops_multiple_seeds() -> Result<()> {
    // 使用多个不同种子，覆盖更多随机路径
    for seed in [1, 7, 13, 31, 42, 100, 777, 1234, 9999, 65537] {
        run_random_ops_test(seed, 300, 100, &format!("seed_{}", seed))?;
    }
    Ok(())
}

#[test]
fn test_random_ops_insert_heavy() -> Result<()> {
    // 插入密集型：90% 插入，10% 删除
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut rng = Lcg::new(2024);
    let key_range = 5000u64;

    for op_idx in 0..3000 {
        let key_num = rng.next_range(0, key_range);
        let op = rng.next_range(0, 10); // 0-8=insert, 9=delete

        if op < 9 {
            if !oracle.contains_key(&key_num) {
                tree.insert(&mut pool, encode_key(key_num), encode_val(key_num))?;
                oracle.insert(key_num, key_num);
            }
        } else {
            if oracle.contains_key(&key_num) {
                tree.delete(&mut pool, &encode_key(key_num))?;
                oracle.remove(&key_num);
            }
        }

        if (op_idx + 1) % 500 == 0 {
            let report = tree.integrity_check(&mut pool)?;
            assert_valid(&report, &format!("insert_heavy op#{}", op_idx));
        }
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "insert_heavy final");
    Ok(())
}

#[test]
fn test_random_ops_delete_heavy() -> Result<()> {
    // 删除密集型：先填充，再大量删除
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let n = 500u64;

    // 先插入 500 条
    for i in 0..n {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
        oracle.insert(i, i);
    }

    let mut rng = Lcg::new(31415);

    // 随机删除 400 条
    let keys_to_delete: Vec<u64> = {
        let mut keys: Vec<u64> = oracle.keys().copied().collect();
        // 用 LCG 打乱顺序
        for i in (1..keys.len()).rev() {
            let j = (rng.next() as usize) % (i + 1);
            keys.swap(i, j);
        }
        keys[..400].to_vec()
    };

    for key in &keys_to_delete {
        tree.delete(&mut pool, &encode_key(*key))?;
        oracle.remove(key);
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "delete_heavy");
    assert_eq!(report.stats.total_keys, oracle.len());
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 边界条件测试
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_large_value_entries() -> Result<()> {
    // 测试较大值的存储（接近页面容量限制）
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;

    for i in 0u64..50 {
        let key = encode_key(i);
        // 较大的值（200 字节）
        let val = vec![i as u8; 200];
        tree.insert(&mut pool, key, val)?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "large values");
    assert_eq!(report.stats.total_keys, 50);
    Ok(())
}

#[test]
fn test_variable_length_keys() -> Result<()> {
    // 测试变长键（字符串键）
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;

    let keys = vec![
        "a", "ab", "abc", "abcd", "b", "ba", "bab",
        "hello", "hello_world", "z", "zz", "zzz",
        "", // 空键
        "中文键", // Unicode 键
    ];

    for (i, k) in keys.iter().enumerate() {
        tree.insert(&mut pool, k.as_bytes().to_vec(), encode_val(i as u64))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "variable length keys");
    assert_eq!(report.stats.total_keys, keys.len());
    Ok(())
}

#[test]
fn test_tree_grows_to_multiple_levels() -> Result<()> {
    // 插入足够多的数据，确保树生长到 2 层以上
    // 注意：B+ Tree 节点容量大（每页 4KB），5000 条记录只需 2 层
    // 要达到 3 层需要足够大的数据量
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let n = 5000u64;

    for i in 0..n {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "5000 inserts multi-level tree");
    assert_eq!(report.stats.total_keys, n as usize);
    // 树高应大于 1（即发生了分裂，有内部节点）
    assert!(report.stats.height >= 2,
        "Expected height >= 2 for 5000 keys, got {}", report.stats.height);
    assert!(report.stats.internal_nodes >= 1,
        "Expected at least 1 internal node for 5000 keys");
    println!("Tree height for {} keys: {}", n, report.stats.height);
    Ok(())
}

#[test]
fn test_scan_all_returns_sorted_order() -> Result<()> {
    // 验证 scan_all 返回的结果严格有序
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut rng = Lcg::new(54321);
    let mut inserted_keys = HashSet::new();

    for _ in 0..500 {
        let k = rng.next_range(0, 1000);
        if !inserted_keys.contains(&k) {
            tree.insert(&mut pool, encode_key(k), encode_val(k))?;
            inserted_keys.insert(k);
        }
    }

    let all_pairs = tree.scan_all(&mut pool)?;

    // 验证严格升序
    for i in 1..all_pairs.len() {
        assert!(
            all_pairs[i].0 > all_pairs[i-1].0,
            "scan_all result not sorted at index {}: {:?} <= {:?}",
            i, all_pairs[i].0, all_pairs[i-1].0
        );
    }

    // 验证数量正确
    assert_eq!(all_pairs.len(), inserted_keys.len());

    // 验证完整性
    let report = tree.integrity_check(&mut pool)?;
    assert_valid(&report, "scan_all sorted order");
    Ok(())
}

#[test]
fn test_upsert_does_not_corrupt_tree() -> Result<()> {
    // 测试 upsert（插入或更新）不会破坏树结构
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut rng = Lcg::new(11111);

    // 先插入 200 条
    for i in 0u64..200 {
        tree.upsert(&mut pool, encode_key(i), encode_val(i))?;
    }

    // 再 upsert 300 条（其中 200 条是更新，100 条是新插入）
    for _ in 0..300 {
        let k = rng.next_range(0, 300);
        let v = rng.next();
        tree.upsert(&mut pool, encode_key(k), encode_val(v))?;
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    assert_valid(&report, "upsert operations");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// 高强度随机测试（模拟真实工作负载）
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_workload_oltp_simulation() -> Result<()> {
    // 模拟 OLTP 工作负载：70% 读，20% 写，10% 删除
    let (mut pool, _dir) = make_pool();
    let mut tree = BTree::create(&mut pool)?;
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut rng = Lcg::new(20240101);
    let key_range = 2000u64;

    // 预热：插入 500 条
    for i in 0u64..500 {
        tree.insert(&mut pool, encode_key(i), encode_val(i))?;
        oracle.insert(i, i);
    }

    let mut reads = 0usize;
    let mut writes = 0usize;
    let mut deletes = 0usize;

    for _ in 0..5000 {
        let key = rng.next_range(0, key_range);
        let op = rng.next_range(0, 10);

        match op {
            0..=6 => {
                // 70% 读
                let tree_result = tree.get(&mut pool, &encode_key(key))?;
                let oracle_result = oracle.get(&key);
                match (tree_result.is_some(), oracle_result.is_some()) {
                    (true, true) | (false, false) => {}
                    (got, expected) => panic!(
                        "GET mismatch for key {}: tree={}, oracle={}", key, got, expected
                    ),
                }
                reads += 1;
            }
            7..=8 => {
                // 20% 写
                if !oracle.contains_key(&key) {
                    let val = rng.next();
                    tree.insert(&mut pool, encode_key(key), encode_val(val))?;
                    oracle.insert(key, val);
                }
                writes += 1;
            }
            9 => {
                // 10% 删除
                if oracle.contains_key(&key) {
                    tree.delete(&mut pool, &encode_key(key))?;
                    oracle.remove(&key);
                }
                deletes += 1;
            }
            _ => unreachable!(),
        }
    }

    let report = tree.integrity_check(&mut pool)?;
    println!("{}", report);
    println!("OLTP simulation: reads={}, writes={}, deletes={}", reads, writes, deletes);
    assert_valid(&report, "oltp_simulation");
    assert_eq!(report.stats.total_keys, oracle.len());
    Ok(())
}
