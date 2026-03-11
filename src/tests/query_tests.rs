use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;
use crate::bufferpool::{BufferPool, DiskManager};
use crate::errors::DbError;
use crate::query::Query;
use crate::table::Table;

static TEST_DIR_CTR: AtomicUsize = AtomicUsize::new(0);

fn make_bp(prefix: &str) -> Arc<BufferPool> {
    let id = TEST_DIR_CTR.fetch_add(1, Ordering::Relaxed);
    let dir = format!("./test_tmp/{}_{}", prefix, id);
    let _ = std::fs::remove_dir_all(&dir);
    let dm = Arc::new(RwLock::new(DiskManager::new(&dir).unwrap()));
    Arc::new(BufferPool::new(dm))
}

fn setup(num_columns: usize) -> Query {
    let bp = make_bp("qtest");
    let table = Table::new_no_transaction(String::from("test"), num_columns, 0, 0, bp);
    Query::new(Arc::from(table))
}

#[test]
fn insert_and_select() {
    let q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20), Some(30)]);
}

#[test]
fn insert_and_select_version_1() {
    let  q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    q.update(10, vec![None, Some(2), Some(3)]).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select_version(10, 0, &mask, -1).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20), Some(30)]);
}

#[test]
fn insert_and_select_version_2() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();

    q.update(1, vec![None, None, Some(6)]).unwrap();
    q.update(1, vec![None, None, Some(5)]).unwrap();
    q.update(1, vec![None, Some(10), Some(4)]).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select_version(1, 0, &mask, -2).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(1), Some(2), Some(6)]);
}

#[test]
fn remove_and_select_version_error() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();

    q.update(10, vec![None, None, Some(6)]).unwrap();
    q.update(10, vec![None, None, Some(5)]).unwrap();
    q.delete(1).unwrap();

    let mask = [1i64, 1, 1];
    assert!(matches!(q.select_version(1, 0, &mask, -1), Err(DbError::KeyNotFound(1))));
}

#[test]
fn insert_duplicate_key_fails() {
    let  q = setup(3);
    assert!(q.insert(vec![Some(1), Some(2), Some(3)]).unwrap());
    assert!(!q.insert(vec![Some(1), Some(5), Some(6)]).unwrap());
}

#[test]
fn update_and_select() {
    let  q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();

    q.update(1, vec![None, Some(20), None, Some(40)]).unwrap();

    let mask = [1i64, 1, 1, 1];
    let result = q.select(1, 0, &mask).unwrap();
    assert_eq!(result[0], vec![Some(1), Some(20), Some(3), Some(40)]);
}

#[test]
fn delete_removes_from_index() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.delete(1).unwrap();
    assert!(q.table.indices[0].locate(1).is_none());
}

#[test]
fn select_deleted_key_returns_empty() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.delete(1).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select(1, 0, &mask).unwrap();
    assert!(result.is_empty());
}

#[test]
fn sum_range() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(10), Some(100)]).unwrap();
    q.insert(vec![Some(2), Some(20), Some(200)]).unwrap();
    q.insert(vec![Some(3), Some(30), Some(300)]).unwrap();

    assert_eq!(q.sum(1, 3, 1).unwrap(), 60);
    assert_eq!(q.sum(1, 2, 2).unwrap(), 300);
}

#[test]
fn increment() {
    let mut q = setup(3);
    q.insert(vec![Some(1), Some(10), Some(100)]).unwrap();

    q.increment(1, 1).unwrap();
    q.increment(1, 1).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select(1, 0, &mask).unwrap();
    assert_eq!(result[0][1], Some(12));
    assert_eq!(result[0][0], Some(1));
    assert_eq!(result[0][2], Some(100));
}

#[test]
fn quick_test_all() {
    let bp = make_bp("quick");
    let table: Table = Table::new_no_transaction(String::from("test"), 5, 0, 0, bp);
    let mut query: Query = Query::new(Arc::from(table));

    let rec_one: Vec<Option<i64>> = vec![Some(1); 5];
    let rec_two: Vec<Option<i64>> = vec![Some(2); 5];
    let rec_three: Vec<Option<i64>> = vec![Some(3), Some(4), Some(5), Some(6), Some(7)];
    let rec_four: Vec<Option<i64>> = vec![Some(4), Some(5), Some(6), Some(7), Some(8)];

    query.insert(rec_one).unwrap();
    query.insert(rec_two).unwrap();

    let rid1 = query.table.indices[0].locate(1).unwrap();
    let row1 = query.table.read(rid1).unwrap();
    assert_eq!(&row1[..5], &[Some(1); 5]);

    let rid2 = query.table.indices[0].locate(2).unwrap();
    let row2 = query.table.read(rid2).unwrap();
    assert_eq!(&row2[..5], &[Some(2); 5]);

    query.insert(rec_three).unwrap();

    let ans: i64 = query.sum(1, 3, 3).unwrap();
    assert_eq!(ans, 9);

    query.insert(rec_four).unwrap();
    query.delete(4).unwrap();

    let mask: [i64; 5] = [1, 0, 1, 0, 1];
    let ans_list: Vec<Vec<Option<i64>>> = query.select(1, 0, &mask).unwrap();
    assert_eq!(ans_list.len(), 1);

    assert!(query.table.indices[0].locate(4).is_none());

    query.increment(2, 0).unwrap();
    query.increment(1, 0).unwrap();

    let full_mask: [i64; 5] = [1, 1, 1, 1, 1];
    let ans_list_two: Vec<Vec<Option<i64>>> = query.select(2, 0, &full_mask).unwrap();
    assert_eq!(ans_list_two[0][0], Some(2));

    let ans_list_three: Vec<Vec<Option<i64>>> = query.select(3, 0, &full_mask).unwrap();
    assert_eq!(ans_list_three[0][0], Some(3));
}

#[test]
fn sum_version_1() {
    let  q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();
    q.insert(vec![Some(2), Some(6), Some(8)]).unwrap();

    q.update(2, vec![None, Some(2), Some(3)]).unwrap();
    q.update(2, vec![None, Some(4), Some(5)]).unwrap();
    q.update(2, vec![None, Some(4), Some(6)]).unwrap();

    let ans = q.sum_version(1, 5, 2, -1).unwrap();
    assert_eq!(ans, 15);
}

#[test]
fn sum_version_2() {
    let q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.insert(vec![Some(2), Some(6), Some(1)]).unwrap();
    q.insert(vec![Some(3), Some(10), Some(8)]).unwrap();
    q.insert(vec![Some(4), Some(2), Some(13)]).unwrap();
    q.insert(vec![Some(5), Some(12), Some(7)]).unwrap();
    q.insert(vec![Some(6), Some(6), Some(18)]).unwrap();

    q.update(2, vec![None, None, Some(3)]).unwrap();
    q.update(2, vec![None, None, Some(5)]).unwrap();
    q.update(2, vec![None, None, Some(6)]).unwrap();
    q.update(4, vec![None, None, Some(3)]).unwrap();
    q.update(5, vec![None, None, Some(5)]).unwrap();
    q.update(6, vec![None, None, Some(6)]).unwrap();
    q.update(6, vec![None, Some(5), None]).unwrap();
    q.update(6, vec![None, Some(8), None]).unwrap();

    let ans = q.sum_version(6, 6, 2, -1).unwrap();
    assert_eq!(ans, 18);
}

#[test]
fn sum_version_3() {
    let q = setup(3);
    q.insert(vec![Some(1), Some(52), Some(-3)]).unwrap();
    q.insert(vec![Some(2), Some(63), Some(-1)]).unwrap();
    q.insert(vec![Some(3), Some(210), Some(8)]).unwrap();
    q.insert(vec![Some(4), Some(2), Some(134)]).unwrap();
    q.insert(vec![Some(5), Some(152), Some(37)]).unwrap();
    q.insert(vec![Some(6), Some(1), Some(128)]).unwrap();

    q.update(2, vec![None, None, Some(2)]).unwrap();
    q.update(2, vec![None, Some(5), None]).unwrap();
    q.update(2, vec![None, Some(8), None]).unwrap();
    q.update(2, vec![None, None, Some(-5)]).unwrap();
    q.update(2, vec![None, None, Some(6)]).unwrap();

    q.update(4, vec![None, None, Some(3)]).unwrap();

    q.update(5, vec![None, None, Some(-5)]).unwrap();
    q.update(6, vec![None, None, Some(-6)]).unwrap();
    q.update(6, vec![None, None, Some(-3)]).unwrap();
    q.update(6, vec![None, Some(5), None]).unwrap();
    q.update(6, vec![None, None, Some(-6)]).unwrap();
    q.update(6, vec![None, Some(5), None]).unwrap();
    q.update(6, vec![None, Some(8), None]).unwrap();

    let ans = q.sum_version(2, 6, 2, -2).unwrap();
    assert_eq!(ans, 175);
}

#[test]
fn select_version_disjoint_column_updates() {
    let q = setup(3);
    q.insert(vec![Some(100), Some(10), Some(20)]).unwrap();

    q.update(100, vec![None, Some(11), None]).unwrap();
    q.update(100, vec![None, None, Some(22)]).unwrap();

    let mask = [1i64, 1, 1];

    let latest = q.select_version(100, 0, &mask, 0).unwrap();
    assert_eq!(latest[0], vec![Some(100), Some(11), Some(22)]);

    let prev = q.select_version(100, 0, &mask, -1).unwrap();
    assert_eq!(prev[0], vec![Some(100), Some(11), Some(20)]);

    let base = q.select_version(100, 0, &mask, -2).unwrap();
    assert_eq!(base[0], vec![Some(100), Some(10), Some(20)]);
}

#[test]
fn test_version_single() {
    let q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();
    q.insert(vec![Some(2), Some(6), Some(8)]).unwrap();

    q.update(2, vec![None, Some(2), Some(3)]).unwrap();
    q.update(2, vec![None, Some(4), Some(5)]).unwrap();
    q.update(2, vec![None, Some(4), Some(6)]).unwrap();

    let num1 = q.table.read_version_single(0, 2, -2).unwrap();
    let num2 = q.table.read_version_single(1, 2, -5).unwrap();
    let num3 = q.table.read_version_single(2, 2, 0).unwrap();
    assert_eq!(num1.unwrap(), 3);
    assert_eq!(num2.unwrap(), 7);
    assert_eq!(num3.unwrap(), 6);
}