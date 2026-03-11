use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;
use crate::bufferpool::{BufferPool, DiskManager};
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
    let bp = make_bp("ttest");
    let table = Table::new_no_transaction(String::from("test"), num_columns, 0, 0, bp);
    Query::new(Arc::from(table))
}

#[test]
fn insert_and_read_latest() {
    let q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    let rid = q.table.indices[0].locate(10).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(&result[..3], &[Some(10), Some(20), Some(30)]);
}

#[test]
fn read_latest_follows_update_chain() {
    let q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    q.update(10, vec![None, Some(99), None]).unwrap();

    let rid = q.table.indices[0].locate(10).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(&result[..3], &[Some(10), Some(99), Some(30)]);
}

#[test]
fn read_latest_multiple_updates() {
    let q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();

    q.update(1, vec![None, Some(20), None, None]).unwrap();
    q.update(1, vec![None, None, Some(30), None]).unwrap();

    let rid = q.table.indices[0].locate(1).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(&result[..4], &[Some(1), Some(20), Some(30), Some(4)]);
}

#[test]
fn read_latest_projected() {
    let q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();
    q.update(1, vec![None, Some(99), None, None]).unwrap();

    let rid = q.table.indices[0].locate(1).unwrap();
    let projected = [1, 0, 1, 0];
    let result = q.table.read_latest_projected(&projected, rid).unwrap();
    assert_eq!(result, vec![Some(1), None, Some(3), None]);
}

#[test]
fn read_latest_single() {
    let q = setup(3);
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();
    q.update(5, vec![None, Some(60), None]).unwrap();

    let rid = q.table.indices[0].locate(5).unwrap();
    assert_eq!(q.table.read_latest_single(rid, 0).unwrap(), Some(5));
    assert_eq!(q.table.read_latest_single(rid, 1).unwrap(), Some(60));
    assert_eq!(q.table.read_latest_single(rid, 2).unwrap(), Some(7));
}

#[test]
fn index_being_weird() {
    let q = setup(3);
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();

    let rid = q.table.indices[0].locate(5).unwrap();
    assert_eq!(q.table.read_latest_single(rid, 0).unwrap(), Some(5));
}