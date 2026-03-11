use crate::db::Database;
use crate::query::Query;
use crate::table::Table;
use crate::transaction::{QueryOp, Transaction};
use crate::transaction_worker::TransactionWorker;
use std::sync::Arc;
use tempfile::TempDir;

fn new_test_db(
    table_name: &str,
    num_columns: usize,
    key_index: usize,
) -> (TempDir, Database, Arc<Table>) {
    let temp_dir = TempDir::new().unwrap();
    let mut db = Database::new();
    db.open(temp_dir.path().to_str().unwrap()).unwrap();
    db.create_table(table_name.into(), num_columns, key_index);
    let table = db.get_table(table_name).unwrap();
    (temp_dir, db, table)
}

fn seed(table: &Arc<Table>, vals: &[i64]) {
    let q = Query::new(table.clone());
    let args: Vec<Option<i64>> = vals.iter().copied().map(Some).collect();
    assert!(q.insert(args).unwrap(), "seed insert failed");
}

fn read_row(table: &Arc<Table>, key: i64) -> Vec<Option<i64>> {
    let proj = vec![1i64; table.num_data_columns];
    let q = Query::new(table.clone());
    let rows = q.select(key, table.key_index, &proj).unwrap();
    assert_eq!(rows.len(), 1, "expected 1 row for key {key}");
    rows.into_iter().next().unwrap()
}

fn key_exists(table: &Arc<Table>, key: i64) -> bool {
    let proj = vec![1i64; table.num_data_columns];
    let q = Query::new(table.clone());
    q.select(key, table.key_index, &proj).unwrap().len() == 1
}

fn insert_op(table: &Arc<Table>, vals: &[i64]) -> QueryOp {
    QueryOp::Insert {
        table: table.clone(),
        args: vals.iter().copied().map(Some).collect(),
    }
}
fn update_op(table: &Arc<Table>, key: i64, cols: Vec<Option<i64>>) -> QueryOp {
    QueryOp::Update { table: table.clone(), key, cols }
}
fn delete_op(table: &Arc<Table>, key: i64) -> QueryOp {
    QueryOp::Delete { table: table.clone(), key }
}
fn select_op(table: &Arc<Table>, key: i64) -> QueryOp {
    QueryOp::Select {
        table: table.clone(),
        key,
        search_col: table.key_index,
        proj: vec![1i64; table.num_data_columns],
    }
}
fn select_version_op(table: &Arc<Table>, key: i64, version: i64) -> QueryOp {
    QueryOp::SelectVersion {
        table: table.clone(),
        key,
        search_col: table.key_index,
        proj: vec![1i64; table.num_data_columns],
        version,
    }
}
fn increment_op(table: &Arc<Table>, key: i64, col: usize) -> QueryOp {
    QueryOp::Increment { table: table.clone(), key, col }
}

fn run_txn(ops: Vec<QueryOp>) -> bool {
    Transaction::from_ops(ops).run()
}

#[test]
fn single_insert() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    assert!(run_txn(vec![insert_op(&t, &[1, 10, 20])]));
    assert_eq!(read_row(&t, 1), vec![Some(1), Some(10), Some(20)]);
}

#[test]
fn single_update() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 10, 20]);
    assert!(run_txn(vec![update_op(&t, 1, vec![None, Some(99), None])]));
    assert_eq!(read_row(&t, 1), vec![Some(1), Some(99), Some(20)]);
}

#[test]
fn single_delete() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 10, 20]);
    assert!(run_txn(vec![delete_op(&t, 1)]));
    assert!(!key_exists(&t, 1));
}

#[test]
fn insert_then_update() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    assert!(run_txn(vec![
        insert_op(&t, &[1, 10, 20]),
        update_op(&t, 1, vec![None, Some(99), None]),
    ]));
    assert_eq!(read_row(&t, 1), vec![Some(1), Some(99), Some(20)]);
}

#[test]
fn multiple_inserts() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    assert!(run_txn(vec![
        insert_op(&t, &[1, 10, 20]),
        insert_op(&t, &[2, 30, 40]),
    ]));
    assert_eq!(read_row(&t, 1), vec![Some(1), Some(10), Some(20)]);
    assert_eq!(read_row(&t, 2), vec![Some(2), Some(30), Some(40)]);
}

#[test]
fn rollback_undoes_insert() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[2, 99, 99]);
    assert!(!run_txn(vec![
        insert_op(&t, &[1, 10, 20]),
        insert_op(&t, &[2, 30, 40]),
    ]));
    assert!(!key_exists(&t, 1));
    assert_eq!(read_row(&t, 2), vec![Some(2), Some(99), Some(99)]);
}

#[test]
fn rollback_undoes_update() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 10, 20]);
    assert!(!run_txn(vec![
        update_op(&t, 1, vec![None, Some(99), Some(88)]),
        update_op(&t, 999, vec![None, Some(1), None]),
    ]));
    assert_eq!(read_row(&t, 1), vec![Some(1), Some(10), Some(20)]);
}

#[test]
fn concurrent_disjoint_keys_all_succeed() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    for i in 0..8i64 {
        seed(&t, &[i, i * 10, i * 100]);
    }

    let table = t.clone();
    let handles: Vec<_> = (0..8i64)
        .map(|i| {
            let t = table.clone();
            std::thread::spawn(move || {
                let ops = vec![
                    select_op(&t, i),
                    update_op(&t, i, vec![None, Some(i + 1000), None]),
                ];
                assert!(run_txn(ops), "txn on key {i} should succeed");
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    for i in 0..8i64 {
        assert_eq!(read_row(&t, i)[1], Some(i + 1000));
    }
}

#[test]
fn concurrent_conflicting_keys_retry_succeeds() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 10, 20]);

    let t1 = t.clone();
    let t2 = t.clone();

    let h1 = std::thread::spawn(move || loop {
        if run_txn(vec![update_op(&t1, 1, vec![None, Some(100), None])]) {
            break;
        }
        std::thread::yield_now();
    });

    let h2 = std::thread::spawn(move || loop {
        if run_txn(vec![update_op(&t2, 1, vec![None, Some(200), None])]) {
            break;
        }
        std::thread::yield_now();
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let val = read_row(&t, 1)[1];
    assert!(val == Some(100) || val == Some(200));
}

#[test]
fn concurrent_2pl_atomicity() {
    let (_tmp, _db, t) = new_test_db("t", 2, 0);
    seed(&t, &[1, 100]);
    seed(&t, &[2, 200]);

    let t1 = t.clone();
    let t2 = t.clone();

    let h1 = std::thread::spawn(move || {
        let ops = vec![
            update_op(&t1, 1, vec![Some(1), Some(101)]),
            update_op(&t1, 2, vec![Some(2), Some(201)]),
        ];
        loop {
            if Transaction::from_ops(ops.clone()).run() {
                break;
            }
            std::thread::yield_now();
        }
    });

    let h2 = std::thread::spawn(move || {
        let ops = vec![
            update_op(&t2, 2, vec![Some(2), Some(202)]),
            update_op(&t2, 1, vec![Some(1), Some(102)]),
        ];
        loop {
            if Transaction::from_ops(ops.clone()).run() {
                break;
            }
            std::thread::yield_now();
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let v1 = read_row(&t, 1)[1].unwrap();
    let v2 = read_row(&t, 2)[1].unwrap();
    assert!(
        (v1 == 101 && v2 == 201) || (v1 == 102 && v2 == 202),
        "non-serializable: key1={v1}, key2={v2}"
    );
}

#[test]
fn worker_concurrent_updates_serializable() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    for i in 0..20i64 {
        seed(&t, &[i, 0, 0]);
    }

    let mut workers: Vec<TransactionWorker> = (0..4).map(|_| TransactionWorker::new()).collect();
    for i in 0..20i64 {
        workers[i as usize % 4].add_transaction(vec![
            select_op(&t, i),
            update_op(&t, i, vec![None, Some(i + 100), Some(i + 200)]),
        ]);
    }

    for w in workers.iter_mut() {
        w.run();
    }
    for w in &workers {
        w.join();
    }

    for i in 0..20i64 {
        assert_eq!(
            read_row(&t, i),
            vec![Some(i), Some(i + 100), Some(i + 200)],
            "mismatch on key {i}"
        );
    }
}

#[test]
fn select_version_after_update() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 10, 20]);
    assert!(run_txn(vec![update_op(&t, 1, vec![None, Some(50), None])]));
    assert!(run_txn(vec![update_op(&t, 1, vec![None, Some(90), None])]));
    assert!(run_txn(vec![select_version_op(&t, 1, 0)]));
    assert!(run_txn(vec![select_version_op(&t, 1, -1)]));
    assert!(run_txn(vec![select_version_op(&t, 1, -2)]));
}
#[test]
fn many_ops_single_transaction() {
    let (_tmp, _db, t) = new_test_db("t", 3, 0);
    seed(&t, &[1, 0, 0]);
    let ops: Vec<QueryOp> = (0..100).map(|_| increment_op(&t, 1, 1)).collect();
    assert!(run_txn(ops));
    assert_eq!(read_row(&t, 1)[1], Some(100));
}