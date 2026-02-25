use crate::db::Database;
use crate::query::Query;
use tempfile::{tempdir, TempDir};

pub(crate) fn setup_test_db() -> (Database, TempDir) {
    let dir = tempdir().unwrap();
    let mut db = Database::new();
    db.open(dir.path().to_str().unwrap());
    (db, dir)
}

pub(crate) fn setup_test_table(
    name: &str,
    num_columns: usize,
    key_index: usize,
) -> (Database, TempDir) {
    let (mut db, dir) = setup_test_db();
    db.create_table(name.to_string(), num_columns, key_index);
    (db, dir)
}

pub(crate) fn setup_default_table() -> (Database, TempDir) {
    setup_test_table("test", 5, 0)
}

/// Close query and reopen db
pub(crate) fn persistence_round_trip(db: &mut Database) {
    db.close().expect("close failed");
    db.get_table("test").expect("failed to reload table");
}

pub(crate) fn setup_query(db: &mut Database) -> Option<Query<'_>> {
    if let Ok(Some(table)) = db.get_table(&String::from("test")) {
        return Some(Query::new(table));
    }
    None
}

pub(crate) fn all_columns_mask(num_cols: usize) -> Vec<i64> {
    vec![1i64; num_cols]
}
/// Make records with incremental value
pub(crate) fn make_record(start: i64, num_cols: usize) -> Vec<Option<i64>> {
    (0..num_cols as i64).map(|i| Some(start + i)).collect()
}
/// Make a record but BYO values
pub(crate) fn record(vals: &[i64]) -> Vec<Option<i64>> {
    vals.iter().map(|&v| Some(v)).collect()
}

/// Batches insert with the same value
pub(crate) fn bulk_insert(q: &mut Query, count: i64, num_cols: usize) {
    for i in 0..count {
        q.insert(make_record(i, num_cols)).unwrap();
    }
}

pub(crate) fn updates_from(cols: &[(usize, i64)], num_cols: usize) -> Vec<Option<i64>> {
    let mut record = vec![None; num_cols];
    for &(col, val) in cols {
        record[col] = Some(val);
    }
    record
}

pub(crate) fn sparse_update(col: usize, value: i64, num_cols: usize) -> Vec<Option<i64>> {
    let mut record = vec![None; num_cols];
    record[col] = Some(value);
    record
}

pub(crate) fn assert_select_eq(
    q: &mut Query,
    key: i64,
    num_cols: usize,
    expected: Vec<Option<i64>>,
) {
    let mask = all_columns_mask(num_cols);
    let result = q.select(key, 0, &mask).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], expected);
}

pub(crate) fn assert_select_version_eq(
    q: &mut Query,
    key: i64,
    num_cols: usize,
    version: i64,
    expected: Vec<Option<i64>>,
) {
    let mask = all_columns_mask(num_cols);
    let result = q.select_version(key, 0, &mask, version).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], expected);
}
