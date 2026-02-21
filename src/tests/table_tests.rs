use crate::query::Query;
use crate::table::Table;

fn setup(num_columns: usize) -> Query {
    let table = Table::new(String::from("test"), num_columns, 0);
    Query::new(table)
}

#[test]
fn insert_and_read_latest() {
    let mut q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    let rid = q.table.rid_for_unique_key(10).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(result, vec![Some(10), Some(20), Some(30)]);
}

#[test]
fn read_latest_follows_update_chain() {
    let mut q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    // Update column 1 only
    q.update(10, vec![None, Some(99), None]).unwrap();

    let rid = q.table.rid_for_unique_key(10).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(result, vec![Some(10), Some(99), Some(30)]);
}

#[test]
fn read_latest_multiple_updates() {
    let mut q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();

    // First update: column 1
    q.update(1, vec![None, Some(20), None, None]).unwrap();
    // Second update: column 2
    q.update(1, vec![None, None, Some(30), None]).unwrap();

    let rid = q.table.rid_for_unique_key(1).unwrap();
    let result = q.table.read_latest(rid).unwrap();
    assert_eq!(result, vec![Some(1), Some(20), Some(30), Some(4)]);
}

#[test]
fn read_latest_projected() {
    let mut q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();
    q.update(1, vec![None, Some(99), None, None]).unwrap();

    let rid = q.table.rid_for_unique_key(1).unwrap();
    let projected = [1, 0, 1, 0]; // columns 0 and 2
    let result = q.table.read_latest_projected(&projected, rid).unwrap();
    assert_eq!(result, vec![Some(1), None, Some(3), None]);
}

#[test]
fn read_latest_single() {
    let mut q = setup(3);
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();
    q.update(5, vec![None, Some(60), None]).unwrap();

    let rid = q.table.rid_for_unique_key(5).unwrap();
    assert_eq!(q.table.read_latest_single(rid, 0).unwrap(), Some(5));
    assert_eq!(q.table.read_latest_single(rid, 1).unwrap(), Some(60));
    assert_eq!(q.table.read_latest_single(rid, 2).unwrap(), Some(7));
}


#[test]
fn index_being_weird() {
    let mut q = setup(3);
    q.insert(vec![Some(5), Some(6), Some(7)]).unwrap();

    let rid = q.table.rid_for_unique_key(5).unwrap();
    assert_eq!(q.table.read_latest_single(rid, 0).unwrap(), Some(5));
}
