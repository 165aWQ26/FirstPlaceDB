use crate::error::DbError;
use crate::query::Query;
use crate::table::Table;

fn setup(num_columns: usize) -> Query {
    let table = Table::new(String::from("test"), num_columns, 0);
    Query::new(table)
}

#[test]
fn insert_and_select() {
    let mut q = setup(3);
    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20), Some(30)]);
}

#[test]
fn insert_duplicate_key_fails() {
    let mut q = setup(3);
    assert!(q.insert(vec![Some(1), Some(2), Some(3)]).unwrap());
    assert!(!q.insert(vec![Some(1), Some(5), Some(6)]).unwrap());
}

#[test]
fn update_and_select() {
    let mut q = setup(4);
    q.insert(vec![Some(1), Some(2), Some(3), Some(4)]).unwrap();

    // Update columns 1 and 3
    q.update(1, vec![None, Some(20), None, Some(40)]).unwrap();

    let mask = [1i64, 1, 1, 1];
    let result = q.select(1, 0, &mask).unwrap();
    assert_eq!(result[0], vec![Some(1), Some(20), Some(3), Some(40)]);
}

#[test]
fn delete_removes_from_index() {
    let mut q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.delete(1).unwrap();
    assert!(q.table.indices[0].locate(1).is_none());
}

#[test]
fn select_deleted_key_fails() {
    let mut q = setup(3);
    q.insert(vec![Some(1), Some(2), Some(3)]).unwrap();
    q.delete(1).unwrap();

    let mask = [1i64, 1, 1];
    assert_eq!(q.select(1, 0, &mask), Err(DbError::KeyNotFound(1)));
}

#[test]
fn sum_range() {
    let mut q = setup(3);
    q.insert(vec![Some(1), Some(10), Some(100)]).unwrap();
    q.insert(vec![Some(2), Some(20), Some(200)]).unwrap();
    q.insert(vec![Some(3), Some(30), Some(300)]).unwrap();

    // Sum column 1 for keys 1..3
    assert_eq!(q.sum(1, 3, 1).unwrap(), 60);
    // Sum column 2 for keys 1..2
    assert_eq!(q.sum(1, 2, 2).unwrap(), 300);
}

#[test]
fn increment() {
    let mut q = setup(3);
    q.insert(vec![Some(1), Some(10), Some(100)]).unwrap();

    q.increment(1, 1).unwrap(); // col 1: 10 → 11
    q.increment(1, 1).unwrap(); // col 1: 11 → 12

    let mask = [1i64, 1, 1];
    let result = q.select(1, 0, &mask).unwrap();
    assert_eq!(result[0][1], Some(12));
    // Other columns unchanged
    assert_eq!(result[0][0], Some(1));
    assert_eq!(result[0][2], Some(100));
}

// Keep the original integration test
#[test]
fn quick_test_all() {
    let table: Table = Table::new(String::from("test"), 5, 0);
    let mut query: Query = Query::new(table);

    let rec_one: Vec<Option<i64>> = vec![Some(1); 5];
    let rec_two: Vec<Option<i64>> = vec![Some(2); 5];
    let rec_three: Vec<Option<i64>> = vec![Some(3), Some(4), Some(5), Some(6), Some(7)];
    let rec_four: Vec<Option<i64>> = vec![Some(4), Some(5), Some(6), Some(7), Some(8)];

    query.insert(rec_one).unwrap();
    query.insert(rec_two).unwrap();

    let rid1 = query.table.indices[0].locate(1).unwrap();
    assert_eq!(query.table.read(rid1), Ok(vec![Some(1); 5]));

    let rid2 = query.table.indices[0].locate(2).unwrap();
    assert_eq!(query.table.read(rid2), Ok(vec![Some(2); 5]));

    query.insert(rec_three).unwrap();

    // key 1: col3=1, key 2: col3=2, key 3: col3=6 → 1+2+6=9
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

    // After increment(2,0): key 2→3. After increment(1,0): key 1→2.
    let full_mask: [i64; 5] = [1, 1, 1, 1, 1];
    let ans_list_two: Vec<Vec<Option<i64>>> = query.select(2, 0, &full_mask).unwrap();
    assert_eq!(ans_list_two[0][0], Some(2));

    let ans_list_three: Vec<Vec<Option<i64>>> = query.select(3, 0, &full_mask).unwrap();
    assert_eq!(ans_list_three[0][0], Some(3));
}
