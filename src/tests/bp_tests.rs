use crate::tests::setup_tests::{setup_db, setup_query};

// can we pull tables from disk
#[test]
fn read_from_file_insert() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();
    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20), Some(30)]);

    db.close().expect("close failed");
}

#[test]
fn read_from_file_insert_1000() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    for i in 0..1000 {
        q.insert(vec![Some(i), Some(i + 1), Some(i + 2)]).unwrap();
    }
    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result1 = q.select(321, 0, &mask).unwrap();
    let result2 = q.select(632, 0, &mask).unwrap();

    assert_eq!(result1.len(), 1);
    assert_eq!(result2.len(), 1);
    assert_eq!(result1[0], vec![Some(321), Some(322), Some(333)]);
    assert_eq!(result2[1], vec![Some(321), Some(322), Some(333)]);

    db.close().expect("close failed");
}

#[test]
fn read_updates_from_file() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();
    q.insert(vec![Some(20), Some(40), Some(60)]).unwrap();

    q.update(10, vec![None, Some(21), Some(31)]).unwrap();
    q.update(20, vec![None, Some(41), Some(61)]).unwrap();

    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result1 = q.select(10, 0, &mask).unwrap();
    let result2 = q.select(20, 0, &mask).unwrap();

    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0], vec![Some(10), Some(21), Some(31)]);

    assert_eq!(result2.len(), 1);
    assert_eq!(result2[0], vec![Some(10), Some(41), Some(61)]);

    db.close().expect("close failed");
}

#[test]
fn read_1000_updates_from_file() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();

    for i in 0..1000 {
        q.update(10, vec![None, Some(20 + i), Some(30 + i)])
            .unwrap();
    }

    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20 + 999), Some(30 + 999)]);

    db.close().expect("close failed");
}

#[test]
fn read_from_file_1000_inserts_1_update_() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    for i in 0..1000 {
        q.insert(vec![Some(i), Some(i + 1), Some(i + 2)]).unwrap();
    }

    for i in 0..1000 {
        for j in i..1000 {
            q.update(i, vec![None, Some(i + 1), Some(i + j)]).unwrap();
        }
    }

    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20 + 999), Some(30 + 999)]);

    db.close().expect("close failed");
}

#[test]
fn read_uneven_number_read_and_updates() {
    let mut db = setup_db(3);
    let mut q = setup_query(&mut db).unwrap();

    for i in 0..1000 {
        q.insert(vec![Some(i), Some(i + 1), Some(i + 2)]).unwrap();
    }

    for i in 0..1000 {
        for j in i..1000 {
            q.update(i, vec![None, Some(i + 1), Some(i + j)]).unwrap();
        }
    }

    db.close().expect("We have a problem.");

    // see if we can pull the record from disk
    db.open("./ECS165");
    db.get_table("test").expect("andrew fucked something up");

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20 + 999), Some(30 + 999)]);

    db.close().expect("close failed");
}
