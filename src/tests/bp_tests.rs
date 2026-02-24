use crate::tests::setup_tests::{setup_db, setup_query};

// can we pull our tables from disk
#[test]
fn read_from_file() {

    let mut db = setup_db(3);
    let q =  setup_query(&mut db).unwrap();

    q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();
    db.close().expect("We have a problem.");




    let mask = [1i64, 1, 1];
    let result = q.select(10, 0, &mask).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(20), Some(30)]);

    db.close().expect("close failed");
}





