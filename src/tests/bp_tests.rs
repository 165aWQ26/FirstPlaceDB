use crate::tests::setup_tests::{
    assert_select_eq, assert_select_version_eq, bulk_insert, make_record, persistence_round_trip,
    record, setup_query, setup_test_db, setup_test_table, sparse_update, updates_from,
};

// can we pull tables from disk
#[test]
fn read_from_file_insert() {
    let (mut db, dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();
        for i in 0..4 {
            bulk_insert(&mut q, i, 3);
        }
    }
    persistence_round_trip(&mut db);
    let mut q = setup_query(&mut db).unwrap();
    assert_select_eq(&mut q, 1, 1, make_record(1, 1));
    assert_select_eq(&mut q, 3, 3, make_record(3, 3));

    db.close().expect("close failed");
}

#[test]
fn read_from_file_insert_10000() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();
        bulk_insert(&mut q, 10000, 3);
    }
    persistence_round_trip(&mut db);
    let mut q = setup_query(&mut db).unwrap();
    assert_select_eq(&mut q, 321, 3, make_record(321, 3));
    assert_select_eq(&mut q, 632, 3, make_record(632, 3));

    db.close().expect("close failed");
}

#[test]
fn read_updates_from_file() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();

        q.insert(record(&[10, 20, 30])).unwrap();
        q.insert(record(&[20, 40, 60])).unwrap();

        q.update(10, sparse_update(2, 31, 3)).unwrap();
        q.update(20, updates_from(&[(1, 41), (2, 61)], 3)).unwrap();
    }

    persistence_round_trip(&mut db);

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    {
        let mut q = setup_query(&mut db).unwrap();
        // let result1 = q.select(10, 0, &mask).unwrap();
        // let result2 = q.select(20, 0, &mask).unwrap();
        //
        // assert_eq!(result1.len(), 1);
        // assert_eq!(result1[0], vec![Some(10), Some(20), Some(31)]);
        //
        // assert_eq!(result2.len(), 1);
        // assert_eq!(result2[0], vec![Some(20), Some(41), Some(61)]);
        assert_select_eq(&mut q, 10, 3, record(&[10, 20, 31]));
        assert_select_eq(&mut q, 20, 3, record(&[20, 41, 61]));
    }

    db.close().expect("close failed");
}

#[test]
fn read_1000_updates_from_file() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();

        q.insert(record(&[10, 20, 30])).unwrap();

        for i in 0..1000 {
            q.update(10, updates_from(&[(1, 20 + i), (2, 30 + i)], 3))
                .unwrap();
        }
    }

    persistence_round_trip(&mut db);

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    {
        let mut q = setup_query(&mut db).unwrap();
        let result = q.select(10, 0, &mask).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], record(&[10,20 + 999, 30 + 999]));
    }

    db.close().expect("close failed");
}

#[test]
fn read_uneven_number_read_and_updates() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();

        for i in 0..1000 {
            q.insert(vec![Some(i), Some(i + 1), Some(i + 2)]).unwrap();
        }

        for i in 0..1000 {
            for j in i..1000 {
                q.update(i, vec![None, Some(i + 1), Some(i + j)]).unwrap();
            }
        }
    }

    persistence_round_trip(&mut db);

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    {
        let mut q = setup_query(&mut db).unwrap();
        let result = q.select(10, 0, &mask).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![Some(10), Some(11), Some(1009)]);
    }

    db.close().expect("close failed");
}

#[test]
fn read_select_version() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();

        q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();
        q.update(10, vec![None, Some(21), Some(31)]).unwrap();
        q.update(10, vec![None, Some(22), Some(32)]).unwrap();
        q.update(10, vec![None, Some(23), Some(33)]).unwrap();
        q.update(10, vec![None, Some(24), Some(34)]).unwrap();
        q.update(10, vec![None, Some(25), Some(35)]).unwrap();
    }
    persistence_round_trip(&mut db);

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let mut q = setup_query(&mut db).unwrap();
    let result = q.select_version(10, 0, &mask, -2).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(23), Some(33)]);

    db.close().expect("close failed");
}

#[test]
fn read_select_version_across_pages() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    {
        let mut q = setup_query(&mut db).unwrap();

        q.insert(vec![Some(10), Some(20), Some(30)]).unwrap();
        q.insert(vec![Some(11), Some(12), Some(13)]).unwrap();
        q.update(10, vec![None, Some(21), Some(31)]).unwrap();
        q.update(10, vec![None, Some(22), Some(32)]).unwrap();
        q.update(10, vec![None, Some(23), Some(33)]).unwrap();
        for i in 0..10000 {
            q.update(11, vec![Some(i), Some(i + 1), Some(i + 2)])
                .unwrap();
        }
        q.update(10, vec![None, Some(24), Some(34)]).unwrap();
        for i in 0..10000 {
            q.update(11, vec![None, Some(12 + i), Some(13 + i)])
                .unwrap();
        }
        q.update(10, vec![None, Some(25), Some(35)]).unwrap();
    }
    persistence_round_trip(&mut db);

    // select the record that we pulled wow
    let mask = [1i64, 1, 1];
    let mut q = setup_query(&mut db).unwrap();
    let result = q.select_version(10, 0, &mask, -2).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![Some(10), Some(23), Some(33)]);

    db.close().expect("close failed");
}

#[test]
fn select_version_disjoint_column_updates() {
    let (mut db, _dir) = setup_test_table("test", 3, 0);
    let mut q = setup_query(&mut db).unwrap();
    q.insert(vec![Some(100), Some(10), Some(20)]).unwrap();

    q.update(100, sparse_update(1, 11, 3)).unwrap();
    q.update(100, sparse_update(2, 22, 3)).unwrap();

    assert_select_version_eq(&mut q, 100, 3, 0, vec![Some(100), Some(11), Some(22)]);
    assert_select_version_eq(&mut q, 100, 3, -1, vec![Some(100), Some(11), Some(20)]);
    assert_select_version_eq(&mut q, 100, 3, -2, vec![Some(100), Some(10), Some(20)]);

    db.close().expect("close failed");
}
