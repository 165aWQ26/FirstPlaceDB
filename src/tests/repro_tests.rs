/// Tests that reproduce the exact Python tester failure scenarios.
/// Each test mirrors a specific Python tester to isolate the bug.
use crate::db::Database;
use crate::query::Query;
use crate::tests::setup_tests::{setup_query, setup_test_table};
// __main__.py — insert 10k then update loses evicted pages

#[test]
fn repro_no_open_insert_then_update() {
    // Mirrors __main__.py: no db.open(), insert 10k, then update
    let mut db = Database::new();
    db.create_table("Grades".to_string(), 5, 0);
    let table = db.tables.get_mut("Grades").unwrap();
    let mut q = Query::new(table);

    for i in 0..10000i64 {
        q.insert(vec![
            Some(906659671 + i),
            Some(93),
            Some(0),
            Some(0),
            Some(0),
        ])
        .unwrap();
    }

    // Update a base record that was evicted from the
    // 32-frame LRU cache. Since there's no disk path, the evicted page
    // is lost -> IndexOutOfBounds.
    q.update(906659671, vec![None, Some(99), None, None, None])
        .unwrap();

    let mask = vec![1i64; 5];
    let result = q.select(906659671, 0, &mask).unwrap();
    assert_eq!(result[0][1], Some(99));
}

#[test]
fn repro_no_open_insert_then_select_early_key() {
    // Mirrors m1_tester.py failure: 1000 inserts + updates, then
    // select on an early key whose base page got evicted
    let (mut table,name, _dir) = setup_test_table("Grades", 5, 0);
    let mut q = setup_query(&mut table, name).unwrap();

    for i in 0..1000i64 {
        q.insert(vec![Some(i), Some(1), Some(2), Some(3), Some(4)])
            .unwrap();
    }

    // Do 3 updates per key (like m1_tester) to generate tail pressure
    for i in 0..1000i64 {
        for col in 2..5usize {
            let mut upd: Vec<Option<i64>> = vec![None; 5];
            upd[col] = Some(99);
            q.update(i, upd).unwrap();
        }
    }

    // Select first key — its base page was likely evicted
    let mask = vec![1i64; 5];
    let result = q.select(0, 0, &mask).unwrap();
    assert_eq!(result[0][0], Some(0));
    assert_eq!(result[0][2], Some(99));
    assert_eq!(result[0][3], Some(99));
    assert_eq!(result[0][4], Some(99));
}
