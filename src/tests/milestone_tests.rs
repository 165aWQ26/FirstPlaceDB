use crate::db::Database;
use crate::query::Query;
use crate::tests::setup_tests::all_columns_mask;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;

/// Convert plain i64 slice to Option vec (matches select output format)
fn to_opt(vals: &[i64]) -> Vec<Option<i64>> {
    vals.iter().map(|&v| Some(v)).collect()
}

//  M1 TESTER — insert 1000 unique-key records, select all,
//              update cols 2-4 one-at-a-time, sum random ranges
#[test]
fn m1_tester() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::new();
    db.open(dir.path().to_str().unwrap());
    db.create_table("Grades".to_string(), 5, 0);
    let table = db.tables.get_mut("Grades").unwrap();
    let mut q = Query::new(table);

    let mut rng = StdRng::seed_from_u64(3562901);
    let mut records: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let number_of_records: i64 = 1000;
    let number_of_aggregates = 100;

    // ── Insert ──
    for _ in 0..number_of_records {
        let mut key = 92106429 + rng.gen_range(0..=number_of_records);
        while records.contains_key(&key) {
            key = 92106429 + rng.gen_range(0..=number_of_records);
        }
        let vals = vec![
            key,
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
        ];
        q.insert(to_opt(&vals)).unwrap();
        records.insert(key, vals);
    }

    // ── Select all ──
    let mask = all_columns_mask(5);
    for (&key, expected) in &records {
        let result = q.select(key, 0, &mask).unwrap();
        assert_eq!(result.len(), 1, "select on key {key}");
        assert_eq!(result[0], to_opt(expected), "select mismatch on key {key}");
    }

    // ── Update cols 2-4 one at a time, verify after each ──
    let ordered_keys: Vec<i64> = records.keys().copied().collect();
    for &key in &ordered_keys {
        for i in 2..5usize {
            let value: i64 = rng.gen_range(0..=20);
            let mut updated_columns: Vec<Option<i64>> = vec![None; 5];
            updated_columns[i] = Some(value);
            records.get_mut(&key).unwrap()[i] = value;

            q.update(key, updated_columns).unwrap();

            let result = q.select(key, 0, &mask).unwrap();
            assert_eq!(
                result[0],
                to_opt(&records[&key]),
                "update error on key {key} col {i}"
            );
        }
    }

    // ── Sum random ranges on every column ──
    let keys: Vec<i64> = records.keys().copied().collect();
    for c in 0..5usize {
        for _ in 0..number_of_aggregates {
            let mut r = [rng.gen_range(0..keys.len()), rng.gen_range(0..keys.len())];
            r.sort();
            let column_sum: i64 = keys[r[0]..=r[1]].iter().map(|k| records[k][c]).sum();
            let result = q.sum(keys[r[0]], keys[r[1]], c).unwrap();
            assert_eq!(
                result, column_sum,
                "sum error on [{}, {}] col {c}",
                keys[r[0]], keys[r[1]]
            );
        }
    }

    db.close().expect("close failed");
}

//  M1 TESTER NEW — versioned select & sum after a single bulk update
#[test]
fn m1_tester_versioned() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::new();
    db.open(dir.path().to_str().unwrap());
    db.create_table("Grades".to_string(), 5, 0);
    let table = db.tables.get_mut("Grades").unwrap();
    let mut q = Query::new(table);

    let mut rng = StdRng::seed_from_u64(3562901);
    let mut records: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let number_of_records: i64 = 1000;
    let number_of_aggregates = 100;

    // ── Insert ──
    for _ in 0..number_of_records {
        let mut key = 92106429 + rng.gen_range(0..=number_of_records);
        while records.contains_key(&key) {
            key = 92106429 + rng.gen_range(0..=number_of_records);
        }
        let vals = vec![
            key,
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
            rng.gen_range(0..=20),
        ];
        q.insert(to_opt(&vals)).unwrap();
        records.insert(key, vals);
    }

    let mask = all_columns_mask(5);

    // ── select_version(-1) on insert-only records → returns base ──
    for (&key, expected) in &records {
        let result = q.select_version(key, 0, &mask, -1).unwrap();
        assert_eq!(
            result[0],
            to_opt(expected),
            "select_version(-1) pre-update mismatch on key {key}"
        );
    }

    // ── One bulk update per key (cols 2-4 at once) ──
    let mut updated_records = records.clone();
    let ordered_keys: Vec<i64> = records.keys().copied().collect();
    for &key in &ordered_keys {
        let mut updated_columns: Vec<Option<i64>> = vec![None; 5];
        for i in 2..5usize {
            let value: i64 = rng.gen_range(0..=20);
            updated_columns[i] = Some(value);
            updated_records.get_mut(&key).unwrap()[i] = value;
        }
        q.update(key, updated_columns).unwrap();

        // version -1: original
        let result = q.select_version(key, 0, &mask, -1).unwrap();
        assert_eq!(
            result[0],
            to_opt(&records[&key]),
            "version -1 mismatch on key {key}"
        );

        // version -2: also original (only 1 update, clamps to base)
        let result = q.select_version(key, 0, &mask, -2).unwrap();
        assert_eq!(
            result[0],
            to_opt(&records[&key]),
            "version -2 mismatch on key {key}"
        );

        // version 0: updated
        let result = q.select_version(key, 0, &mask, 0).unwrap();
        assert_eq!(
            result[0],
            to_opt(&updated_records[&key]),
            "version 0 mismatch on key {key}"
        );
    }

    // ── sum_version checks ──
    let keys: Vec<i64> = records.keys().copied().collect();
    for c in 0..5usize {
        for _ in 0..number_of_aggregates {
            let mut r = [rng.gen_range(0..keys.len()), rng.gen_range(0..keys.len())];
            r.sort();

            // version -1: original values
            let orig_sum: i64 = keys[r[0]..=r[1]].iter().map(|k| records[k][c]).sum();
            let result = q.sum_version(keys[r[0]], keys[r[1]], c, -1).unwrap();
            assert_eq!(result, orig_sum, "sum_version(-1) col {c}");

            // version -2: still original
            let result = q.sum_version(keys[r[0]], keys[r[1]], c, -2).unwrap();
            assert_eq!(result, orig_sum, "sum_version(-2) col {c}");

            // version 0: updated values
            let upd_sum: i64 = keys[r[0]..=r[1]]
                .iter()
                .map(|k| updated_records[k][c])
                .sum();
            let result = q.sum_version(keys[r[0]], keys[r[1]], c, 0).unwrap();
            assert_eq!(result, upd_sum, "sum_version(0) col {c}");
        }
    }

    db.close().expect("close failed");
}

//  M2 PART 1 + PART 2 — persistence round-trip
//    Part 1: insert 1000, update 2 rounds per-col, sum, close
//    Part 2: fresh db, read from disk, verify all, delete 100, close
#[test]
fn m2_tester_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    let mut rng = StdRng::seed_from_u64(3562901);
    let mut records: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let number_of_records: i64 = 1000;
    let number_of_aggregates = 100;
    let number_of_updates = 2;

    // ── Part 1: insert → update → close ──
    {
        let mut db = Database::new();
        db.open(path);
        db.create_table("Grades".to_string(), 5, 0);
        let table = db.tables.get_mut("Grades").unwrap();
        let mut q = Query::new(table);

        for i in 0..number_of_records {
            let key = 92106429 + i;
            let vals = vec![
                key,
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
            ];
            q.insert(to_opt(&vals)).unwrap();
            records.insert(key, vals);
        }

        let keys: Vec<i64> = records.keys().copied().collect();
        let mask = all_columns_mask(5);

        // Verify inserts
        for &key in &keys {
            let result = q.select(key, 0, &mask).unwrap();
            assert_eq!(result[0], to_opt(&records[&key]), "p1 select key {key}");
        }

        // N rounds of per-column updates
        for _ in 0..number_of_updates {
            for &key in &keys {
                for i in 2..5usize {
                    let value: i64 = rng.gen_range(0..=20);
                    let mut upd: Vec<Option<i64>> = vec![None; 5];
                    upd[i] = Some(value);
                    records.get_mut(&key).unwrap()[i] = value;
                    q.update(key, upd).unwrap();

                    let result = q.select(key, 0, &mask).unwrap();
                    assert_eq!(
                        result[0],
                        to_opt(&records[&key]),
                        "p1 update key {key} col {i}"
                    );
                }
            }
        }

        // Sum on col 0
        for _ in 0..number_of_aggregates {
            let mut r = [rng.gen_range(0..keys.len()), rng.gen_range(0..keys.len())];
            r.sort();
            let expected: i64 = keys[r[0]..=r[1]].iter().map(|k| records[k][0]).sum();
            let result = q.sum(keys[r[0]], keys[r[1]], 0).unwrap();
            assert_eq!(result, expected, "p1 sum");
        }

        db.close().expect("p1 close failed");
    }

    // ── Part 2: fresh db → read from disk → verify → delete → close ──
    {
        let mut db = Database::new();
        db.open(path);
        let table = db.get_table("Grades").expect("get_table").unwrap();
        let mut q = Query::new(table);

        let keys: Vec<i64> = records.keys().copied().collect();
        let mask = all_columns_mask(5);

        // Verify persisted records match expected state
        for &key in &keys {
            let result = q.select(key, 0, &mask).unwrap();
            assert_eq!(result[0], to_opt(&records[&key]), "p2 select key {key}");
        }

        // Sum on col 0
        let mut rng2 = StdRng::seed_from_u64(99999);
        for _ in 0..number_of_aggregates {
            let mut r = [rng2.gen_range(0..keys.len()), rng2.gen_range(0..keys.len())];
            r.sort();
            let expected: i64 = keys[r[0]..=r[1]].iter().map(|k| records[k][0]).sum();
            let result = q.sum(keys[r[0]], keys[r[1]], 0).unwrap();
            assert_eq!(result, expected, "p2 sum");
        }

        // Delete 100 random keys
        let delete_keys: Vec<i64> = {
            let mut rng3 = StdRng::seed_from_u64(42);
            let mut ks = keys.clone();
            for i in 0..100 {
                let j = rng3.gen_range(i..ks.len());
                ks.swap(i, j);
            }
            ks[..100].to_vec()
        };
        for &key in &delete_keys {
            q.delete(key).unwrap();
        }

        db.close().expect("p2 close failed");
    }
}

//  M2 NEW PART 1 + PART 2 — versioned persistence round-trip
//    Part 1: insert 1000, 1 bulk update round, close
//    Part 2: fresh db, version selects & sums from disk, delete, close
#[test]
fn m2_tester_versioned_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    let mut rng = StdRng::seed_from_u64(3562901);
    let mut records: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let mut updated_records: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    let number_of_records: i64 = 1000;
    let number_of_aggregates = 100;

    // ── Part 1: insert → 1 bulk update → verify → close ──
    {
        let mut db = Database::new();
        db.open(path);
        db.create_table("Grades".to_string(), 5, 0);
        let table = db.tables.get_mut("Grades").unwrap();
        let mut q = Query::new(table);

        for i in 0..number_of_records {
            let key = 92106429 + i;
            let vals = vec![
                key,
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
                rng.gen_range(0..=20),
            ];
            q.insert(to_opt(&vals)).unwrap();
            records.insert(key, vals);
        }

        let keys: Vec<i64> = records.keys().copied().collect();
        let mask = all_columns_mask(5);

        // 1 round: update cols 2-4 all at once
        for &key in &keys {
            let mut upd: Vec<Option<i64>> = vec![None; 5];
            let mut updated = records[&key].clone();
            for i in 2..5usize {
                let value: i64 = rng.gen_range(0..=20);
                upd[i] = Some(value);
                updated[i] = value;
            }
            updated_records.insert(key, updated);
            q.update(key, upd).unwrap();
        }

        // Verify latest = updated
        for &key in &keys {
            let result = q.select(key, 0, &mask).unwrap();
            assert_eq!(
                result[0],
                to_opt(&updated_records[&key]),
                "p1 latest key {key}"
            );
        }

        db.close().expect("p1 close failed");
    }

    // ── Part 2: fresh db → read from disk → version checks → delete → close ──
    {
        let mut db = Database::new();
        db.open(path);
        let table = db.get_table("Grades").expect("get_table").unwrap();
        let mut q = Query::new(table);

        let keys: Vec<i64> = records.keys().copied().collect();
        let mask = all_columns_mask(5);

        // version -1: original (pre-update)
        for &key in &keys {
            let result = q.select_version(key, 0, &mask, -1).unwrap();
            assert_eq!(
                result[0],
                to_opt(&records[&key]),
                "p2 version(-1) key {key}"
            );
        }

        // version -2: still original (only 1 update)
        for &key in &keys {
            let result = q.select_version(key, 0, &mask, -2).unwrap();
            assert_eq!(
                result[0],
                to_opt(&records[&key]),
                "p2 version(-2) key {key}"
            );
        }

        // version 0: updated
        for &key in &keys {
            let result = q.select_version(key, 0, &mask, 0).unwrap();
            assert_eq!(
                result[0],
                to_opt(&updated_records[&key]),
                "p2 version(0) key {key}"
            );
        }

        // sum_version(-1) on col 0
        let mut rng2 = StdRng::seed_from_u64(77777);
        for _ in 0..number_of_aggregates {
            let mut r = [rng2.gen_range(0..keys.len()), rng2.gen_range(0..keys.len())];
            r.sort();
            let expected: i64 = keys[r[0]..=r[1]].iter().map(|k| records[k][0]).sum();
            let result = q.sum_version(keys[r[0]], keys[r[1]], 0, -1).unwrap();
            assert_eq!(result, expected, "p2 sum_version(-1)");
        }

        // sum_version(0) on col 0
        let mut rng3 = StdRng::seed_from_u64(88888);
        for _ in 0..number_of_aggregates {
            let mut r = [rng3.gen_range(0..keys.len()), rng3.gen_range(0..keys.len())];
            r.sort();
            let expected: i64 = keys[r[0]..=r[1]]
                .iter()
                .map(|k| updated_records[k][0])
                .sum();
            let result = q.sum_version(keys[r[0]], keys[r[1]], 0, 0).unwrap();
            assert_eq!(result, expected, "p2 sum_version(0)");
        }

        // Delete 100 random keys
        let delete_keys: Vec<i64> = {
            let mut rng4 = StdRng::seed_from_u64(42);
            let mut ks = keys.clone();
            for i in 0..100 {
                let j = rng4.gen_range(i..ks.len());
                ks.swap(i, j);
            }
            ks[..100].to_vec()
        };
        for &key in &delete_keys {
            q.delete(key).unwrap();
        }

        db.close().expect("p2 close failed");
    }
}
