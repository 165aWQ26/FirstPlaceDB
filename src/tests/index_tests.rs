
#[cfg(test)]
mod tests {
    use crate::index::Index;
    // ===== unique TESTS =====
    #[test]
    fn test_unique_insert_and_locate() {
        let p = Index::new_unique();
        p.insert(10, 100);
        assert_eq!(p.locate(10), Some(100));
    }

    #[test]
    fn test_unique_locate_missing() {
        let p = Index::new_unique();
        assert!(p.locate(99).is_none());
    }

    #[test]
    fn test_unique_insert_unique_success() {
        let p = Index::new_unique();
        assert!(p.insert_unique(1, 100));
        assert_eq!(p.locate(1), Some(100));
    }

    #[test]
    fn test_unique_insert_unique_duplicate() {
        let p = Index::new_unique();
        p.insert_unique(1, 100);
        assert!(!p.insert_unique(1, 200)); // Duplicate rejected
        assert_eq!(p.locate(1), Some(100)); // Original unchanged
    }

    #[test]
    fn test_unique_insert_overwrites() {
        let p = Index::new_unique();
        p.insert(1, 100);
        p.insert(1, 999);
        assert_eq!(p.locate(1), Some(999));
    }

    #[test]
    fn test_unique_remove() {
        let p = Index::new_unique();
        p.insert(1, 100);
        p.remove(1, 100);
        assert!(p.locate(1).is_none());
    }

    #[test]
    fn test_unique_locate_range_all() {
        let p = Index::new_unique();
        p.insert(1, 10);
        p.insert(2, 20);
        p.insert(3, 30);
        assert_eq!(p.locate_range(1, 3), vec![10, 20, 30]);
    }

    #[test]
    fn test_unique_locate_range_partial() {
        let p = Index::new_unique();
        p.insert(1, 10);
        p.insert(3, 30);
        p.insert(5, 50);
        assert_eq!(p.locate_range(2, 4), vec![30]);
    }

    #[test]
    fn test_unique_locate_range_none() {
        let p = Index::new_unique();
        p.insert(1, 10);
        assert!(p.locate_range(5, 10).is_empty());
    }

    #[test]
    fn test_unique_locate_range_inclusive_boundaries() {
        let p = Index::new_unique();
        p.insert(1, 10);
        p.insert(5, 50);
        assert_eq!(p.locate_range(1, 5), vec![10, 50]);
    }

    // #[test]
    // fn test_unique_iter() {
    //     let p = Index::new_unique();
    //     p.insert(3, 30);
    //     p.insert(1, 10);
    //     p.insert(2, 20);
    //     let items: Vec<(i64, i64)> = p.iter().map(|(k, v)| (k, v)).collect();
    //     assert_eq!(items, Some((1, 10), (2, 20), (3, 30))); // BTree sorted
    // }

    // ===== NonUnique TESTS =====
    #[test]
    fn test_nonunique_insert_and_locate() {
        let s = Index::new_non_unique();
        s.insert(42, 100);
        assert_eq!(s.locate_all(42), vec![100]);
    }

    #[test]
    fn test_nonunique_insert_multiple_rids() {
        let s = Index::new_non_unique();
        s.insert(42, 100);
        s.insert(42, 101);
        s.insert(42, 102);
        assert_eq!(s.locate_all(42), vec![100, 101, 102]);
    }

    #[test]
    fn test_nonunique_locate_missing() {
        let s = Index::new_non_unique();
        assert!(s.locate_all(99).is_empty());
    }

    #[test]
    fn test_nonunique_remove_one_rid() {
        let s = Index::new_non_unique();
        s.insert(42, 100);
        s.insert(42, 101);
        s.remove(42, 100);
        assert_eq!(s.locate_all(42), vec![101]);
    }

    #[test]
    fn test_nonunique_remove_last_rid_cleans_key() {
        let s = Index::new_non_unique();
        s.insert(42, 100);
        s.remove(42, 100);
        assert!(s.locate_all(42).is_empty());
    }

    #[test]
    fn test_nonunique_locate_range() {
        let s = Index::new_non_unique();
        s.insert(1, 10);
        s.insert(1, 11);
        s.insert(3, 30);
        s.insert(5, 50);
        let mut result = s.locate_range(1, 3);
        result.sort();
        assert_eq!(result, vec![10, 11, 30]);
    }

    #[test]
    fn test_nonunique_locate_range_none() {
        let s = Index::new_non_unique();
        s.insert(1, 10);
        assert!(s.locate_range(5, 10).is_empty());
    }

    // #[test]
    // fn test_nonunique_iter() {
    //     let s = Index::new_non_unique();
    //     s.insert(1, 10);
    //     s.insert(1, 11);
    //     let items: Vec<(i64, i64)> = s.iter().map(|(k, v)| (k, v)).collect();
    //     assert_eq!(items, Some((1, 10), (1,11)));
    // }
}