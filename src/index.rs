// Wrapper over the std btreemap. Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mapping primary/secondary keys -> vector of RIDs
// M1: primary key only (BTreeMap<i64, i64>)
// M2: restore secondary indices — switch back to BTreeMap<i64, Vec<i64>> or similar
use std::collections::BTreeMap;
#[derive(Clone)]
pub enum Index {
    Primary(Primary),
    Secondary(Secondary),
}
#[derive(Clone)]
pub struct Primary {
    index: BTreeMap<i64, i64>,
}

#[derive(Clone)]
pub struct Secondary {
    index: BTreeMap<i64, Vec<i64>>,
}

impl Default for Primary {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for Secondary {
    fn default() -> Self {
        Self::new()
    }
}

impl Primary {
    pub fn new() -> Self {
        Primary {
            index: BTreeMap::new(),
        }
    }

    /// Single-traversal insert that checks uniqueness. Returns true if inserted, false if key exists.
    #[inline]
    pub fn insert_unique(&mut self, key: i64, rid: i64) -> bool {
        use std::collections::btree_map::Entry;
        match self.index.entry(key) {
            Entry::Vacant(e) => {
                e.insert(rid);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    #[inline]
    pub fn locate(&self, value: i64) -> Option<i64> {
        self.index.get(&value).copied()
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Option<Vec<i64>> {
        let result: Vec<i64> = self.index.range(begin..=end).map(|(_, &rid)| rid).collect();
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    #[inline]
    pub fn insert(&mut self, key: i64, rid: i64) {
        self.index.insert(key, rid);
    }

    #[inline]
    pub fn remove(&mut self, key: i64, _rid: i64) {
        self.index.remove(&key);
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, i64, i64> {
        self.index.iter()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }
}

impl Secondary {
    pub fn new() -> Self {
        Secondary {
            index: BTreeMap::new(),
        }
    }
    #[inline]
    pub fn locate(&self, value: i64) -> Option<Vec<i64>> {
        self.index.get(&value).cloned()
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Option<Vec<i64>> {
        let result: Vec<i64> = self.index.range(begin..=end).flat_map(|(_, rids)| rids.iter().copied()).collect();

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    #[inline]
    pub fn insert(&mut self, key: i64, rid: i64) {
        // if key is not in index, make new vector
        // else -- add to vector that exists
        if self.index.contains_key(&key) {
            self.index.get_mut(&key).unwrap().push(rid);
        } else {
            self.index.insert(key, vec![rid]);
        }
    }

    #[inline]
    pub fn remove(&mut self, key: i64, rid: i64) {
        self.index.get_mut(&key).unwrap().retain(|&x| x != rid);

        if self.index.get(&key).unwrap().len() == 0 {
            self.index.remove(&key);
        }
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, i64, Vec<i64>> {
        self.index.iter()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    // drop_index and create_index is left to the Table
}
#[cfg(test)]
mod tests {
    use super::*;

    // ===== PRIMARY TESTS =====

    #[test]
    fn test_primary_new_is_empty() {
        let p = Primary::new();
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_primary_insert_and_locate() {
        let mut p = Primary::new();
        p.insert(10, 100);
        assert_eq!(p.locate(10), Some(100));
    }

    #[test]
    fn test_primary_locate_missing() {
        let p = Primary::new();
        assert_eq!(p.locate(99), None);
    }

    #[test]
    fn test_primary_insert_unique_success() {
        let mut p = Primary::new();
        assert!(p.insert_unique(1, 100));
        assert_eq!(p.locate(1), Some(100));
    }

    #[test]
    fn test_primary_insert_unique_duplicate() {
        let mut p = Primary::new();
        p.insert_unique(1, 100);
        assert!(!p.insert_unique(1, 200)); // Duplicate rejected
        assert_eq!(p.locate(1), Some(100)); // Original unchanged
    }

    #[test]
    fn test_primary_insert_overwrites() {
        let mut p = Primary::new();
        p.insert(1, 100);
        p.insert(1, 999);
        assert_eq!(p.locate(1), Some(999));
    }

    #[test]
    fn test_primary_remove() {
        let mut p = Primary::new();
        p.insert(1, 100);
        p.remove(1, 100);
        assert_eq!(p.locate(1), None);
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_primary_remove_nonexistent() {
        let mut p = Primary::new();
        p.insert(1, 100);
        p.remove(99, 100); // Should not panic
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn test_primary_locate_range_all() {
        let mut p = Primary::new();
        p.insert(1, 10);
        p.insert(2, 20);
        p.insert(3, 30);
        assert_eq!(p.locate_range(1, 3), Some(vec![10, 20, 30]));
    }

    #[test]
    fn test_primary_locate_range_partial() {
        let mut p = Primary::new();
        p.insert(1, 10);
        p.insert(3, 30);
        p.insert(5, 50);
        assert_eq!(p.locate_range(2, 4), Some(vec![30]));
    }

    #[test]
    fn test_primary_locate_range_none() {
        let mut p = Primary::new();
        p.insert(1, 10);
        assert_eq!(p.locate_range(5, 10), None);
    }

    #[test]
    fn test_primary_locate_range_inclusive_boundaries() {
        let mut p = Primary::new();
        p.insert(1, 10);
        p.insert(5, 50);
        assert_eq!(p.locate_range(1, 5), Some(vec![10, 50]));
    }

    #[test]
    fn test_primary_iter_order() {
        let mut p = Primary::new();
        p.insert(3, 30);
        p.insert(1, 10);
        p.insert(2, 20);
        let items: Vec<(i64, i64)> = p.iter().map(|(&k, &v)| (k, v)).collect();
        assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]); // BTree sorted
    }

    #[test]
    fn test_primary_len() {
        let mut p = Primary::new();
        p.insert(1, 10);
        p.insert(2, 20);
        assert_eq!(p.len(), 2);
    }

    // ===== SECONDARY TESTS =====

    #[test]
    fn test_secondary_new_is_empty() {
        let s = Secondary::new();
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn test_secondary_insert_and_locate() {
        let mut s = Secondary::new();
        s.insert(42, 100);
        assert_eq!(s.locate(42), Some(vec![100]));
    }

    #[test]
    fn test_secondary_insert_multiple_rids() {
        let mut s = Secondary::new();
        s.insert(42, 100);
        s.insert(42, 101);
        s.insert(42, 102);
        assert_eq!(s.locate(42), Some(vec![100, 101, 102]));
    }

    #[test]
    fn test_secondary_locate_missing() {
        let s = Secondary::new();
        assert_eq!(s.locate(99), None);
    }

    #[test]
    fn test_secondary_remove_one_rid() {
        let mut s = Secondary::new();
        s.insert(42, 100);
        s.insert(42, 101);
        s.remove(42, 100);
        assert_eq!(s.locate(42), Some(vec![101]));
    }

    #[test]
    fn test_secondary_remove_last_rid_cleans_key() {
        let mut s = Secondary::new();
        s.insert(42, 100);
        s.remove(42, 100);
        assert_eq!(s.locate(42), None);
        assert_eq!(s.len(), 0); // Key deleted entirely
    }

    #[test]
    fn test_secondary_locate_range() {
        let mut s = Secondary::new();
        s.insert(1, 10);
        s.insert(1, 11);
        s.insert(3, 30);
        s.insert(5, 50);
        let mut result = s.locate_range(1, 3).unwrap();
        result.sort();
        assert_eq!(result, vec![10, 11, 30]);
    }

    #[test]
    fn test_secondary_locate_range_none() {
        let mut s = Secondary::new();
        s.insert(1, 10);
        assert_eq!(s.locate_range(5, 10), None);
    }

    #[test]
    fn test_secondary_len_counts_keys_not_rids() {
        let mut s = Secondary::new();
        s.insert(1, 10);
        s.insert(1, 11); // Same key, different RID
        s.insert(2, 20);
        assert_eq!(s.len(), 2); // 2 unique keys, not 3 RIDs
    }

    #[test]
    fn test_secondary_iter() {
        let mut s = Secondary::new();
        s.insert(1, 10);
        s.insert(1, 11);
        let items: Vec<(i64, usize)> = s.iter().map(|(&k, v)| (k, v.len())).collect();
        assert_eq!(items, vec![(1, 2)]);
    }

    // ===== INDEX ENUM TESTS =====

    #[test]
    fn test_index_enum_creates_primary() {
        let idx = Index::Primary(Primary::new());
        assert!(matches!(idx, Index::Primary(_)));
    }

    #[test]
    fn test_index_enum_creates_secondary() {
        let idx = Index::Secondary(Secondary::new());
        assert!(matches!(idx, Index::Secondary(_)));
    }

    #[test]
    fn test_create_mixed_indices() {
        let num_cols = 5;
        let primary_col = 2;
        let indices: Vec<Index> = (0..num_cols).map(|col| {
            if col == primary_col {
                Index::Primary(Primary::new())
            } else {
                Index::Secondary(Secondary::new())
            }
        }).collect();

        for (i, idx) in indices.iter().enumerate() {
            if i == primary_col {
                assert!(matches!(idx, Index::Primary(_)));
            } else {
                assert!(matches!(idx, Index::Secondary(_)));
            }
        }
    }

    #[test]
    fn test_primary_default() {
        let p = Primary::default();
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_secondary_default() {
        let s = Secondary::default();
        assert_eq!(s.len(), 0);
    }
}
