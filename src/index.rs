// Wrapper over the std btreemap. Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mapping primary/secondary keys -> vector of RIDs
// M1: primary key only (BTreeMap<i64, i64>)
// M2: restore secondary indices — switch back to BTreeMap<i64, Vec<i64>> or similar
use crate::index::Index::{NonUnique, Unique};
use std::collections::{BTreeMap, BTreeSet};

pub trait TableIndex {
    fn insert(&mut self, key: i64, val: i64);
    fn remove(&mut self, key: i64, val: i64);
    fn locate(&self, key: i64) -> Vec<i64>;
    fn locate_range(&self, begin: i64, end: i64) -> Vec<i64>;
    fn len(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = (i64, i64)> + '_>;
}
#[derive(Clone, Default)]
pub struct UniqueIndex {
    index: BTreeMap<i64, i64>,
}

#[derive(Clone, Default)]
pub struct NonUniqueIndex {
    index: BTreeMap<i64, BTreeSet<i64>>,
}

impl UniqueIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn insert_unique(&mut self, key: i64, rid: i64) -> bool {
        use std::collections::btree_map::Entry;
        match self.index.entry(key) {
            Entry::Vacant(e) => {
                e.insert(rid);
                true
            }
            Entry::Occupied(_) => false,
        }
    }
    #[allow(dead_code)]
    fn try_insert(&mut self, key: i64, rid: i64) -> bool {
        self.insert_unique(key, rid)
    }
}
impl TableIndex for UniqueIndex {
    fn insert(&mut self, key: i64, val: i64) {
        self.index.insert(key, val);
    }

    fn remove(&mut self, key: i64, _val: i64) {
        self.index.remove(&key);
    }

    fn locate(&self, key: i64) -> Vec<i64> {
        self.index.get(&key).copied().into_iter().collect()
    }

    fn locate_range(&self, begin: i64, end: i64) -> Vec<i64> {
        self.index.range(begin..=end).map(|(_, &rid)| rid).collect()
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (i64, i64)> + '_> {
        Box::new(self.index.iter().map(|(k, v)| (*k, *v)))
    }
}

impl NonUniqueIndex {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn iter_raw(&self) -> std::collections::btree_map::Iter<'_, i64, BTreeSet<i64>> {
        self.index.iter()
    }
}
impl TableIndex for NonUniqueIndex {
    fn insert(&mut self, key: i64, val: i64) {
        self.index.entry(key).or_default().insert(val);
    }
    fn remove(&mut self, key: i64, val: i64) {
        if let Some(rid) = self.index.get_mut(&key) {
            rid.remove(&val);
            if rid.is_empty() {
                self.index.remove(&key);
            }
        }
    }

    fn locate(&self, key: i64) -> Vec<i64> {
        self.index
            .get(&key)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    fn locate_range(&self, begin: i64, end: i64) -> Vec<i64> {
        self.index
            .range(begin..=end)
            .flat_map(|(_, rids)| rids.iter().copied())
            .collect()
    }
    fn len(&self) -> usize {
        self.index.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (i64, i64)> + '_> {
        Box::new(
            self.index
                .iter()
                .flat_map(|(&k, rids)| rids.iter().map(move |&rid| (k, rid))),
        )
    }
}

#[derive(Clone)]
pub enum Index {
    Unique(UniqueIndex),
    NonUnique(NonUniqueIndex),
}

impl TableIndex for Index {
    fn insert(&mut self, key: i64, val: i64) {
        match self {
            Unique(idx) => idx.insert(key, val),
            NonUnique(idx) => idx.insert(key, val),
        }
    }

    fn remove(&mut self, key: i64, val: i64) {
        match self {
            Unique(idx) => idx.remove(key, val),
            NonUnique(idx) => idx.remove(key, val),
        }
    }

    fn locate(&self, key: i64) -> Vec<i64> {
        match self {
            Unique(idx) => idx.locate(key),
            NonUnique(idx) => idx.locate(key),
        }
    }

    fn locate_range(&self, begin: i64, end: i64) -> Vec<i64> {
        match self {
            Unique(idx) => idx.locate_range(begin, end),
            NonUnique(idx) => idx.locate_range(begin, end),
        }
    }

    fn len(&self) -> usize {
        match self {
            Unique(idx) => idx.len(),
            NonUnique(idx) => idx.len(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (i64, i64)> + '_> {
        match self {
            Unique(idx) => idx.iter(),
            NonUnique(idx) => idx.iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== unique TESTS =====

    #[test]
    fn test_unique_new_is_empty() {
        let p = UniqueIndex::new();
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_unique_insert_and_locate() {
        let mut p = UniqueIndex::new();
        p.insert(10, 100);
        assert_eq!(p.locate(10), vec![100]);
    }

    #[test]
    fn test_unique_locate_missing() {
        let p = UniqueIndex::new();
        assert!(p.locate(99).is_empty());
    }

    #[test]
    fn test_unique_insert_unique_success() {
        let mut p = UniqueIndex::new();
        assert!(p.insert_unique(1, 100));
        assert_eq!(p.locate(1), vec![100]);
    }

    #[test]
    fn test_unique_insert_unique_duplicate() {
        let mut p = UniqueIndex::new();
        p.insert_unique(1, 100);
        assert!(!p.insert_unique(1, 200)); // Duplicate rejected
        assert_eq!(p.locate(1), vec![100]); // Original unchanged
    }

    #[test]
    fn test_unique_insert_overwrites() {
        let mut p = UniqueIndex::new();
        p.insert(1, 100);
        p.insert(1, 999);
        assert_eq!(p.locate(1), vec![999]);
    }

    #[test]
    fn test_unique_remove() {
        let mut p = UniqueIndex::new();
        p.insert(1, 100);
        p.remove(1, 100);
        assert!(p.locate(1).is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_unique_remove_nonexistent() {
        let mut p = UniqueIndex::new();
        p.insert(1, 100);
        p.remove(99, 100); // Should not panic
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn test_unique_locate_range_all() {
        let mut p = UniqueIndex::new();
        p.insert(1, 10);
        p.insert(2, 20);
        p.insert(3, 30);
        assert_eq!(p.locate_range(1, 3), vec![10, 20, 30]);
    }

    #[test]
    fn test_unique_locate_range_partial() {
        let mut p = UniqueIndex::new();
        p.insert(1, 10);
        p.insert(3, 30);
        p.insert(5, 50);
        assert_eq!(p.locate_range(2, 4), vec![30]);
    }

    #[test]
    fn test_unique_locate_range_none() {
        let mut p = UniqueIndex::new();
        p.insert(1, 10);
        assert!(p.locate_range(5, 10).is_empty());
    }

    #[test]
    fn test_unique_locate_range_inclusive_boundaries() {
        let mut p = UniqueIndex::new();
        p.insert(1, 10);
        p.insert(5, 50);
        assert_eq!(p.locate_range(1, 5), vec![10, 50]);
    }

    #[test]
    fn test_unique_len() {
        let mut p = UniqueIndex::new();
        p.insert(1, 10);
        p.insert(2, 20);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn test_unique_iter() {
        let mut p = UniqueIndex::new();
        p.insert(3, 30);
        p.insert(1, 10);
        p.insert(2, 20);
        let items: Vec<(i64, i64)> = p.iter().map(|(k, v)| (k, v)).collect();
        assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]); // BTree sorted
    }

    // ===== NonUnique TESTS =====

    #[test]
    fn test_nonunique_new_is_empty() {
        let s = NonUniqueIndex::new();
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn test_nonunique_insert_and_locate() {
        let mut s = NonUniqueIndex::new();
        s.insert(42, 100);
        assert_eq!(s.locate(42), vec![100]);
    }

    #[test]
    fn test_nonunique_insert_multiple_rids() {
        let mut s = NonUniqueIndex::new();
        s.insert(42, 100);
        s.insert(42, 101);
        s.insert(42, 102);
        assert_eq!(s.locate(42), vec![100, 101, 102]);
    }

    #[test]
    fn test_nonunique_locate_missing() {
        let s = NonUniqueIndex::new();
        assert!(s.locate(99).is_empty());
    }

    #[test]
    fn test_nonunique_remove_one_rid() {
        let mut s = NonUniqueIndex::new();
        s.insert(42, 100);
        s.insert(42, 101);
        s.remove(42, 100);
        assert_eq!(s.locate(42), vec![101]);
    }

    #[test]
    fn test_nonunique_remove_last_rid_cleans_key() {
        let mut s = NonUniqueIndex::new();
        s.insert(42, 100);
        s.remove(42, 100);
        assert!(s.locate(42).is_empty());
        assert_eq!(s.len(), 0); // Key deleted entirely
    }

    #[test]
    fn test_nonunique_locate_range() {
        let mut s = NonUniqueIndex::new();
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
        let mut s = NonUniqueIndex::new();
        s.insert(1, 10);
        assert!(s.locate_range(5, 10).is_empty());
    }

    #[test]
    fn test_nonunique_len_counts_keys_not_rids() {
        let mut s = NonUniqueIndex::new();
        s.insert(1, 10);
        s.insert(1, 11); // Same key, different RID
        s.insert(2, 20);
        assert_eq!(s.len(), 2); // 2 unique keys, not 3 RIDs
    }

    #[test]
    fn test_nonunique_iter() {
        let mut s = NonUniqueIndex::new();
        s.insert(1, 10);
        s.insert(1, 11);
        let items: Vec<(i64, i64)> = s.iter().map(|(k, v)| (k, v)).collect();
        assert_eq!(items, vec![(1, 10), (1,11)]);
    }

    // ===== INDEX ENUM TESTS =====

    #[test]
    fn test_index_enum_creates_unique() {
        // let idx = Index::unique(UniqueIndex::new());
        // assert!(matches!(idx, Index::unique(_)));
        let idx = Unique(UniqueIndex::new());
        assert!(matches!(idx, Unique(_)));
    }

    #[test]
    fn test_index_enum_creates_nonunique() {
        let idx = NonUnique(NonUniqueIndex::new());
        assert!(matches!(idx, NonUnique(_)));
    }

    #[test]
    fn test_create_mixed_indices() {
        let num_cols = 5;
        let unique_col = 2;
        let indices: Vec<Index> = (0..num_cols)
            .map(|col| {
                if col == unique_col {
                    Unique(UniqueIndex::new())
                } else {
                    NonUnique(NonUniqueIndex::new())
                }
            })
            .collect();

        for (i, idx) in indices.iter().enumerate() {
            if i == unique_col {
                assert!(matches!(idx, Unique(_)));
            } else {
                assert!(matches!(idx, NonUnique(_)));
            }
        }
    }

    #[test]
    fn test_unique_default() {
        let p = UniqueIndex::default();
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn test_secondary_default() {
        let s = NonUniqueIndex::default();
        assert_eq!(s.len(), 0);
    }
}
