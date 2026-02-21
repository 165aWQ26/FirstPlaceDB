// Wrapper over the std btreemap. Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mapping primary/secondary keys -> vector of RIDs
// M1: primary key only (BTreeMap<i64, i64>)
// M2: restore secondary indices — switch back to BTreeMap<i64, Vec<i64>> or similar
use std::collections::BTreeMap;

pub struct Index {
    index: BTreeMap<i64, Vec<i64>>,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Index {
            index: BTreeMap::new(),
        }
    }

    #[inline]
    pub fn locate(&self, value: i64) -> Option<&Vec<i64>> {
        self.index.get(&value)
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
    
    // when inserting, we can't have duplicate primary keys.
    #[inline]
    pub fn insert_unique(&mut self, key: i64, rid: i64) -> bool {
        if !self.index.contains_key(&key) {
            self.index.insert(key, vec![rid]);
            true
        } else {
            false
        }
    }

    // /// Single-traversal insert that checks uniqueness. Returns true if inserted, false if key exists.
    // #[inline]
    // pub fn insert_unique(&mut self, key: i64, rid: i64) -> bool {
    //     use std::collections::btree_map::Entry;
    //     match self.index.entry(key) {
    //         Entry::Vacant(e) => {
    //             e.insert(rid);
    //             true
    //         }
    //         Entry::Occupied(_) => false,
    //     }
    // }

    #[inline]
    pub fn remove(&mut self, key: i64, rid: i64) {
        self.index.get_mut(&key).map(|vec| vec.remove(rid as usize));

        if self.index.get(&key).unwrap().len() > 0 {
            self.index.remove(&key);
        }
    }

    // drop_index and create_index is left to the Table
}