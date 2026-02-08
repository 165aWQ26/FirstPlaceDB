// This is just a wrapper over a B+ tree. Table will have many of these.
// Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mapping primary/secondary keys -> vector of RIDs
use bplustree::BPlusTreeMap;

const MAX_RECORDS_TOTAL: usize = 64000;

pub struct Index {
    // primary key ->> STILL a vector of 1 RID
    index: BPlusTreeMap<i64, Vec<i64>>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            index: BPlusTreeMap::new(MAX_RECORDS_TOTAL).unwrap(),
        }
    }

    pub fn locate(&self, value: i64) -> Option<&Vec<i64>> {
        return self.index.get(&value);
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Option<Vec<i64>> {
        let mut result: Vec<i64> = Vec::new();

        for (_key, rid) in self.index.range(begin..=end) {
            result.extend(rid);
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    // for query and table
    pub fn insert(&mut self, key: i64, rid: i64) -> () {
        if self.index.contains_key(&key) {
            // push RID onto the vector
            self.index.get_mut(&key).unwrap().push(rid);
        } else {
            // no RID yet
            self.index.insert(key, vec![rid]);
        }
    }

    pub fn remove(&mut self, key: i64, rid: i64) -> () {
        // find vector for key, remove that RID from the vector
        // if vector is empty, remove will REMOVE THAT MAPPING.
        // locate will then always generate some result, never None.
        if !self.index.contains_key(&key) {
            return;
        }

        self.index.get_mut(&key).unwrap().retain(|&x| x != rid);
        if self.index.get(&key).unwrap().is_empty() {
            let _ = self.index.remove_item(&key);
        }
    }

    // --drop_index and create_index-- is left to the Table
}
