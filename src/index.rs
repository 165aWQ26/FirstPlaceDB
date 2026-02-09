// This is just a wrapper over a B+ tree. Table will have many of these.
// Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mapping primary/secondary keys -> vector of RIDs
use bplustree::BPlusTreeMap;

#[derive(Default)]
pub struct Index {
    // primary key ->> STILL a vector of 1 RID
    index: BPlusTreeMap<i64, Vec<i64>>,
}

impl Index {

    //Changed to this value: essentially just when a mode
    const MAX_RECORDS_PER_NODE: usize = 128;
    pub fn new() -> Self {
        Index {
            index: BPlusTreeMap::new(Index::MAX_RECORDS_PER_NODE).unwrap(),
        }
    }

    pub fn locate(&self, value: i64) -> Option<&Vec<i64>> {
        self.index.get(&value)
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Option<Vec<i64>> {
        let mut result: Vec<i64> = Vec::new();

        for (_key, rid) in self.index.range(begin..=end) {
            result.extend(rid);
        }

        Some(result)
    }

    // for query and table
    pub fn insert(&mut self, key: i64, rid: i64) {
        //Single lookup
        if let Some(rids) = self.index.get_mut(&key) {
            rids.push(rid);
        } else {
            self.index.insert(key, vec![rid]);
        }
    }

    pub fn remove(&mut self, key: i64, rid: i64) {
        // find vector for key, remove that RID from the vector
        // if vector is empty, remove will REMOVE THAT MAPPING.
        // locate will then always generate some result, never None.

        //Lookup key
        if let Some(rids) = self.index.get_mut(&key) {
            if let Some(pos) = rids.iter().position(|&x| x == rid) {
                rids.swap_remove(pos);
            }

            //We only need to remove entire keys directly after removing associated rids
            if rids.is_empty() {
                self.index.remove(&key);
            }
        }
    }

    // --drop_index and create_index-- is left to the Table
}