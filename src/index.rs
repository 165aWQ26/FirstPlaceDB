use bplustree::BPlusTreeMap;    
const MAX_RECORDS: usize = 512;
// This is just a wrapper over a B+ tree. Table will have many of these.
// Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mappping primary/secondary keys -> vector of RIDs
pub struct Index { 
    // primary key ->> STILL a vector of 1 RID
    index: BPlusTreeMap<i64, Vec<u64>>
}

impl Index {
    pub fn new(self) -> Self {
        Index {
            index: BPlusTreeMap::new(MAX_RECORDS).unwrap(),
        }
    }
    
    pub fn locate(&self, value: i64) -> Option<&Vec<u64>> {
        return self.index.get(&value);
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::new();

        for (_key, rid) in self.index.range(begin..=end) {
            result.extend(rid);
        }

        return result
    }

    // for query and table
    pub fn insert(&mut self, key: i64, rid: u64) -> () {
        if self.index.contains_key(&key) {
            // push RID onto the vector 
            self.index.get_mut(&key).unwrap().push(rid);
        } else {
            // no RID yet
            self.index.insert(key, vec![rid]);
        }
    }

    // --drop_index and create_index-- is left to the Table
}