use bplustree::BPlusTreeMap;
const MAX_RECORDS_TOTAL: usize = 64000;
// This is just a wrapper over a B+ tree. Table will have many of these.
// Table will have [Index, Index, Index, ..., ]
// B+ Tree wrapper for mappping primary/secondary keys -> vector of RIDs
pub struct Index {
    // primary key ->> STILL a vector of 1 RID
    index: BPlusTreeMap<i64, Vec<u64>>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            index: BPlusTreeMap::new(MAX_RECORDS_TOTAL).unwrap(),
        }
    }

    pub fn locate(&self, value: i64) -> Option<&Vec<u64>> {
        return self.index.get(&value);
    }

    pub fn locate_range(&self, begin: i64, end: i64) -> Option<Vec<u64>> {
        let mut result: Vec<u64> = Vec::new();

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
    pub fn insert(&mut self, key: i64, rid: u64) -> () {
        if self.index.contains_key(&key) {
            // push RID onto the vector
            self.index.get_mut(&key).unwrap().push(rid);
        } else {
            // no RID yet
            self.index.insert(key, vec![rid]);
        }
    }

    pub fn remove(&mut self, key: i64, rid: u64) -> () {
        // find vector for key, remove that RID from the vector
        // We will call remove always when we have 
        
        // scan RIDs for which one to remove
        self.index.get_mut(&key).unwrap().retain(|&x| x != rid);
    }

    // --drop_index and create_index-- is left to the Table
}

// -- tests --
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_one() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        assert_eq!(my_index.locate(5), Some(&vec![1]));
    }

    #[test]
    fn insert_two() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(4, 2);
        assert_eq!(my_index.locate(5), Some(&vec![1]));
        assert_eq!(my_index.locate(4), Some(&vec![2]));
    }

    #[test]
    fn insert_dups() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);
        assert_eq!(my_index.locate(5), Some(&vec![1, 2]));
    }

    #[test]
    fn range_query() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);
        my_index.insert(6, 3);
        my_index.insert(7, 4);
        my_index.insert(8, 5);
        my_index.insert(8, 6);
        assert_eq!(my_index.locate_range(5, 8), Some(vec![1, 2, 3, 4, 5, 6]));
    }

    #[test]
    fn test_remove() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5,2);
        my_index.insert(6, 3);
        my_index.insert(7,4);
        my_index.insert(8, 5);
        my_index.insert(8,6);
        my_index.remove(5, 1);
        assert_eq!(my_index.locate_range(5, 8), Some(vec![2, 3, 4, 5, 6]));
    }
}
