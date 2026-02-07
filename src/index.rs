// B Trees are nice for range queries. B+ Trees are even better. 
use bplustree::BPlusTreeMap;    
// B+ Tree for indexing keys -> RIDs

// Table will have [Index, Index, Index, ..., ]
// This is just a wrapper over a B+ tree. Table will have many.
// Index should work with primary and secondary keys.
pub struct Index { 
    // indexes are a list of B+ Tree Maps
    // for the primary key, it's just one.
    index: BPlusTreeMap<i64, u64>
}

impl Index {
    // find RID in a specific column that has same value
    // implement for just primary keys for now.
    // everything about this works, it's just our implemention of column will vary from what they cooked up.
    pub fn locate(&self, value: i64) -> Option<&u64> {
        // usize is always the same lowkey. so just fucking ignore it lmfao
        return self.index.get(&value);
    }

    // need to use a vector for the values if we want to support secondary indexes later.
    // this will prob change hella so trying to go simple to not blow my head off
    pub fn locate_range(&self, begin: i64, end: i64) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::new();

        for (_key, rid) in self.index.range(begin..=end) {
            result.push(*rid); // rid: &u64
        }

        return result;
    }

    // --drop_index and create_index-- is left to the Table
}

// What does it need to do?
// Initialize index
// locate(value, column) Return RID of all records with the value on given column
// locate(value, begin, end, column) -- return locations of all records in col where val is between beg and end
// create_index(column) create an index on a column (?)
// drop_index(column) drop the index of a column (?)

// query(key) -> needs to get record from key