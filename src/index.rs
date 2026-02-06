// B Trees are nice for range queries. B+ Trees are even better. 

// milestone says: "All columns are 64-bit integers in this implementation."
// "Given a certain value for a column, the index should efficiently locate all records having that value.""
use std::vec;
use bplustree::BPlusTreeMap;    
// B+ Tree for indexing keys -> RIDs
// Index will have lots of B+ Trees
// 1 per col.
// We'll search the associated one based on what column is given in a query.
// The tree stores key-value pairs -> search for key, find key, get RID. O(lg n)
// Should (in theory) be really fast for range queries. 

// We need the Table for when we're making a new index
// How else will we know the amount of cols for the B+ Trees?
pub struct Index { 
    // indexes are a list of B+ Tree Maps
    indexes: Vec<Option<BPlusTreeMap<i64, u64>>>,
    table: Table
}

// Not sure if this will work, but I'm going through what I assume Table will look like.
// Running with my own assumption of Table
// Table::numColumns -> returns number of columns in a table
// Table::primaryCol -> returns the column of primary key -- secondary key is later.
impl Index {
    // method for intializing an index structure from a table
    pub fn new(table: Table) -> Index {
        // dont have to do self.index like python
        // use vec! for vector with known amount of cols
        // table knows amt of cols
        let mut indexes: Vec<Option<_>> = vec![None; table.numColumns];
        // map for primary cols 
        indexes[table.primaryCol] = Some(BPlusTreeMap::new(MAX_RECORDS));

        return Index { indexes, table }
    }

    // find RID in a specific column that has same value
    // lowkey i just kept changing the types until rust analyzer stopped yelling at me. 
    pub fn locate(&self, column: usize, value: i64) -> Option<&u64> {
        if let Some(bptree) = &self.indexes[column] {
            return bptree.get(&value)
        }
        None
    }

    // need to use a vector for the values if we want to support secondary indexes later.
    // this will prob change hella so trying to go simple to not blow my head off
    pub fn locate_range(&self, begin: i64, end: i64, column: usize) -> Option<Vec<u64>> {
        let tree = self.indexes.get(column)?.as_ref()?; 

        let mut result: Vec<u64> = Vec::new();

        for (_key, rid) in tree.range(begin..=end) {
            result.push(*rid); // rid: &u64
        }

        Some(result)
    }


    // Optional -- 
    // if col doesnt have a tree yet, make one
    pub fn create_index(&mut self, column_num: usize) {
        // 
        if let Some(slot) = self.indexes.get_mut(column_num) {
            // wow there's nothing in the slot -- make a tree here
            if slot.is_none() {
                *slot = Some(BPlusTreeMap::new(MAX_RECORDS).unwrap());
            }
        }
    }

    // Optional --
    // set index to None to drop it.
    pub fn drop_index(&mut self, column_num: usize) {
        self.indexes[column_num] = None
    }
}

// What does it need to do?
// Initialize index
// locate(value, column) Return location (?) on all records with the value on given column
// locate(value, begin, end, column) -- return locations of all records in col where val is between beg and end
// create_index(column) create an index on a column (?)
// drop_index(column) drop the index of a column (?)

// query(key) -> needs to get record from key (this operation should be fast.)