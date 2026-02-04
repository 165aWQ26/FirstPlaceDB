// B Trees are nice for range queries. B+ Trees are even better. 
// https://docs.rs/bplustree/latest/bplustree/index.html
use bplustree::BPlusTree;

// what are keys and values going to be?
// values are RIDs, u64
// keys are --> values in columns
// values in columns 
// so each RID should map to its own value..ok yeah that makes sense.
// thats like the page right?

// milestone says: "All columns are 64-bit integers in this implementation."
// "Given a certain value for a column, the index should efficiently locate all records having that value.""
pub struct Index {
   tree: BPlusTree<i64, u64> 
}

impl Index {
   // burger
}
// What is the BPlusTree storing?
// RIDs.

// What does it need to do?
// Initialize index
// locate(value, column) Return location (?) on all records with the value on given column
// locate(value, begin, end, column) -- return locations of all records in col where val is between beg and end
// create_index(column) create an index on a column (?)
// drop_index(column) drop the index of a column (?)


// Make a data structure for the index
// Add records to the data structure that can be found using keys.

// query(key) -> record (this operation should be fast.)