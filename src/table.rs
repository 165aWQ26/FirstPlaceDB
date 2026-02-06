use std::collections::HashMap;

pub const INDIRECTION_COLUMN: usize = 0;
pub const RID_COLUMN: usize = 1;
pub const TIMESTAMP_COLUMN: usize = 2;
pub const SCHEMA_ENCODING_COLUMN: usize = 3;

// Record 
#[derive(Debug, Clone)]
pub struct Record {
    pub rid: u64,
    pub key: i32,
    pub columns: Vec<i32>,
}

impl Record {
    pub fn new(rid: u64, key: i32, columns: Vec<i32>) -> Self {
        Self { rid, key, columns }
    }
}

// Table
#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub num_columns: usize,
    pub key: usize,
    pub page_directory: HashMap<u64, (usize, usize)>,
    // need to add index
}

impl Table {
    pub fn new(name: &str, num_columns: usize, key: usize) -> Self {
        Self {
            name: name.to_string(),
            num_columns,
            key,
            page_directory: HashMap::new(),
            // need to add index
        }
    }

    pub fn insert() {

    }

    pub fn read() {

    }

    pub fn update() {
        
    }

    pub fn merge() {
        // add later
    }
}
