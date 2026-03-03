use std::sync::mpsc;
use rustc_hash::FxHashMap;

use crate::table::Table;
use std::thread;
use std::sync::mpsc::{channel, Sender};
use crate::page::Page;
use crate::page_collection::Pid;

struct Database {
    tables: FxHashMap<usize, Table>,
    table_names: FxHashMap<String, usize>, //use this for the guard check
    table_id: std::ops::RangeFrom<usize>,
    buffer_pool_req_sender: Sender<Pid>,
    buffer_pool: //Todo: Change me
}

#[allow(dead_code)]
impl Database {
    pub fn new() -> Self {
        let (tx, rx) = channel::<Pid>();
        Self {
            tables: FxHashMap::default(),
            table_names: FxHashMap::default(),
            table_id: 0..,
            buffer_pool_req_sender: tx,
            buffer_pool: rx //Must recieve rx in its constructor
        }
    }
    
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        let table_id = self.table_id.next().unwrap();
        let table = Table::new(
            name.clone(),
            num_columns,
            key_index,
            table_id,
            self.buffer_pool_req_sender.clone(),
        );
        self.tables.insert(table_id, table);
        self.table_names.insert(name, table_id);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(self.table_names.get(name).unwrap()) //Todo danny fix this error handling
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(self.table_names.get(name).unwrap()); //Todo danny fix this error handling
    }
}