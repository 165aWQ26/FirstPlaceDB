use core::num;
use std::rc::Rc;
use std::cell::RefCell;

use rustc_hash::FxHashMap;

use crate::bufferpool::{self, BufferPool};
use crate::table::Table;


struct Database{
    tables: FxHashMap<String, Table>,
    //Will want to add functionality to work with other tables
    bufferpool:  Rc<RefCell<BufferPool>>,
}


#[allow(dead_code)]
impl Database{
    pub fn new() -> Self{
        Self {
            tables: FxHashMap::default(),
            bufferpool: Rc::new(RefCell::new(BufferPool::default())),
        }
    }

    pub const NUMBER_OF_FRAMES: usize = 20;
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //once again this assumes single table functionality
        self.bufferpool.borrow_mut().total_cols = num_columns;
        self.bufferpool.borrow_mut().path = name;
        let table = Table::new(
            name.clone(),
            num_columns + Table::NUM_META_PAGES,
            key_index,
            Rc::clone(&self.bufferpool),
        );
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}