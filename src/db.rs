use std::sync::Arc;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::bufferpool::{BufferPool};
use crate::table::Table;


struct Database{
    tables: FxHashMap<String, Table>,
    //Will want to add functionality to work with other tables
    bufferpool: Arc<RwLock<BufferPool>>,
    path: String,
}


#[allow(dead_code)]
impl Database{
    pub fn new() -> Self{
        Self {
            tables: FxHashMap::default(),
            bufferpool: Arc::new(RwLock::new(BufferPool::default())),
            path : String::new()
        }
    }

    pub const NUMBER_OF_FRAMES: usize = 20;
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //once again this assumes single table functionality
        self.bufferpool.write().set_total_cols(num_columns);
        self.bufferpool.write().set_path(self.path.clone());
        let table = Table::new(
            name.clone(),
            num_columns + Table::NUM_META_PAGES,
            key_index,
        );
        self.tables.insert(name, table);
    }

    pub fn open(&mut self, path:String){
        //create_indexes  
        self.path.push_str(&path);
    }


    pub fn close(& self){
        //Need to implement indexes stuff
        //  Drop index function
        self.bufferpool.write().evict_all().unwrap();
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}