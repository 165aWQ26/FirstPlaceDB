use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::bufferpool::BufferPool;
use crate::table::Table;

#[derive(Clone)]
pub struct Database {
    tables: FxHashMap<String, Table>,
    //Will want to add functionality to work with other tables
    bufferpool: Arc<RwLock<BufferPool>>,
    path: String,
}


#[allow(dead_code)]
impl Database {
    pub fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
            bufferpool: Arc::new(RwLock::new(BufferPool::default())),
            path: String::new(),
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //Todo Throw an error if path is ""

        //once again this assumes single table functionality
        self.bufferpool.write().set_total_cols(num_columns);
        self.bufferpool.write().set_path(self.path.clone());
        let table = Table::new(
            name.clone(),
            num_columns,
            key_index,
            Arc::clone(&self.bufferpool),
        );
        self.tables.insert(name, table);
    }

    pub fn open(&mut self, path: &str) {
        //create_indexes

        self.path.push_str(path);
        self.path.push('/');
    }


    pub fn close(&self) {
        //Need to implement indexes stuff
        //  Drop index function
        self.bufferpool.write().evict_all().unwrap();
        for table in self.tables.values() {
            (*table).write_table_to_disk(self.path);

        }
    }

    pub fn write_table_to_disk(&self, path: &str) {

    }

    pub fn write_page_directory(&self,directory_path: String){
        //(*table).save_page_directory(self.path);
    }

    pub fn read_table_from_disk(){
        //Assumes only one table
        let mut path : String = directory_path.clone();
        path.push_str("table_data");

    }

    pub fn get_table(&mut self, name: &str) -> Option<&Table> {
        // if(!self.tables.contains_key(name)) {
        //     self.tables.insert(String::from(name),self.read_table_from_disk(name,Arc::clone(&self.bufferpool)))?;
        // }
        self.tables.get(name)
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}