use std::sync::Arc;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;

use crate::bufferpool::{BufferPool};
use crate::db_error::DbError;
use crate::table::{Table};

pub struct Database {
    tables: FxHashMap<String, Table>,
    // Will want to add functionality to work with other tables
    bufferpool: Arc<Mutex<BufferPool>>,
    path: String,
    table_id_iterator: std::ops::RangeFrom<usize>,
}


#[allow(dead_code)]
impl Database {
    pub fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
            bufferpool: Arc::new(Mutex::new(BufferPool::default())),
            path: String::new(),
            table_id_iterator: 0..
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //Todo Throw an error if path is ""

        //once again this assumes single table functionality
        // self.bufferpool.set_total_cols(num_columns);
        // self.bufferpool.set_path(self.path.clone());
        self.bufferpool.lock().set_total_cols(num_columns);
        let table = Table::new(
            name.clone(),
            num_columns,
            key_index,
            bufferpool = Arc::new(Mutex::new(BufferPool::default())),
            self.table_id_iterator.next().unwrap()
        );
        self.tables.insert(name, table);
    }

    pub fn open(&mut self, path: &str) {
        //create_indexes

        self.path.push_str(path);
        self.path.push('/');
    }


    pub fn close(&self) -> Result<(), DbError> {
        //Need to implement indexes stuff
        //  Drop index function
        self.bufferpool.lock().evict_all()?;
        for table in self.tables.values() {
            (*table).write_to_disk(self.path.clone()).map_err(|_| DbError::WriteTableFailed())?;
        }

        Ok(())
    }

    pub fn read_table_from_disk(&mut self,name:String) -> Result<(), DbError> {
        //Assumes only one table
        let mut table = Table::new(
            name.clone(),
            0,
            0,
            Arc::clone(&self.bufferpool), 0
        );
        self.path = name.clone();
        table.read_from_disk(self.path.clone()).map_err(|_| DbError::ReadTableFailed())?;
        self.tables.insert(name, table);

        Ok(())

    }

    pub fn get_table(&mut self, name: &str) -> Result<Option<&Table>, DbError> {
        if(!self.tables.contains_key(name)) {
            self.read_table_from_disk(String::from(name)).map_err(|_| DbError::ReadTableFailed())?;
        }
        Ok(self.tables.get(name))
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}