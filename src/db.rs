use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::bufferpool::BufferPool;
use crate::db_error::DbError;
use crate::table::Table;

pub struct Database {
    pub(crate) tables: FxHashMap<String, Table>,
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
            table_id_iterator: 0..,
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //Todo Throw an error if path is ""
        let mut table_path = self.path.clone();
        table_path.push_str("/");
        table_path.push_str(&name);

        let table = Table::new(
            table_path.clone(),
            num_columns,
            key_index,
            Arc::clone(&self.bufferpool),
            self.table_id_iterator.next().unwrap(),
        );
        self.tables.insert(name.clone(), table);
        self.bufferpool.lock().append_name(name)
    }

    pub fn open(&mut self, path: &str) {
        //create_indexes

        self.path.push_str(path);
        self.path.push('/');
    }

    pub fn close(&self) -> Result<(), DbError> {
        //Need to implement indexes stuff
        //  Drop index function
        self.bufferpool.lock().evict_all(&self.tables)?;
        for table in self.tables.values() {
            (*table)
                .write_to_disk(self.path.clone())
                .map_err(|_| DbError::WriteTableFailed())?;
        }

        Ok(())
    }

    pub fn read_table_from_disk(&mut self, name: String) -> Result<(), DbError> {
        //Assumes only one table
        let mut table = Table::new(name.clone(), 0, 0, Arc::clone(&self.bufferpool), 0);
        self.path = name.clone();
        table
            .read_from_disk(self.path.clone())
            .map_err(|_| DbError::ReadTableFailed())?;
        self.tables.insert(name, table);

        Ok(())
    }

    pub fn get_table(&mut self, name: &str) -> Result<Option<&mut Table>, DbError> {
        if !self.tables.contains_key(name) {
            let mut table_path = self.path.clone();
            table_path.push_str("/");
            table_path.push_str(&name);
            self.read_table_from_disk(table_path.clone())
                .map_err(|_| DbError::ReadTableFailed())?;
            self.bufferpool.lock().append_name(table_path.clone());
        }
        Ok(self.tables.get_mut(name))
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}
