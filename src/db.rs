use dashmap::{DashMap, mapref::entry::Entry};
use std::sync::Arc;
use crate::table::Table;
use crate::bufferpool::BufferPool;
use crate::bufferpool::DiskManager;
use crate::iterators::AtomicIterator;

struct Database {
    tables: DashMap<usize, Arc<Table>>,
    table_names: DashMap<String, usize>,
    table_id: AtomicIterator,
    bufferpool: Arc<BufferPool>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            table_names: DashMap::new(),
            table_id: AtomicIterator::new(),
            bufferpool: Arc::new(BufferPool::new(DiskManager::new("db").unwrap())),
        }
    }

    pub fn create_table(
        &self,
        name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Result<usize, String> {

        //atomic check table_names and return an entry 
        match self.table_names.entry(name.clone()) {
            Entry::Vacant(vacant) => {
                let table_id = self.table_id.next();
                let table = Arc::new(Table::new(
                    name.clone(),
                    num_columns,
                    key_index,
                    table_id,
                    self.bufferpool.clone(),
                ));

                //insert into tables
                self.tables.insert(table_id, table);

                //insert into table_names
                vacant.insert(table_id);

                Ok(table_id)
            }
            Entry::Occupied(_) => Err("Table alr exists".to_string()), //Todo better error handling
        }
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let id = self.table_names.get(name)?.value().clone();
        self.tables.get(&id).map(|t| t.value().clone())
    }

    pub fn drop_table(&self, name: &str) -> bool {
        if let Some((_, table_id)) = self.table_names.remove(name) {
            self.tables.remove(&table_id);
            true
        } else {
            false
        }
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.table_names.contains_key(name)
    }
}