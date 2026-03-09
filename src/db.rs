use crate::bufferpool::DiskManager;
use crate::bufferpool::{BufferPool, BufferPoolError};
use crate::errors::DbError;
use crate::iterators::AtomicIterator;
use crate::table::Table;
use dashmap::{mapref::entry::Entry, DashMap};
use parking_lot::RwLock;
use sanitise_file_name::sanitize;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub(crate) struct Database {
    pub(crate) tables: DashMap<usize, Arc<Table>>,
    table_names: DashMap<String, usize>,
    table_id: AtomicIterator<AtomicUsize>,
    bufferpool: Arc<BufferPool>,
    pub path: Option<PathBuf>,
    disk_manager: Arc<RwLock<DiskManager>>,
}

impl Database {
    pub const DEFAULT_PATH: &str = "db_data";
    pub fn new() -> Self {
        let temp_path = Self::create_temp_path();
        let disk_manager = Arc::new(RwLock::new(DiskManager::new(temp_path).unwrap()));
        Self {
            tables: DashMap::new(),
            table_names: DashMap::new(),
            table_id: AtomicIterator::default(),
            bufferpool: Arc::new(BufferPool::new(disk_manager.clone())),
            path: None,
            disk_manager,
        }
    }

    fn create_temp_path() -> PathBuf {
        let temp_path = std::env::temp_dir().join(format!("firstplacedb_{}", std::process::id()));
        std::fs::create_dir_all(&temp_path).expect("TODO: panic message");
        temp_path
    }

    pub fn create_table(&self, name: String, num_columns: usize, key_index: usize) {
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
            }
            Entry::Occupied(_) => {} //Todo better error handling
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

    pub fn open(&mut self, path: &str) -> Result<(), DbError> {
        //Todo: check test cases because sanitize path turns "" into "_"
        let sanitized_path = Some(PathBuf::from(Self::DEFAULT_PATH).join(sanitize(path)));
        self.path = sanitized_path.clone();
        self.disk_manager
            .write()
            .set_path(sanitized_path)?;
        Ok(())
    }

    pub fn close(&self) -> Result<(), DbError> {
        let dm = self.disk_manager.read();

        dm.write_table_names(&self.table_names)?;

        dm.write_tables(&self.tables, self.table_id.current())?;

        drop(dm);

        self.bufferpool.evict_all()?;

        Ok(())
    }
}
