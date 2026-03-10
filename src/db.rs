use crate::bufferpool::DiskManager;
use crate::bufferpool::BufferPool;
use crate::errors::DbError;
use crate::iterators::AtomicIterator;
use crate::merge_worker::MergeWorker;
use crate::table::Table;
use dashmap::{mapref::entry::Entry, DashMap};
use parking_lot::RwLock;
use sanitise_file_name::sanitize;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use crate::disk_manager::TableCounters;

pub(crate) struct Database {
    pub(crate) tables: DashMap<usize, Arc<Table>>,
    table_names: DashMap<String, usize>,
    table_id: AtomicIterator<AtomicUsize>,
    bufferpool: Arc<BufferPool>,
    pub path: Option<PathBuf>,
    disk_manager: Arc<RwLock<DiskManager>>,
    merge_worker: MergeWorker,
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
            merge_worker: MergeWorker::new(),
        }
    }

    fn create_temp_path() -> PathBuf {
        let temp_path = std::env::temp_dir().join(format!("firstplacedb_{}", std::process::id()));
        std::fs::create_dir_all(&temp_path).expect("TODO: panic message");
        temp_path
    }

    pub fn create_table(&self, name: String, num_columns: usize, key_index: usize) {
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

                self.merge_worker.spawn_for_table(table.clone());
                self.tables.insert(table_id, table);
                vacant.insert(table_id);
            }
            Entry::Occupied(_) => {}
        }
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let id = *self.table_names.get(name)?.value();
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
        let sanitized_path = Some(PathBuf::from(Self::DEFAULT_PATH).join(sanitize(path)));
        self.path = sanitized_path.clone();
        self.disk_manager.write().set_path(sanitized_path)?;

        let (table_metas, next_table_id) = {
            let dm = self.disk_manager.read();
            dm.read_tables()?
        };

        let name_pairs = {
            let dm = self.disk_manager.read();
            dm.read_table_names()?
        };

        self.table_id.set(next_table_id);

        for (name, table_id) in name_pairs {
            self.table_names.insert(name.clone(), table_id);

            if let Some(meta) = table_metas.iter().find(|m| m.table_id == table_id) {
                let dm = self.disk_manager.read();

                let page_dir_pairs = dm.read_page_directory(table_id)?;
                let counters = dm.read_table_counters(table_id)?;
                let primary_pairs = dm.read_primary_index(table_id)?;
                drop(dm);

                let table = Arc::new(Table::restore(
                    name,
                    meta.num_data_columns,
                    meta.key_index,
                    table_id,
                    self.bufferpool.clone(),
                    page_dir_pairs,
                    counters,
                    primary_pairs,
                ));

                self.merge_worker.spawn_for_table(table.clone());
                self.tables.insert(table_id, table);
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<(), DbError> {
        self.merge_worker.stop();

        let dm = self.disk_manager.read();

        dm.write_table_names(&self.table_names)?;
        dm.write_tables(&self.tables, self.table_id.current())?;

        for entry in self.tables.iter() {
            let table = entry.value();
            let tid = table.table_id;

            let page_dir = table.page_directory.snapshot();
            dm.write_page_directory(tid, &page_dir)?;

            let counters = TableCounters {
                next_rid: table.rid.current(),
                base_next_addr: table.page_ranges.base_next_addr(),
                tail_next_addr: table.page_ranges.tail_next_addr(),
                pid_next_start: table.page_ranges.pid_next_start(),
                base_collections: table.page_ranges.base_collection_pid_ranges(),
                tail_collections: table.page_ranges.tail_collection_pid_ranges(),
            };

            dm.write_table_counters(tid, &counters)?;

            let primary_pairs = table.indices[table.key_index].all_pairs();
            dm.write_primary_index(tid, &primary_pairs)?;
        }

        drop(dm);

        self.bufferpool.evict_all()?;

        Ok(())
    }
}