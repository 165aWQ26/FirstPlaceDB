use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::bufferpool::BufferPool;
use crate::db_error::DbError;
use crate::table::Table;

pub struct Database {
    pub(crate) tables: FxHashMap<String, Table>,
    bufferpool: Arc<Mutex<BufferPool>>,
    pub(crate) path: String,
    table_id_iterator: std::ops::RangeFrom<usize>,
    // For "In-memory" db, you still need a  temp dir so the
    // bufferpool can still evict/reload pages.  Cleaned up on close().
    fallback_dir: Option<std::path::PathBuf>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
            bufferpool: Arc::new(Mutex::new(BufferPool::default())),
            path: String::new(),
            table_id_iterator: 0..,
            fallback_dir: None,
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        if self.path.is_empty() {
            let dir = std::env::temp_dir().join(format!("firstplacedb_{}", std::process::id()));
            let _ = std::fs::create_dir_all(&dir);
            self.path = format!("{}/", dir.display());
            self.fallback_dir = Some(dir);
        }

        let mut table_path = self.path.clone();
        table_path.push_str(&name);
        table_path.push('/');
        let _ = std::fs::create_dir_all(&table_path);

        let table = Table::new(
            table_path.clone(),
            num_columns,
            key_index,
            Arc::clone(&self.bufferpool),
            self.table_id_iterator.next().unwrap(),
        );

        // Register name and context with the bufferpool so eviction can look them up
        self.bufferpool.lock().append_name(name.clone());
        self.bufferpool
            .lock()
            .append_context(table.table_ctx.clone());

        self.tables.insert(name, table);
    }

    pub fn open(&mut self, path: &str) {
        self.path = format!("{}/", path);
    }

    pub fn close(&self) -> Result<(), DbError> {
        self.bufferpool.lock().evict_all()?;
        for table in self.tables.values() {
            std::fs::create_dir_all(&table.table_ctx.path)
                .map_err(|_| DbError::WriteTableFailed())?;
            table
                .write_to_disk(table.table_ctx.path.clone())
                .map_err(|_| DbError::WriteTableFailed())?;
        }
        Ok(())
    }

    pub fn read_table_from_disk(&mut self, name: &str, table_path: String) -> Result<(), DbError> {
        let table_id = self.table_id_iterator.next().unwrap();
        let mut table = Table::new(
            table_path.clone(),
            0,
            0,
            Arc::clone(&self.bufferpool),
            table_id,
        );
        table
            .read_from_disk(table_path)
            .map_err(|_| DbError::ReadTableFailed())?;

        self.bufferpool.lock().append_name(name.to_string());
        self.bufferpool
            .lock()
            .append_context(table.table_ctx.clone());

        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    pub fn get_table(&mut self, name: &str) -> Result<Option<&mut Table>, DbError> {
        if !self.tables.contains_key(name) {
            let mut table_path = self.path.clone();
            table_path.push_str(name);
            table_path.push('/');
            self.read_table_from_disk(name, table_path)
                .map_err(|_| DbError::ReadTableFailed())?;
        }
        Ok(self.tables.get_mut(name))
    }

    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
