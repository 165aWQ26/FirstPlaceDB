use crate::db::Database;
use crate::table::Table;
use parking_lot::{Mutex, RwLock};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use crate::iterators::AtomicIterator;

#[pyclass]
pub struct CoreDatabase {
    pub(crate) inner: Arc<RwLock<Database>>,
}

#[pymethods]
impl CoreDatabase {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Database::new()))
        }
    }

    fn open(&mut self, path: &str) {
        self.inner.write().open(path);
    }

    fn close(&self) -> PyResult<()> {
        self.inner.read()
            .close()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn create_table(&self, name: String, num_columns: usize, key_index: usize) {
        self.inner.read().create_table(name, num_columns, key_index)
    }

    fn drop_table(&self, name: String) {
        self.inner.lock().drop_table(name.as_str())
    }

    fn get_table(&self, name: String) -> PyResult<Option<(usize, usize)>> {
        let mut lock = self.inner.lock();
        let opt = lock.get_table(name.as_str()).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(opt.map(|t| (t.table_ctx.total_cols - Table::NUM_META_PAGES, t.key_index)))
    }
    
}
