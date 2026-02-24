use crate::db::Database;
use crate::table::Table;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass]
pub struct CoreDatabase {
    pub(crate) inner: Arc<Mutex<Database>>,
}

#[pymethods]
impl CoreDatabase {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Database::new())),
        }
    }

    fn open(&self, path: &str) {
        self.inner.lock().open(path)
    }

    fn close(&self) -> PyResult<()> {
        self.inner
            .lock()
            .close()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn create_table(&self, name: String, num_columns: usize, key_index: usize) {
        self.inner.lock().create_table(name, num_columns, key_index)
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

