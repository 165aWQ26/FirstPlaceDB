use crate::db::Database;
use parking_lot::RwLock;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

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

    fn open(&mut self, path: &str) -> PyResult<()> {
        self.inner.write().open(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))
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
        self.inner.write().drop_table(name.as_str());
    }

    fn get_table(&self, name: String) -> Option<(usize, usize)> {
        self.inner.read().get_table(&name).map(|t| (t.num_data_columns, t.key_index))
    }

    fn table_exists(&self, name: String) -> bool {
        self.inner.read().table_exists(name.as_str())
    }
    
}
