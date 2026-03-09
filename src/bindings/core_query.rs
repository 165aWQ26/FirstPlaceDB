use std::path::Path;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::bufferpool::{BufferPool, DiskManager};
use crate::query::Query;
use crate::table::Table;

#[pyclass]
pub struct CoreQuery {
    inner: Query,
    disk_manager: DiskManager,
    bufferpool: Arc<BufferPool>
}

#[pymethods]
impl CoreQuery {
    // TODO fix when I figure out how wtf table is written to in diskmanager
    #[new]
    fn new(name: String, num_columns: usize, key_index: usize, path: String) -> Self {
        // let table_path = format!("{}/lstore_data/{}", path, name);
        let table_path = Path::new(&path).join("tables").join(name).to_str().unwrap;
        let dm = Arc::new(RwLock::new(DiskManager::new(&dir).expect("failed to create disk manager")));
        let bp = Arc::new(BufferPool::new(dm));
        let table = Table::new(name, num_columns, key_index, 0, bp);
        Self {
            inner: Query::new(table),
        }
    }

    #[pyo3(signature = (*columns))]
    fn insert(&mut self, columns: Vec<i64>) -> bool {
        let mut nullable_rec = Vec::with_capacity(columns.len() + 4);
        nullable_rec.extend(columns.into_iter().map(Some));
        self.inner.insert(nullable_rec).unwrap_or(false)
    }

    fn select(
        &self,
        search_key: i64,
        search_key_index: usize,
        projected_columns_index: Vec<i64>,
    ) -> PyResult<Vec<Vec<Option<i64>>>> {
        self.inner
            .select(search_key, search_key_index, &projected_columns_index)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (primary_key, *columns))]
    fn update(&mut self, primary_key: i64, columns: Vec<Option<i64>>) -> bool {
        self.inner.update(primary_key, columns).unwrap_or(false)
    }

    fn delete(&mut self, key: i64) -> bool {
        self.inner.delete(key).unwrap_or(false)
    }

    fn sum(&self, start_range: i64, end_range: i64, col: usize) -> PyResult<i64> {
        self.inner
            .sum(start_range, end_range, col)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn increment(&mut self, key: i64, column: usize) -> bool {
        self.inner.increment(key, column).unwrap_or(false)
    }

    fn sum_version(&self, start_range: i64, end_range: i64, column: usize, relative_version: i64) -> PyResult<i64>{
        self.inner.sum_version(start_range, end_range, column, relative_version)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn select_version(
        &self,
        search_key: i64,
        search_key_index: usize,
        projected_columns_index: Vec<i64>,
        relative_version: i64
    ) -> PyResult<Vec<Vec<Option<i64>>>> {
        self.inner
            .select_version(search_key, search_key_index, &projected_columns_index,relative_version)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
    
}
