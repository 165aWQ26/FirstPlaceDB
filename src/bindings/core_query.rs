use crate::bindings::core_db::CoreDatabase;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::db::Database;
use crate::query::Query;

#[pyclass]
pub struct CoreQuery {
    db: Arc<Database>,
    table_name: String,
}

impl CoreQuery {
    fn with_query<F, T>(&self, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut Query) -> Result<T, crate::db_error::DbError>,
    {
        let mut db = self.db;
        let table = db.tables.get(self.table_name.as_str()).ok_or_else(|| {
            PyRuntimeError::new_err(format!("table '{}' not found", self.table_name))
        })?;
        let mut query = Query::new(table);
        f(&mut query).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[pymethods]
impl CoreQuery {
    #[new]
    fn new(db: &CoreDatabase, table_name: String) -> PyResult<Self> {
        {
            let db_lock = db.inner;
            if !db_lock.get_table(table_name.as_str()) {
                return Err(PyRuntimeError::new_err(format!(
                    "table '{}' not found",
                    table_name
                )));
            }
        }
        Ok(Self {
            db: Arc::clone(&db.inner),
            table_name,
        })
    }

    fn insert(&self, record: Vec<Option<i64>>) -> PyResult<bool> {
        self.with_query(|q| q.insert(record))
    }

    fn select(
        &self,
        search_key: i64,
        search_key_index: usize,
        projected_columns_index: Vec<i64>,
    ) -> PyResult<Vec<Vec<Option<i64>>>> {
        self.with_query(|q| q.select(search_key, search_key_index, &projected_columns_index))
    }

    fn select_version(
        &self,
        search_key: i64,
        search_key_index: usize,
        projected_columns_index: Vec<i64>,
        relative_version: i64,
    ) -> PyResult<Vec<Vec<Option<i64>>>> {
        self.with_query(|q| {
            q.select_version(
                search_key,
                search_key_index,
                &projected_columns_index,
                relative_version,
            )
        })
    }

    fn update(&self, primary_key: i64, record: Vec<Option<i64>>) -> PyResult<bool> {
        self.with_query(|q| q.update(primary_key, record))
    }

    fn delete(&self, primary_key: i64) -> PyResult<bool> {
        self.with_query(|q| q.delete(primary_key))
    }

    fn sum(&self, start_range: i64, end_range: i64, col: usize) -> PyResult<i64> {
        self.with_query(|q| q.sum(start_range, end_range, col))
    }

    fn sum_version(
        &self,
        start_range: i64,
        end_range: i64,
        col: usize,
        relative_version: i64,
    ) -> PyResult<i64> {
        self.with_query(|q| q.sum_version(start_range, end_range, col, relative_version))
    }

    fn increment(&self, key: i64, column: usize) -> PyResult<bool> {
        self.with_query(|q| q.increment(key, column))
    }
}