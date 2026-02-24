use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::db::Database as RustDatabase;
use crate::table::Table as RustTable;
use crate::bindings::table::Table;


#[pyclass]
#[derive(Default, Clone)]
pub struct Database {
    inner: RustDatabase,
}

#[pymethods]
impl Database {
    #[new]
    fn new() -> Self {
        Self {
            inner: RustDatabase::default(),
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) -> (String, Self) {
        (self.inner.create_table(name.clone(), num_columns, key_index), self.clone())
    }

    //Return name
    pub fn get_table(&self, name: &str) -> (Option<String>, Self) {
        (self.inner.get_table(name).is_some().then(|| name.to_string()), *self)
    }

    pub fn drop_table(&mut self, name: &str) {
        self.inner.drop_table(name);
    }
}