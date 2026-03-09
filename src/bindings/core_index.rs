use crate::table::Table;
use std::sync::Arc;
use pyo3::prelude::*;

#[pyclass]
pub struct CoreIndex {
    pub(crate) table: Arc<Table>,
}

#[pymethods]
impl CoreIndex {
    pub fn create_index(&self, col: usize) {
        if col < self.table.num_data_columns {
            self.table.indices[col].enable()
        }
    }

    pub fn drop_index(&self, col: usize) {
        if col < self.table.num_data_columns {
            self.table.indices[col].disable()
        }
    }
}