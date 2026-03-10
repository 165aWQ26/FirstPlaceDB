use crate::bindings::CoreIndex;
use crate::table::Table;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass]
pub struct CoreTable {
    pub(crate) inner: Arc<Table>,
}

#[pymethods]
impl CoreTable {
    #[getter]
    fn num_columns(&self) -> usize {
        self.inner.num_data_columns
    }

    #[getter]
    fn key_index(&self) -> usize {
        self.inner.key_index
    }

    #[getter]
    fn index(&self) -> CoreIndex {
        CoreIndex {
            table: self.inner.clone(),
        }
    }
}
