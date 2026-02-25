use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use crate::table::Table as RustTable;

#[pyclass]
pub struct Table {
    inner: RustTable
}