use pyo3::{PyErrArguments, PyResult, exceptions::PyIndexError, pyclass, pymethods};

const MAX_RECORDS: usize = 512;

#[pyclass]
#[derive(Debug)]
struct Page {
    // num_records = data.len, so could do taht also
    data: Vec<i64>,
}

#[pymethods]
impl Page {
    #[new]
    pub fn new() -> Self {
        Page {
            data: Vec::with_capacity(MAX_RECORDS),
        }
    }

    pub fn has_capacity(&mut self) -> bool {
        self.data.len() < MAX_RECORDS
    }

    pub fn write(&mut self, val: i64) -> PyResult<()> {
        if self.has_capacity() {
            self.data.push(val);
            Ok(())
        } else {
            Err(PyIndexError::new_err("Page is full"))
        }
    }

    pub fn update(&mut self, index: usize, val: i64) -> PyResult<()> {
        if index >= self.data.len() {
            Err(PyIndexError::new_err(format!(
                "Index {} out of bounds",
                index
            )))
        } else {
            self.data[index] = val;
            Ok(())
        }
    }

    pub fn read(&self, index: usize) -> PyResult<i64> {
        self.data
            .get(index)
            .copied()
            .ok_or_else(|| PyIndexError::new_err("Index out of bounds"))
    }

    fn __len__(&self) -> usize {
        self.data.len()
    }
}
