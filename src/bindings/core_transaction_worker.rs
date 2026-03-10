use pyo3::prelude::*;
use crate::bindings::core_transaction::CoreTransaction;
use crate::transaction_worker::TransactionWorker;

#[pyclass]
pub struct CoreTransactionWorker {
    inner: TransactionWorker,
}

#[pymethods]
impl CoreTransactionWorker {
    #[new]
    pub fn new() -> Self {
        Self { inner: TransactionWorker::new() }
    }

    pub fn add_transaction(&mut self, txn: &Bound<CoreTransaction>) {
        // Snapshot ops out of the Python object while GIL is held.
        // After this the worker holds no Python objects.
        self.inner.add_transaction(txn.borrow().inner.ops.clone());
    }

    pub fn run(&mut self) {
        self.inner.run();
    }

    pub fn join(&self) {
        self.inner.join();
    }
}