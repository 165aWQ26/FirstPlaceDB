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
        //snapshot ops out of the Python object while GIL is held.
        self.inner.add_transaction(txn.borrow().inner.ops.clone());
    }

    pub fn run(&mut self, py: Python) {
        //release the GIL while running Rust-only worker thread
        py.detach(|| {
            self.inner.run();
        });
    }

    pub fn join(&self, py: Python) {
        //rel the GIL while blocking on the JoinHandle
        py.detach(|| {
            self.inner.join();
        });
    }
}