use crate::transaction::{Transaction, QueryOp};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct TransactionWorker {
    pub transactions: Vec<Vec<QueryOp>>,   // each inner Vec is one transaction's ops
    handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl TransactionWorker {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
            handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn add_transaction(&mut self, ops: Vec<QueryOp>) {
        self.transactions.push(ops);
    }

    pub fn run(&mut self) {
        let transactions = self.transactions.clone();
        let handle = thread::spawn(move || {
            for ops in transactions {
                loop {
                    if Transaction::from_ops(ops.clone()).run() {
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        });
        *self.handle.lock().unwrap() = Some(handle);
    }

    pub fn join(&self) {
        if let Some(h) = self.handle.lock().unwrap().take() {
            h.join().ok();
        }
    }
}