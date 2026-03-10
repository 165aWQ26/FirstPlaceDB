use crate::table::Table;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MERGE_INTERVAL_MS: u64 = 50;

pub struct MergeWorker {
    workers: Mutex<Vec<(Arc<AtomicBool>, JoinHandle<()>)>>,
}

impl MergeWorker {
    pub fn new() -> Self {
        Self { workers: Mutex::new(Vec::new()) }
    }

    pub fn spawn_for_table(&self, table: Arc<Table>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_ref = shutdown.clone();

        let handle = thread::spawn(move || {
            while !shutdown_ref.load(Ordering::Relaxed) {
                if !table.dirty_base_rids.is_empty() {
                    let _ = table.merge();
                }
                thread::sleep(Duration::from_millis(MERGE_INTERVAL_MS));
            }
        });

        self.workers.lock().unwrap().push((shutdown, handle));
    }

    pub fn stop(&self) {
        let mut workers = self.workers.lock().unwrap();
        for (shutdown, handle) in workers.drain(..) {
            shutdown.store(true, Ordering::Relaxed);
            handle.join().ok();
        }
    }
}