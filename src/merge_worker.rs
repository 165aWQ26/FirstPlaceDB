use crate::lock_manager::LockManager;
use crate::table::Table;
use std::sync::{mpsc, Arc};
use std::thread;

pub enum MergeOp {
    Merge,
    Shutdown,
}

pub struct MergeWorker {
    pub cmd_tx: mpsc::Sender<MergeOp>,
    _thread: thread::JoinHandle<()>,
}

impl MergeWorker {
    pub fn new(table: Arc<Table>, lock_manager: Arc<LockManager>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let _thread = thread::spawn(move || {
            while let Ok(op) = cmd_rx.recv() {
                match op {
                    MergeOp::Merge => {
                        let safe_rids: Vec<i64> = table
                            .dirty_base_rids
                            .iter()
                            .map(|r| *r)
                            .filter(|&rid| {
                                let key = table
                                    .read_latest_single(rid, table.key_index)
                                    .ok()
                                    .flatten();
                                match key {
                                    Some(k) => !lock_manager
                                        .is_exclusively_locked(table.table_id, k),
                                    None => false,
                                }
                            })
                            .collect();

                        if !safe_rids.is_empty() {
                            let _ = table.merge_rids(&safe_rids);
                        }
                    }
                    MergeOp::Shutdown => break,
                }
            }
        });

        Self { cmd_tx, _thread }
    }

    pub fn trigger(&self) {
        let _ = self.cmd_tx.send(MergeOp::Merge);
    }

    pub fn shutdown(&self) {
        let _ = self.cmd_tx.send(MergeOp::Shutdown);
    }
}