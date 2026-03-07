use crate::bufferpool::BufferPoolError;
use crate::disk_manager::DiskManager;
use crate::page_collection::Pid;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc};

pub enum BufferPoolOp {
    /// Evict N pages from buffer pool
    FlushAll {
        pids: Vec<Pid>,
        res_tx: mpsc::SyncSender<Result<(), BufferPoolError>>,
    },

    /// Shutdown the background worker
    Shutdown,
}

pub(crate) struct BufferPoolWorker {
    cmd_rx: Receiver<BufferPoolOp>,
    disk_manager: Arc<DiskManager>,
}

impl BufferPoolWorker {
    pub fn new(receiver: Receiver<BufferPoolOp>, disk_manager: Arc<DiskManager>) -> Self {
        Self {
            cmd_rx: receiver,
            disk_manager,
        }
    }

    fn run(self) {
        while let Ok(op) = self.cmd_rx.recv() {
            match (op) {
                BufferPoolOp::FlushAll { pids, res_tx: response } => {
                    let result = self.handle_evict(pids);
                    res_tx
                }
                BufferPoolOp::Shutdown => {
                    break;
                }
            }
        }
    }

    fn handle_evict(&self, pids: Vec<Pid>) -> Result<(), BufferPoolError> {
        todo!()
    }
}
