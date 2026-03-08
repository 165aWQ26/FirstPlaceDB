use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc};
use crate::bufferpool::disk_manager::DiskManager;
use crate::bufferpool::errors::BufferPoolError;
use crate::page::Page;
use crate::page_collection::PageId;

pub enum BufferPoolOp {
    /// Evict N pages from buffer pool
    FlushPages {
        pages: Vec<(PageId, Page)>,
        res_tx: mpsc::SyncSender<Result<(), BufferPoolError>>,
    },

    /// Shutdown the background worker
    Shutdown,
}

pub struct BufferPoolWorker {
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

    pub(crate) fn run(self) {
        while let Ok(op) = self.cmd_rx.recv() {
            match (op) {
                BufferPoolOp::FlushPages { pages, res_tx } => {
                    let result = self.handle_evict(pages);
                    res_tx.send(result).unwrap();
                }
                BufferPoolOp::Shutdown => {
                    break;
                }
            }
        }
    }

    fn handle_evict(&self, pages: Vec<(PageId, Page)>) -> Result<(), BufferPoolError> {
        for (pid, page) in pages {
            self.disk_manager.write_page(pid, &page)?;
        }
        Ok(())
    }
}
