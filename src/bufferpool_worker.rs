use crate::bufferpool::BufferPoolError;
use crate::disk_manager::DiskManager;
use crate::page_collection::{PageId};
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc};
use crate::page::Page;

pub enum BufferPoolOp {
    FlushPages {
        pages: Vec<(PageId, Page)>,
        res_tx: mpsc::SyncSender<Result<(), BufferPoolError>>,
    },

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
