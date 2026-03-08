use crate::bufferpool::bufferpool_worker::{BufferPoolOp, BufferPoolWorker};
use crate::bufferpool::disk_manager::DiskManager;
use crate::bufferpool::errors::BufferPoolError;
use crate::bufferpool::eviction_policy::EvictionPolicy;
use crate::page::{Page, PageError};
use crate::page_collection::PageId;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

pub type FrameId = usize;
// What the TA ended up using
pub const BP_CAP: usize = 256;

struct InnerFrame {
    page: Page,
    pid: Option<PageId>,
}

pub struct Frame {
    //Todo: you don't always write back dirty pages & either your explanation was bad or I misunderstood.
    //I think a disconnect was that you are using this to check if you should evict/if a frame was already evicted... wrong idea
    //This only checks if you write back on evict.
    //You should have pin count on each frame too
    //Possible race condition: between when fid is returned in load and when the locks are acquired.
    //Consider: cache t1: hit --> return fid --> interrupt
    //                t2: cache miss --> evict --> picks fid --> acquires lock --> writes new value releases lock.
    //                t1: resume --> incorrect read
    //                Logical Sol: Maintain a pin count on a frame.
    dirty: AtomicBool,
    inner: RwLock<InnerFrame>,
}

impl Frame {
    pub fn new() -> Self {
        Self {
            dirty: AtomicBool::new(false),
            inner: RwLock::new(InnerFrame {
                page: Default::default(),
                pid: None,
            }),
        }
    }

    pub fn load(&self, pid: PageId, page: Page) {
        let mut guard = self.inner.write();
        guard.page = page;
        guard.pid = Some(pid);
        self.dirty.store(false, Ordering::Release);
    }

    pub fn init(&self, pid: PageId) {
        let mut guard = self.inner.write();
        guard.pid = Some(pid);
        guard.page = Page::default();
        self.dirty.store(false, Ordering::Release);
    }

    pub fn read(&self, offset: usize) -> Result<Option<i64>, PageError> {
        let guard = self.inner.read();
        guard.page.read(offset)
    }

    pub fn write(&self, value: Option<i64>, offset: usize) -> Result<(), PageError> {
        let mut guard = self.inner.write();
        guard.page.write(value, offset)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    pub fn update(&self, offset: usize, value: Option<i64>) -> Result<(), PageError> {
        let mut guard = self.inner.write();
        guard.page.update(offset, value)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    pub fn get_page_copy(&self) -> Page {
        let guard = self.inner.read();
        guard.page.clone()
    }

    pub fn release(&self) {
        let mut guard = self.inner.write();
        guard.pid = None;
        self.dirty.store(false, Ordering::Release);
    }

    pub fn has_capacity(&self) -> bool {
        let guard = self.inner.read();
        guard.page.has_capacity()
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }
    pub fn pid(&self) -> Option<PageId> {
        self.inner.read().pid
    }
}

struct EvictionState {
    pub(super) policy: EvictionPolicy,
    pub(super) free_list: Vec<FrameId>,
}

impl EvictionState {
    pub fn new(capacity: usize) -> Self {
        Self {
            policy: EvictionPolicy::new(capacity),
            free_list: (0..capacity).collect(),
        }
    }
}

pub struct BufferPool {
    page_table: DashMap<PageId, FrameId>,
    frames: Vec<Frame>,
    eviction_state: Mutex<EvictionState>,
    disk_manager: Arc<DiskManager>,
    command_tx: mpsc::Sender<BufferPoolOp>,
    _bg_thread: thread::JoinHandle<()>,
}

impl BufferPool {
    pub fn new(disk_manager: DiskManager) -> BufferPool {
        let disk_manager = Arc::new(disk_manager);
        let worker_dm = Arc::clone(&disk_manager);
        let (tx, rx) = mpsc::channel(); // unbounded
        let handle = thread::spawn(move || BufferPoolWorker::new(rx, worker_dm).run());
        Self {
            page_table: DashMap::new(),
            frames: (0..BP_CAP).map(|_| Frame::new()).collect(),
            eviction_state: Mutex::new(EvictionState::new(BP_CAP)),
            disk_manager,
            command_tx: tx,
            _bg_thread: handle,
        }
    }

    //todo: when writing close table later, consider the race condition where a write occurs after
    //this function scans the bp. Need to hold write locks on everything before proceeding and reject
    //essentially this function on its own does not guarantee a fully flushed bp.
    pub fn evict_all(&self) -> Result<(), BufferPoolError> {
        let pages: Vec<(PageId, Page)> = self
            .page_table
            .iter()
            .filter_map(|entry| {
                let pid = *entry.key();
                let fid = *entry.value();
                if self.frames[fid].is_dirty() {
                    Some((pid, self.frames[fid].get_page_copy()))
                } else {
                    None
                }
            })
            .collect();
        self.flush_pages(pages)?;

        let mut ev = self.eviction_state.lock();
        let pids: Vec<PageId> = self.page_table.iter().map(|e| *e.key()).collect();
        for pid in pids {
            if let Some((_, fid)) = self.page_table.remove(&pid) {
                self.frames[fid].release();
                ev.free_list.push(fid);
            }
        }

        Ok(())
    }
    pub fn read(&self, pid: PageId, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        loop {
            let fid = self.resolve_or_load(pid)?;
            let guard = self.frames[fid].inner.read();
            if guard.pid == Some(pid) {
                return Ok(guard.page.read(offset)?);
            }
        }
    }

    pub fn write(
        &self,
        pid: PageId,
        val: Option<i64>,
        offset: usize,
    ) -> Result<(), BufferPoolError> {
        loop {
            let fid = self.resolve_or_load(pid)?;
            let mut guard = self.frames[fid].inner.write();
            if guard.pid == Some(pid) {
                guard.page.write(val, offset)?;
                self.frames[fid].dirty.store(true, Ordering::Release);
                return Ok(());
            }
        }
    }

    pub fn update(
        &self,
        pid: PageId,
        offset: usize,
        val: Option<i64>,
    ) -> Result<(), BufferPoolError> {
        loop {
            let fid = self.resolve_or_load(pid)?;
            let mut guard = self.frames[fid].inner.write();
            if guard.pid == Some(pid) {
                guard.page.update(offset, val)?;
                self.frames[fid].dirty.store(true, Ordering::Release);
                return Ok(());
            }
        }
    }

    // TODO wire up for transaction
    // pub fn flush_page(&self, pid: PageId) -> Result<(), BufferPoolError> {
    //     if let Some(entry) = self.page_table.get(&pid) {
    //         let fid = *entry;
    //         self.flush_frame(pid, fid)?;
    //     }
    //     Ok(())
    // }

    //Todo: You are not giving the worker a way to return any result.
    //A pattern I used was sending a different tx channel to the worker and keeping the rx.
    //So two channels: one for bp (tx) --> worker (rx) passed on construction and one for worker (tx) -> bp (rx) passed
    // on function call.
    //There may be a better way to do this.
    pub fn flush_pages(&self, pages: Vec<(PageId, Page)>) -> Result<(), BufferPoolError> {
        let (res_tx, res_rx) = mpsc::sync_channel(1);
        self.command_tx
            .send(BufferPoolOp::FlushPages { pages, res_tx })
            .map_err(|_| BufferPoolError::BackgroundWorkerDead)?;
        res_rx
            .recv()
            .map_err(|_| BufferPoolError::BackgroundWorkerDead)?
    }

    // TODO not needed for now
    // pub fn flush_async(&self, pids: Vec<PageId>) -> Result<(), BufferPoolError> {
    //     self.command_tx
    //         .send(BufferPoolOp::FlushDirty {
    //             pids,
    //             response: None,
    //         })
    //         .map_err(|_| BufferPoolError::BackgroundWorkerDead)
    // }

    //Todo: A thought,
    fn flush_frame(&self, pid: PageId, fid: FrameId) -> Result<(), BufferPoolError> {
        if self.frames[fid].is_dirty() {
            let page = self.frames[fid].get_page_copy();
            self.disk_manager.write_page(pid, &page)?;
            self.frames[fid].clear_dirty();
        }
        Ok(())
    }

    /* Notes

    todo Try to make it RwLock the eviction policy with on_access being read only
        This would allow us to simplify logic.

    todo Move the free list check into the eviction policy
     */

    fn resolve_or_load(&self, pid: PageId) -> Result<FrameId, BufferPoolError> {
        // Case 1: cache hit

        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            drop(entry);
            if let Some(mut eviction_lock) = self.eviction_state.try_lock() {
                eviction_lock.policy.on_access(fid);
            }
            return Ok(fid);
        }

        let mut eviction_lock = self.eviction_state.lock();

        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            drop(entry);
            eviction_lock.policy.on_access(fid);
            drop(eviction_lock);
            return Ok(fid);
        }

        let (fid, victim_pid_opt) = if let Some(free) = eviction_lock.free_list.pop() {
            (free, None)
        } else {
            let victim_fid = eviction_lock
                .policy
                .evict_victim()
                .ok_or(BufferPoolError::AllFramesPinned)?;
            let victim_pid = self.frames[victim_fid]
                .pid()
                .ok_or(BufferPoolError::PidNotInFrame)?;
            self.page_table.remove(&victim_pid);
            (victim_fid, Some(victim_pid))
        };

        drop(eviction_lock);

        if let Some(victim_pid) = victim_pid_opt {
            self.flush_frame(victim_pid, fid)?;
            self.frames[fid].release();
        }

        if self.disk_manager.page_exists(pid) {
            let page = self.disk_manager.read_page(pid)?;
            self.frames[fid].load(pid, page);
        } else {
            self.frames[fid].init(pid);
        }

        let mut eviction_lock = self.eviction_state.lock();
        self.page_table.insert(pid, fid);
        eviction_lock.policy.on_insert(fid);
        drop(eviction_lock);

        Ok(fid)
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let _ = self.evict_all();
        let _ = self.command_tx.send(BufferPoolOp::Shutdown);
    }
}
