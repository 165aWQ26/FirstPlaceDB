use crate::bufferpool::bufferpool_worker::{BufferPoolOp, BufferPoolWorker};
use crate::disk_manager::DiskManager;
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

pub struct BufferPool {
    page_table: DashMap<PageId, FrameId>,
    frames: Vec<Frame>,
    eviction_policy: Mutex<EvictionPolicy>,
    disk_manager: Arc<RwLock<DiskManager>>,
    command_tx: mpsc::Sender<BufferPoolOp>,
    _bg_thread: thread::JoinHandle<()>,
}

impl BufferPool {
    pub fn new(disk_manager: Arc<RwLock<DiskManager>>) -> BufferPool {
        let worker_dm = Arc::clone(&disk_manager);
        let (tx, rx) = mpsc::channel(); // unbounded
        let handle = thread::spawn(move || BufferPoolWorker::new(rx, worker_dm).run());
        Self {
            page_table: DashMap::new(),
            frames: (0..BP_CAP).map(|_| Frame::new()).collect(),
            eviction_policy: Mutex::new(EvictionPolicy::new(BP_CAP)),
            disk_manager,
            command_tx: tx,
            _bg_thread: handle,
        }
    }

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

        let mut ev = self.eviction_policy.lock();
        let pids: Vec<PageId> = self.page_table.iter().map(|e| *e.key()).collect();
        for pid in pids {
            if let Some((_, fid)) = self.page_table.remove(&pid) {
                self.frames[fid].release();
                ev.release_frame(fid)
            }
        }
        Ok(())
    }

    pub fn read(&self, pid: PageId, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        loop {
            let fid = self.resolve_or_load(pid)?;
            let guard = self.frames[fid].inner.read();
            if guard.pid == Some(pid) { //pathological edge case, loop if the race occurs
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
            if guard.pid == Some(pid) { //pathological edge case, loop if the race occurs
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
            if guard.pid == Some(pid) { //pathological edge case, loop if the race occurs
                guard.page.update(offset, val)?;
                self.frames[fid].dirty.store(true, Ordering::Release);
                return Ok(())
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
            self.disk_manager.read().write_page(pid, &page)?;
            self.frames[fid].clear_dirty();
        }
        Ok(())
    }

    fn check_cache_hit(&self, pid: PageId) -> Option<FrameId> {
        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            drop(entry);
            self.eviction_policy.lock().on_access(fid);
            return Some(fid)
        }
        None
    }

    fn check_cache_race(&self, pid: PageId, policy: &mut EvictionPolicy) -> Option<FrameId> {
        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            drop(entry);
            policy.on_access(fid);
            return Some(fid)
        }
        None
    }

    fn evict_frame(&self, fid: FrameId) -> Result<(), BufferPoolError> {
        let victim_pid = self.frames[fid]
            .pid()
            .ok_or(BufferPoolError::PidNotInFrame)?;
        self.page_table.remove(&victim_pid);
        self.flush_frame(victim_pid, fid)?;
        self.frames[fid].release();
        Ok(())
    }

    fn read_or_init_page(&self, pid: PageId, fid: FrameId) -> Result<(), BufferPoolError> {
        if self.disk_manager.read().page_exists(pid) {
            let page = self.disk_manager.read().read_page(pid)?;
            self.frames[fid].load(pid, page);
        } else {
            self.frames[fid].init(pid);
        }
        Ok(())
    }

    fn resolve_or_load(&self, pid: PageId) -> Result<FrameId, BufferPoolError> {
        if let Some(fid) = self.check_cache_hit(pid) {
            return Ok(fid)
        }

        let mut policy = self.eviction_policy.lock();

        if let Some(fid) = self.check_cache_race(pid, &mut policy) {
            return Ok(fid)
        }

        let (fid, was_evicted) = policy
            .acquire_frame()
            .ok_or(BufferPoolError::AllFramesPinned)?;

        drop(policy);

        if was_evicted {
            self.evict_frame(fid)?;
        }

        self.read_or_init_page(pid, fid)?;

        let mut policy = self.eviction_policy.lock();
        self.page_table.insert(pid, fid);
        policy.on_insert(fid);
        drop(policy);

        Ok(fid)
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let _ = self.evict_all();
        let _ = self.command_tx.send(BufferPoolOp::Shutdown);
    }
}
