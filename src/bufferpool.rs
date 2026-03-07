use crate::disk_manager::{DiskError, DiskManager};
use crate::eviction_policy::{ArcPolicy, EvictionPolicy};
use crate::page::{Page, PageError};
use crate::page_collection::PageId;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

pub type FrameId = usize;
pub type DefaultEvictionState = EvictionState<ArcPolicy>;
pub type DefaultBufferPool = BufferPool<ArcPolicy>;
pub const BP_CAP: usize = 32;

struct InnerFrame {
    page: Page,
    pid: Option<PageId>
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
    inner: RwLock<InnerFrame>
}

impl Frame {
    pub fn new() -> Self {
        Self {
            //todo there should be one lock on page and pid
            dirty: AtomicBool::new(false),
            inner: RwLock::new(InnerFrame {
                page: Default::default(),
                pid: None
            })
        }
    }

    pub fn load(&self, pid: Pid, page: Page) {
        let mut guard = self.inner.write();
        guard.page = page;
        guard.pid = Some(pid);
        self.dirty.store(false, Ordering::Release);
    }

    pub fn init(&self, pid: Pid) {
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
        let guard = self.inner.write();
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

    // pub fn pid(&self) -> Option<Pid> {
    //     *self.pid.read()
    // }
}


pub struct BufferPool {
    page_table: DashMap<Pid, FrameId>,
    frames: Vec<Frame>,
    eviction: Mutex<EvictionPolicy>,
    disk_manager: Arc<DiskManager>,
    bg_tx: mpsc::SyncSender<BufferPoolOp>,
    _bg_thread: thread::JoinHandle<()>,
}

impl BufferPool {
    pub fn new(disk_manager: DiskManager) -> BufferPool {
        Self {
            page_table: DashMap::new(),
            frames: (0..BP_CAP).map(|_| Frame::new()).collect(),
            eviction: Mutex::new(EvictionState::new()),
            disk_manager: Arc::new(disk_manager),
            bg_tx: (),
            _bg_thread: (),
        }
    }

    //todo: when writing close table later, consider the race condition where a write occurs after
    //this function scans the bp. Need to hold write locks on everything before proceeding and reject
    //essentially this function on its own does not guarentee a fully flushed bp.
    pub fn evict_all(&self) -> Result<(), BufferPoolError> {
        let dirty_pids = self
            .page_table
            .iter()
            .filter_map(|entry| {
                let pid = *entry.key();
                let fid = *entry.value();
                if self.frames[fid].is_dirty() {
                    Some(pid)
                } else {
                    None
                }
            })
            .collect();
        self.flush_pages(dirty_pids)?;

        let mut ev = self.eviction.lock();
        let pids: Vec<Pid> = self.page_table.iter().map(|e| *e.key()).collect();
        for pid in pids {
            if let Some((_, fid)) = self.page_table.remove(&pid) {
                self.frames[fid].release();
                ev.free_list.push(fid);
            }
        }

        Ok(())
    }
    pub fn read(&self, pid: PageId, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].read(offset)?)
    }

    pub fn write(&self, pid: PageId, val: Option<i64>, offset: usize) -> Result<(), BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].write(val, offset)?)
    }

    pub fn update(&self, pid: PageId, offset: usize, val: Option<i64>) -> Result<(), BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].update(offset, val)?)
    }

    pub fn flush_page(&self, pid: PageId) -> Result<(), BufferPoolError> {
        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            self.flush_frame(pid, fid)?;
        }
        Ok(())
    }

    //Todo: You are not giving the worker a way to return any result.
    //A pattern I used was sending a different tx channel to the worker and keeping the rx.
    //So two channels: one for bp (tx) --> worker (rx) passed on construction and one for worker (tx) -> bp (rx) passed
    // on function call.
    //There may be a better way to do this.
    pub fn flush_pages(&self, pids: Vec<PageId>) -> Result<(), BufferPoolError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.bg_tx
            .send(BufferPoolOp::FlushDirty {
                pids,
                response: None,
            })
            .map_err(|_| BufferPoolError::BackgroundWorkerDead);
        rx.recv().map_err(|_| BufferPoolError::BackgroundWorkerDead)
    }

    //Todo: You are not giving the worker a way to return any result.
    //A pattern I used was sending a different tx channel to the worker and keeping the rx.
    //So two channels: one for bp (tx) --> worker (rx) passed on construction and one for worker (tx) -> bp (rx) passed
    // on function call.
    //There may be a better way to do this.
    pub fn flush_async(&self, pids: Vec<PageId>) -> Result<(), BufferPoolError> {
        self.bg_tx
            .send(BufferPoolOp::FlushDirty {
                pids,
                response: None,
            })
            .map_err(|_| BufferPoolError::BackgroundWorkerDead)
    }

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
            //todo: eviction lock should be held here otherwise race condition.
            let fid = *entry;
            self.eviction.lock().policy.on_access(pid);
            return Ok(fid);
        }
        
        let mut eviction_lock = self.eviction.lock();
        
        if let Some(entry) = self.page_table.get(&pid) {
            eviction_lock.policy.on_access(pid);
            return Ok(*entry);
        }
        
        let fid = if let Some(free) = eviction_lock.free_list.pop() {
            free
        } else {
            let victim = eviction_lock
                .policy
                .on_insert(pid)
                .ok_or(BufferPoolError::AllFramesPinned)?;
            let victim_fid = self
                .page_table
                .remove(&victim)
                .ok_or(BufferPoolError::PidNotInFrame)?
                .1;
            if self.frames[victim_fid].is_dirty() {
                self.disk_manager
                    .write_page(victim, &self.frames[victim_fid].page.write())?;
                self.frames[victim_fid].clear_dirty();
            }
            self.frames[victim_fid].release();
            victim_fid
        };

        if self.disk_manager.page_exists(pid) {
            let page = self.disk_manager.read_page(pid)?;
            self.frames[fid].load(pid, page);
        } else {
            self.frames[fid].init(pid);
        }

        self.page_table.insert(pid, fid);
        eviction_lock.policy.on_insert(pid);

        Ok(fid)
    }
}

// impl<P: EvictionPolicy + Send + 'static> Drop for BufferPool<P> {
//     fn drop(&mut self) {
//         let _ = self.bg_tx.send(BufferPoolOp::Shutdown);
//     }
// }

pub(crate) struct EvictionState<P: EvictionPolicy> {
    policy: P,
    free_list: Vec<FrameId>,
}

impl<P: EvictionPolicy + Send + 'static> BufferPool<P> {}

#[derive(Debug)]
pub enum BufferPoolError {
    PageNotFound,

    NoVictim,

    Disk(DiskError),

    Page(PageError),

    BackgroundWorkerDead,

    AllFramesPinned,

    PidNotInFrame,
}

impl std::fmt::Display for BufferPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferPoolError::PageNotFound => write!(f, "Page not found in buffer pool"),
            BufferPoolError::NoVictim => write!(f, "No victim available for eviction"),
            BufferPoolError::Disk(e) => write!(f, "Disk error: {}", e),
            BufferPoolError::Page(e) => write!(f, "Page error: {:?}", e),
            BufferPoolError::BackgroundWorkerDead => write!(f, "Background worker thread has died"),
            BufferPoolError::AllFramesPinned => write!(f, "Every frame is pinned"),
            BufferPoolError::PidNotInFrame => write!(f, "Pid "),
        }
    }
}

impl From<PageError> for BufferPoolError {
    fn from(e: PageError) -> Self {
        BufferPoolError::Page(e)
    }
}

impl From<DiskError> for BufferPoolError {
    fn from(e: DiskError) -> Self {
        BufferPoolError::Disk(e)
    }
}
