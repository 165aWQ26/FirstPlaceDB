use crate::eviction_policy::{ArcPolicy, EvictionPolicy};
use crate::page::{Page, PageError};
use crate::page_collection::Pid;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub type FrameId = usize;
pub type DefaultEvictionState = EvictionState<ArcPolicy>;
pub type DefaultBufferPool = BufferPool<ArcPolicy>;

pub struct Frame {
    page: RwLock<Page>,
    dirty: AtomicBool,
    pid: RwLock<Option<Pid>>,
}

impl Frame {
    pub fn empty() -> Self {
        Self {
            page: RwLock::new(Page::default()),
            dirty: AtomicBool::new(false),
            pid: RwLock::new(None),
        }
    }

    pub fn load(&self, pid: Pid, page: Page) {
        *self.pid.write() = Some(pid);
        *self.page.write() = page;
        self.dirty.store(false, Ordering::Release);
    }

    pub fn init(&self, pid: Pid) {
        *self.pid.write() = Some(pid);
        *self.page.write() = Page::default();
        self.dirty.store(false, Ordering::Release);
    }

    pub fn read(&self, offset: usize) -> Result<Option<i64>, PageError> {
        let page = self.page.read();
        page.read(offset)
    }

    pub fn write(&self, value: Option<i64>) -> Result<(), PageError> {
        let mut page = self.page.write();
        page.write(value)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    pub fn update(&self, offset: usize, value: Option<i64>) -> Result<(), PageError> {
        let mut page = self.page.write();
        page.update(offset, value)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    pub fn get_page_copy(&self) -> Page {
        self.page.read().clone()
    }

    pub fn release(&self) {
        *self.pid.write() = None;
        self.dirty.store(false, Ordering::Release);
    }

    pub fn has_capacity(&self) -> bool {
        self.page.read().has_capacity()
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

pub struct BufferPool<P: EvictionPolicy + Send + 'static> {
    page_table: DashMap<Pid, FrameId>,
    frames: Vec<Frame>,
    eviction: Mutex<EvictionState<P>>,
    disk_manager: Arc<DiskManager>,
    // bg_tx: mpsc::SyncSender<BackgroundOp>,
    // _bg_thread: thread::JoinHandle<()>,
}

impl<P: EvictionPolicy + Send + 'static> BufferPool<P> {
    pub fn new(capacity: usize, disk_manager: DiskManager) -> DefaultBufferPool {
        Self {
            page_table: DashMap::new(),
            frames: Vec::with_capacity(capacity),
            eviction: Mutex::new(EvictionState::new()),
            disk_manager,
        }
    }
    pub fn read(&self, pid: Pid, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].read(offset)?)
    }

    pub fn write(&self, pid: Pid, val: Option<i64>) -> Result<(), BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].write(val)?)
    }

    pub fn update(&self, pid: Pid, offset: usize, val: Option<i64>) -> Result<(), BufferPoolError> {
        let fid = self.resolve_or_load(pid)?;
        Ok(self.frames[fid].update(offset, val)?)
    }

    // pub fn flush_async(&self, pids: Vec<Pid>) -> Result<(), BufferPoolError> {
    //
    // }
    // pub fn flush_page(&self, pid: Pid) -> Result<(), BufferPoolError> {
    //
    // }

    fn flush_frame(&mut self, pid: Pid, fid: FrameId) -> Result<(), BufferPoolError> {
        if self.frames[fid].is_dirty() {
            let page = self.frames[fid].get_page_copy();
            self.disk_manager.write_page(pid, &page)?;
            self.frames[fid].clear_dirty();
        }
        Ok(())
    }

    fn resolve_or_load(&self, pid: Pid) -> Result<FrameId, BufferPoolError> {
        // Case 1: cache hit
        if let Some(entry) = self.page_table.get(&pid) {
            let fid = *entry;
            self.eviction.lock().policy.on_access(pid);
            return Ok(fid);
        }

        let mut eviction = self.eviction.lock();

        if let Some(entry) = self.page_table.get(&pid) {
            eviction.policy.on_access(pid);
            return Ok(*entry);
        }

        let fid = if let Some(free) = eviction.free_list.pop() {
            free
        } else {
            let victim = eviction
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
                    .write_page(victim, &self.frames[victim_fid])?;
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
        eviction.policy.on_insert(pid);

        Ok(fid)
    }
}

// impl<P: EvictionPolicy + Send + 'static> Drop for BufferPool<P> {
//     fn drop(&mut self) {
//         let _ = self.bg_tx.send(BackgroundOp::Shutdown);
//     }
// }

struct EvictionState<P: EvictionPolicy> {
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
