use std::sync::RwLock;
use dashmap::DashMap;
use crate::page::Page;
use crate::page_collection::Pid;
use std::sync::atomic::{AtomicUsize, Ordering};


#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Default)]
pub struct PhysicalAddress {
    pub(crate) offset: usize,
    pub(crate) collection_num: usize,
}
#[derive(Default)]
pub struct PhysicalAddressIterator {
    current: PhysicalAddress,
}

impl Iterator for PhysicalAddressIterator {
    type Item = PhysicalAddress;
    fn next(&mut self) -> Option<Self::Item> {
        let addr = self.current;
        self.current.offset += 1;
        if self.current.offset >= Page::PAGE_SIZE {
            self.current.offset = 0;
            self.current.collection_num += 1;
        }
        Some(addr)
    }
}

#[derive(Default)]
pub struct PidRange {
    pub(crate) start: usize, //inclusive
    pub(crate) end: usize, //exclusive
}
pub struct PidRangeIterator {
    start: usize,
    pages_per_collection: usize,
}

impl PidRangeIterator {
    pub fn new(pages_per_collection: usize) -> Self {
        Self {
            start: 0,
            pages_per_collection,
        }
    }
}

impl Iterator for PidRangeIterator {
    type Item = PidRange;
    fn next(&mut self) -> Option<Self::Item> {

        let end = self.start + self.pages_per_collection;

        let range = PidRange {
            start: self.start,
            end,
        };

        self.start = end;
        Some(range)
    }
}

pub struct BufferPoolFrameMap {
    map: DashMap<Pid, RwLock<Frame>>,
    current: BufferPoolIterator,
    frames: Vec<RwLock<Frame>>,
}

impl BufferPoolFrameMap {

    fn new(frames: Vec<RwLock<Frame>>) -> Self {
        Self {
            frames,
            current: BufferPoolIterator::default(),
            map: DashMap::new(),
        }
    }
    pub fn insert(&self, pid: Pid) {
        self.map.insert(pid, self.frames[self.current.next()]);
    }

    pub fn remove(&self, pid: Pid) {
        self.map.remove(&pid);
    }
}

#[derive(Default)]
pub struct BufferPoolIterator {
    current: AtomicUsize,
}


impl BufferPoolIterator {
    pub fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
        }
    }

    //The current value is the only thing being changed in parallel, we don't care that the modulo
    //is not atomic.
    //AtomicUsize wraps on usize overflow & modulo of a power of 2 is always cheap.
    pub fn next(&self) -> usize {
        self.current.fetch_add(1, Ordering::SeqCst) % //Todo declare this macro in bufferpool
    }
}