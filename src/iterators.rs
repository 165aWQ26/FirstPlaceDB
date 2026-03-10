use crate::page::Page;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

pub struct PhysicalAddress {
    pub(crate) offset: usize,
    pub(crate) collection_num: usize,
    pub(crate) tps: AtomicI64,
}

impl PhysicalAddress {
    pub fn new(offset: usize, collection_num: usize) -> Self {
        Self {
            offset,
            collection_num,
            tps: AtomicI64::new(i64::MIN),
        }
    }
}

impl Clone for PhysicalAddress {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            collection_num: self.collection_num,
            tps: AtomicI64::new(self.tps.load(Ordering::Acquire)),
        }
    }
}

impl PartialEq for PhysicalAddress {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset && self.collection_num == other.collection_num
    }
}

impl Eq for PhysicalAddress {}

impl Hash for PhysicalAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.collection_num.hash(state);
    }
}

impl Default for PhysicalAddress {
    fn default() -> Self {
        Self {
            offset: 0,
            collection_num: 0,
            tps: AtomicI64::new(i64::MIN),
        }
    }
}

impl std::fmt::Debug for PhysicalAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalAddress")
            .field("offset", &self.offset)
            .field("collection_num", &self.collection_num)
            .field("tps", &self.tps.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Default)]
pub struct PhysicalAddressIterator {
    next: AtomicUsize,
}

impl PhysicalAddressIterator {
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> PhysicalAddress {
        let prev = self.next.fetch_add(1, Ordering::Relaxed);

        PhysicalAddress::new(
            prev % Page::PAGE_SIZE,
            prev / Page::PAGE_SIZE,
        )
    }
    pub fn current(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }

    pub fn restore(&self, val: usize) {
        self.next.store(val, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct PidRange {
    pub(crate) start: usize, //inclusive
    pub(crate) end: usize, //exclusive
}

pub struct PidRangeIterator {
    start: AtomicUsize,
    pages_per_collection: usize,
}
impl PidRangeIterator {

    pub fn new(pages_per_collection: usize) -> Self {
        Self {
            start: AtomicUsize::new(0),
            pages_per_collection,
        }
    }

    pub fn current(&self) -> usize {
        self.start.load(Ordering::Relaxed)
    }

    pub fn next(&self) -> PidRange {
        let start = self.start.fetch_add(self.pages_per_collection, Ordering::Relaxed);
        let end = start + self.pages_per_collection;

        PidRange { start, end }
    }

    pub fn restore(start: usize, pages_per_collection: usize) -> Self{
        Self {
            start: AtomicUsize::new(start),
            pages_per_collection,
        }
    }
}

pub struct AtomicIterator<T> {
    pub(crate) next: T,
}

impl AtomicIterator<AtomicUsize> {
    pub fn next(&self) -> usize {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub fn current(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }
    pub fn set(&self, val: usize) {
        self.next.store(val, Ordering::Relaxed);
    }
}

impl AtomicIterator<AtomicI64> {
    pub fn next(&self) -> i64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub fn current(&self) -> i64 {
        self.next.load(Ordering::Relaxed)
    }
    pub fn set(&self, val: i64) {
        self.next.store(val, Ordering::Relaxed);
    }
}

impl Default for AtomicIterator<AtomicUsize> {
    fn default() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }
}

impl Default for AtomicIterator<AtomicI64> {
    fn default() -> Self {
        Self {
            next: AtomicI64::new(0),
        }
    }
}