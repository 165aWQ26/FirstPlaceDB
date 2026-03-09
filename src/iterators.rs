use crate::page::Page;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Default)]
pub struct PhysicalAddress {
    pub(crate) offset: usize,
    pub(crate) collection_num: usize,
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

        PhysicalAddress {
            offset: prev % Page::PAGE_SIZE,
            collection_num: prev / Page::PAGE_SIZE,
        }
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
    pub fn next(&self) -> PidRange {
        let start = self.start.fetch_add(self.pages_per_collection, Ordering::Relaxed);
        let end = start + self.pages_per_collection;

        PidRange { start, end }
    }
}

pub struct AtomicIterator<T> {
    next: T,
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