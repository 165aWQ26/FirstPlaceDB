use crate::page::Page;
use std::sync::atomic::{AtomicUsize, Ordering};
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
        let start = self.start.fetch_add(self.pages_per_collection, Ordering::SeqCst);
        let end = start + self.pages_per_collection;

        PidRange { start, end }
    }
}