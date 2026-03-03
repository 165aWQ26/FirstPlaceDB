use crate::page::Page;

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
    start: usize, //inclusive
    end: usize, //exclusive
}
pub struct PidRangeIterator {
    current: PidRange,
    pages_per_collection: usize,
}

impl PidRangeIterator {
    pub fn new(pages_per_collection: usize) -> Self {
        Self {
            current: PidRange::default(),
            pages_per_collection,
        }
    }
}

impl Iterator for PidRangeIterator {
    type Item = PidRange;
    fn next(&mut self) -> Option<Self::Item> {

        let end = self.current.start + self.pages_per_collection;

        let range = PidRange {
            start: self.current.start,
            end,
        };

        self.current.start = end;
        Some(range)
    }
}