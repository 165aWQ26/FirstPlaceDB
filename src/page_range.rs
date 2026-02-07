use crate::page::Page;
use crate::page_collection::PageCollection;
use crate::table::Table;


pub struct PageRange {
    range : Vec<PageCollection>,
    next_addr : PhysicalAddressIterator,
    num_pages : usize

}

impl PageRange {

    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize = (Page::PAGE_SIZE / Table::PROJECTED_NUM_RECORDS * 2) / 3;

    pub fn new(num_pages: usize) -> Self {
        let mut initRange : Vec<PageCollection> = Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        initRange.push(PageCollection::new(num_pages));

        Self {
            range : initRange,
            next_addr : PhysicalAddressIterator::default(),
            num_pages
        }
    }

    //Append assumes metadata has been pre-calculated (allData)
    //All it does is write to the current offset
    pub fn append(&mut self, allData : Vec<Option<i64>>) -> PhysicalAddress {
        let addr = self.next_addr.next().unwrap();
        self.lazy_create_page_collection(addr.collection_num);

        for (page, data) in self.range[addr.collection_num].iter().zip(allData.iter())  {
            page.write(*data).expect("TODO: panic message");
        }
        addr //return
    }

    fn lazy_create_page_collection(&mut self, page : usize) {
        while self.range.len() <= page {
            self.range.push(PageCollection::new(self.num_pages));
        }
    }
}


pub struct PageRanges {
    tail : PageRange,
    base : PageRange
}

impl PageRanges {
    pub fn new(num_pages: usize) -> Self {
        Self {
            tail : PageRange::new(num_pages),
            base : PageRange::new(num_pages)
        }
    }
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Default)]
pub struct PhysicalAddress {
    offset : usize,
    collection_num : usize
}

#[derive(Default)]
pub struct PhysicalAddressIterator {
    current: PhysicalAddress,
}

impl Iterator for PhysicalAddressIterator {
    type Item = PhysicalAddress;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current.offset < Page::PAGE_SIZE {
            let addr = self.current;
            self.current.offset += 1;
            Some(addr)
        }
        else {
            self.current.offset = 0;
            self.current.collection_num += 1;
            Some(self.current)
        }
    }
}