use crate::page::Page;
use crate::page_collection::PageCollection;
use crate::table::Table;


//TODO: This almost works the only thing I don't do correctly is init/create PageCollection correctly.
// need to properly create it with correct num pages per PageCollection. See page_collection.rs
pub struct PageRange {
    range : Vec<PageCollection>,
    next_addr : PhysicalAddressIterator
}

impl PageRange {

    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize = (Page::PAGE_SIZE / Table::PROJECTED_NUM_RECORDS * 2) / 3;

    //Append assumes metadata has been pre-calculated (allData)
    fn append(&mut self, allData : Vec<Option<i64>>) {
        let addr = self.next_addr.next().unwrap();
        //TODO: IMPORTANT -- Initialize PageCollections pages and have that logic (put it in another function that is called here).
        //Needs to be done lazily

        for (page, data) in self.range[addr.collection_num].iter().zip(allData.iter())  {
            page.write(*data).expect("TODO: panic message");
        }
    }
}

impl Default for PageRange {
    fn default() -> Self {
        PageRange {
            range : Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS),
            next_addr : PhysicalAddressIterator::default(),
        }
    }
}


#[derive(Default)]
pub struct PageRanges {
    tail : PageRange,
    base : PageRange
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