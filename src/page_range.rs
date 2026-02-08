use crate::page::Page;
use crate::page_collection::PageCollection;
use crate::table::Table;


pub struct PageRange {
    range : Vec<PageCollection>,
    next_addr : PhysicalAddressIterator,
    pages_per_collection : usize
}

impl PageRange {

    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize = (Page::PAGE_SIZE / Table::PROJECTED_NUM_RECORDS * 2) / 3;

    pub fn new(data_pages_per_collection: usize) -> Self {
        let pages_per_collection = data_pages_per_collection + Table::NUM_META_PAGES;
        let mut initRange : Vec<PageCollection> = Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        initRange.push(PageCollection::new(pages_per_collection));

        Self {
            range : initRange,
            next_addr : PhysicalAddressIterator::default(),
            pages_per_collection
        }
    }

    //Append assumes metadata has been pre-calculated (allData)
    //All it does is write to the current offset
    //allData cols must be in correct places
    pub fn append(&mut self, allData : Vec<Option<i64>>) -> PhysicalAddress {
        //get next addr
        let addr = self.next_addr.next().unwrap();

        //Lazily create page collection and associated pages
        self.lazy_create_page_collection(addr.collection_num);

        //iterate over page and data
        for (page, data) in self.range[addr.collection_num].iter().zip(allData.iter())  {
            page.write(*data).expect("TODO: panic message");
        }

        addr //return addr (from here add this addr to a page_dir)
            //Note that you should deal with RID elsewhere (imo) --> isn't a PageRange Construct.
            //By this point it will have been generated and be in data.
    }

    //iterators make this so cleannnnn
    fn lazy_create_page_collection(&mut self, page : usize) {
        while self.range.len() <= page {
            self.range.push(PageCollection::new(self.pages_per_collection));
        }
    }
}


pub struct PageRanges {
    tail : PageRange,
    base : PageRange
}

impl PageRanges {
    pub fn new(pages_per_collection: usize) -> Self {
        Self {
            tail : PageRange::new(pages_per_collection),
            base : PageRange::new(pages_per_collection)
        }
    }
}

//Possibly put here & below into its own file
//This iterator automatically manages where you write to.
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