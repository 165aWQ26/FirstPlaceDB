use std::sync::Arc;
use std::usize;
use parking_lot::RwLock;
use crate::error::DbError;
use crate::page::{Page, PageError};
use crate::bufferpool::{BufferPool, MetaPage};
use crate::table::Table;


pub struct PageRange {
    bufferpool: Arc<RwLock<BufferPool>>,
    next_addr: PhysicalAddressIterator,
    pages_per_collection: usize,
}

impl PageRange{
    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub fn new(data_pages_per_collection: usize, bufferpool: Arc<RwLock<BufferPool>>) -> Self {
        let pages_per_collection = data_pages_per_collection + Table::NUM_META_PAGES;
        // let mut init_range: Vec<PageCollection> =
        //     Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        // init_range.push(PageCollection::new(pages_per_collection));

        Self{
            // range: init_range,
            //initialize reference for bufferpool
            bufferpool,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
        }
    }

    //Append assumes metadata has been pre-calculated (allData)
    //All it does is write to the current offset
    //allData cols must be in correct places
    pub fn append(&mut self, all_data: Vec<Option<i64>>, range: WhichRange) -> Result<PhysicalAddress, DbError> {
        //get next addr
        let addr = self.next_addr.next().unwrap();

        // PageRange management of collections is handed over to bufferpool 
        self.bufferpool.write().append(all_data, &addr, range)?;


        //Lazily create page collection and associated pages

        

        // self.lazy_create_page_collection(addr.collection_num);

        // let collection = &mut self.range[addr.collection_num];
        // for (i, data) in all_data.iter().enumerate() {
        //     collection.write_col(i, *data)?;
        // }


        Ok(addr) //return addr (from here add this addr to a page_dir)
    }

    // Will be maintained in the bufferpool rather than pagerange
    // iterators make this so cleannnnn
    // fn lazy_create_page_collection(&mut self, page: usize) {
    //     while self.range.len() <= page {
    //         self.range
    //             .push(PageCollection::new(self.pages_per_collection));
    //     }
    // }

    fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        // given an array (project_columns) of 0's and 1's, return all requested columns (1's), ignore non-required(0's)
        let num_data_cols = self.pages_per_collection - Table::NUM_META_PAGES;
        (0..num_data_cols)
            .map(|col| self.read_single(col, addr, WhichRange::Base))
            .collect()
    }

    #[inline]
    fn read_single(&self, column: usize, addr: &PhysicalAddress, range: WhichRange) -> Result<Option<i64>, DbError> {
        //given single column, return value in row x column
        Ok(self.bufferpool.write().read_col(column, *addr, range)?)
    }

    #[inline]
    pub fn write_meta_col(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col: MetaPage,
        range: WhichRange,
    ) -> Result<(), PageError> {
        self.bufferpool.write().update_meta_col(addr, val, col,range)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col_type : MetaPage, range: WhichRange) -> Result<Option<i64>, PageError>{
        Ok(self.bufferpool.write().read_meta_col(addr, col_type, range)?)
    }

    // todo: is this needed
    // fn read_projected(
    //     &self,
    //     projected: &[i64],
    //     addr: &PhysicalAddress,
    // ) -> Result<Vec<Option<i64>>, DbError> {
    //     projected
    //         .iter()
    //         .enumerate()
    //         .map(|(col, &flag)| {
    //             if flag == 1 {
    //                 self.read_single(col, addr)
    //             } else {
    //                 Ok(None)
    //             }
    //         })
    //         .collect()
    // }
}

pub enum WhichRange {
    Base,
    Tail,
}

pub struct PageRanges{
    tail: PageRange,
    base: PageRange,
}

impl PageRanges{
    pub fn new(pages_per_collection: usize,bufferpool: Arc<RwLock<BufferPool>>) -> Self {
        Self {
            tail: PageRange::new(pages_per_collection,Arc::clone(&bufferpool)),
            base: PageRange::new(pages_per_collection,Arc::clone(&bufferpool)),
        }
    }

    // For inserts: stages metadata (rid, indirection=rid, schema=0) then appends to base
    pub fn append_base(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
    ) -> Result<PhysicalAddress, DbError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(rid)); // indirection (self for new base record)
        data_cols.push(Some(0)); // schema_encoding (no updates)
        data_cols.push(None);
        self.base.append(data_cols, WhichRange::Base)
    }

    // For updates: caller provides indirection (previous version) and schema_encoding (which cols updated)
    pub fn append_tail(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, DbError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(indirection)); // indirection (points to prev version)
        data_cols.push(schema_encoding); // schema_encoding: None = deletion, Some(bitmask) = update
        data_cols.push(None);
        self.tail.append(data_cols, WhichRange::Tail)
    }

    #[inline]
    pub fn read_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
        range: WhichRange
    ) -> Result<Option<i64>, DbError> {
        match range {
            WhichRange::Base => self.base.read_single(column, addr,range),
            WhichRange::Tail => self.tail.read_single(column, addr,range),
        }
    }

    #[inline]
    pub fn read_tail_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.tail.read_single(column, addr, WhichRange::Tail)
    }

    //
    // #[inline]
    // pub fn write_single(
    //     &mut self,
    //     col: usize,
    //     addr: &PhysicalAddress,
    //     val: Option<i64>,
    // ) -> Result<(), PageError> {
    //     self.base.write_meta_col(col, addr, val)
    // }

    #[inline]
    pub fn write_indirection(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        range: WhichRange
    ) -> Result<(), PageError> {
        match range {
            WhichRange::Base => self.base.write_meta_col(addr, val, MetaPage::IndirectionCol,WhichRange::Base),
            WhichRange::Tail => self.tail.write_meta_col(addr, val, MetaPage::IndirectionCol,WhichRange::Tail),
        }
    }


    #[inline]
    pub fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        self.base.read(addr)
    }

    // #[inline]
    // pub fn read_projected(
    //     &self,
    //     projected: &[i64],
    //     addr: &PhysicalAddress,
    // ) -> Result<Vec<Option<i64>>, DbError> {
    //     self.base.read_projected(projected, addr)
    // }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col_type : MetaPage, range: WhichRange) -> Result<Option<i64>, PageError>{
        match range {
            WhichRange::Base => self.base.read_meta_col(addr, col_type, range),
            WhichRange::Tail => self.tail.read_meta_col(addr, col_type, range),
        }
    }
}

//Possibly put here & below into its own file
//This iterator automatically manages where you write to.
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
