use std::fs::Metadata;
use crate::error::DbError;
use crate::page::{Page, PageError};
use crate::page_collection::{MetaPage, PageCollection};
use crate::table::Table;

pub struct PageRange {
    range: Vec<PageCollection>,
    next_addr: PhysicalAddressIterator,
    pages_per_collection: usize,
}

impl PageRange {
    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize =
        (Table::PROJECTED_NUM_RECORDS + Page::PAGE_SIZE - 1) / Page::PAGE_SIZE;

    pub fn new(data_pages_per_collection: usize) -> Self {
        let pages_per_collection = data_pages_per_collection + Table::NUM_META_PAGES;
        let mut init_range: Vec<PageCollection> =
            Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        init_range.push(PageCollection::new(pages_per_collection));

        Self {
            range: init_range,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
        }
    }

    //Append assumes metadata has been pre-calculated (allData)
    //All it does is write to the current offset
    //allData cols must be in correct places
    pub fn append(&mut self, all_data: Vec<Option<i64>>) -> Result<PhysicalAddress, DbError> {
        //get next addr
        let addr = self.next_addr.next().unwrap();

        //Lazily create page collection and associated pages
        self.lazy_create_page_collection(addr.collection_num);

        let collection = &mut self.range[addr.collection_num];
        for (i, data) in all_data.iter().enumerate() {
            collection.write_col(i, *data)?;
        }

        Ok(addr) //return addr (from here add this addr to a page_dir)
        //Note that you should deal with RID elsewhere (imo) --> isn't a PageRange Construct.
        //By this point it will have been generated and be in data.
    }

    //iterators make this so cleannnnn
    fn lazy_create_page_collection(&mut self, page: usize) {
        while self.range.len() <= page {
            self.range
                .push(PageCollection::new(self.pages_per_collection));
        }
    }

    fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        // given an array (project_columns) of 0's and 1's, return all requested columns (1's), ignore non-required(0's)
        let num_data_cols = self.pages_per_collection - Table::NUM_META_PAGES;
        (0..num_data_cols)
            .map(|col| self.read_single(col, addr))
            .collect()
    }

    #[inline]
    fn read_single(&self, column: usize, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        //given single column, return value in row x column
        Ok(self.range[addr.collection_num].read_col(column, addr.offset)?)
    }

    #[inline]
    pub fn write_single_meta_col(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col: MetaPage,
    ) -> Result<(), PageError> {
        self.range[addr.collection_num].update_meta_col(addr.offset, val, col)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, colType : MetaPage) -> Result<Option<i64>, PageError>{
        Ok(self.range[addr.collection_num].read_meta_col(addr.offset, colType)?)
    }

    fn read_projected(
        &self,
        projected: &[i64],
        addr: &PhysicalAddress,
    ) -> Result<Vec<Option<i64>>, DbError> {
        projected
            .iter()
            .enumerate()
            .map(|(col, &flag)| {
                if flag == 1 {
                    self.read_single(col, addr)
                } else {
                    Ok(None)
                }
            })
            .collect()
    }
}

pub enum WhichRange {
    Base,
    Tail,
}

pub struct PageRanges {
    tail: PageRange,
    base: PageRange,
}

impl PageRanges {
    pub fn new(pages_per_collection: usize) -> Self {
        Self {
            tail: PageRange::new(pages_per_collection),
            base: PageRange::new(pages_per_collection),
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
        self.base.append(data_cols)
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
        self.tail.append(data_cols)
    }

    #[inline]
    pub fn read_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.base.read_single(column, addr)
    }

    #[inline]
    pub fn read_tail_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.tail.read_single(column, addr)
    }

    //
    // #[inline]
    // pub fn write_single(
    //     &mut self,
    //     col: usize,
    //     addr: &PhysicalAddress,
    //     val: Option<i64>,
    // ) -> Result<(), PageError> {
    //     self.base.write_single_meta_col(col, addr, val)
    // }

    #[inline]
    pub fn write_indirection(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        range: WhichRange
    ) -> Result<(), PageError> {
        match range {
            WhichRange::Base => self.base.write_single_meta_col(addr, val, MetaPage::INDIRECTION_COL),
            WhichRange::Tail => self.tail.write_single_meta_col(addr, val, MetaPage::INDIRECTION_COL),
        }
    }


    #[inline]
    pub fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        self.base.read(addr)
    }

    #[inline]
    pub fn read_projected(
        &self,
        projected: &[i64],
        addr: &PhysicalAddress,
    ) -> Result<Vec<Option<i64>>, DbError> {
        self.base.read_projected(projected, addr)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, colType : MetaPage, range: WhichRange) -> Result<Option<i64>, PageError>{
        match range {
            WhichRange::Base => self.base.read_meta_col(addr, colType),
            WhichRange::Tail => self.tail.read_meta_col(addr, colType),
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
