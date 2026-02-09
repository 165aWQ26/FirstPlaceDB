use crate::error::DbError;
use crate::page::{Page, PageError};
use crate::page_collection::PageCollection;
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
        (Page::PAGE_SIZE / Table::PROJECTED_NUM_RECORDS * 2) / 3;

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

        //iterate over page and data
        for (page, data) in self.range[addr.collection_num].iter().zip(all_data.iter()) {
            page.write(*data)?;
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

    fn read_single(&self, column: usize, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        //given single column, return value in row x column
        Ok(self.range[addr.collection_num].read_column(column, addr.offset)?)
    }

    pub fn write_single(
        &mut self,
        col: usize,
        addr: &PhysicalAddress,
        val: Option<i64>,
    ) -> Result<(), PageError> {
        self.range[addr.collection_num].update_column(col, addr.offset, val)
    }

    pub fn get_rid(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        Ok(self.range[addr.collection_num].get_rid(addr.offset)?)
    }

    pub fn get_indirection(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        Ok(self.range[addr.collection_num].get_indirection(addr.offset)?)
    }
    pub fn get_schema_encoding(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        Ok(self.range[addr.collection_num].get_schema_encoding(addr.offset)?)
    }

    pub fn get_start_time(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        Ok(self.range[addr.collection_num].get_start_time(addr.offset)?)
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

pub struct PageRanges {
    pub tail: PageRange,
    pub base: PageRange,
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

    pub fn read_tail(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        self.tail.read(addr)
    }

    pub fn read_tail_single(
        &self,
        col: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.tail.read_single(col, addr)
    }

    pub fn get_tail_indirection(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.tail.get_indirection(addr)
    }
    pub fn get_tail_schema_encoding(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.tail.get_schema_encoding(addr)
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

    pub fn read_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.base.read_single(column, addr)
    }

    pub fn write_single(
        &mut self,
        col: usize,
        addr: &PhysicalAddress,
        val: Option<i64>,
    ) -> Result<(), PageError> {
        self.base.write_single(col, addr, val)
    }

    pub fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, DbError> {
        self.base.read(addr)
    }

    pub fn read_projected(
        &self,
        projected: &[i64],
        addr: &PhysicalAddress,
    ) -> Result<Vec<Option<i64>>, DbError> {
        self.base.read_projected(projected, addr)
    }

    pub fn get_rid(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.base.get_rid(addr)
    }

    pub fn get_indirection(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.base.get_indirection(addr)
    }

    pub fn get_schema_encoding(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.base.get_schema_encoding(addr)
    }

    pub fn get_start_time(&self, addr: &PhysicalAddress) -> Result<Option<i64>, DbError> {
        self.base.get_start_time(addr)
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
