use std::sync::mpsc::Sender;
use std::thread::current;
use crate::error::DbError;
use crate::iterators::{PhysicalAddress, PhysicalAddressIterator, PidRange, PidRangeIterator};
use crate::page::{Page, PageError};
use crate::page_collection::{MetaPage, PageCollection, Pid};
use crate::table::Table;

pub struct PageRange {
    range: Vec<PageCollection>,
    next_addr: PhysicalAddressIterator,
    pages_per_collection: usize,
    table_id: usize,
    buffer_pool_req_sender: Sender<Pid>
}

impl PageRange {
    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize =
        (Table::PROJECTED_NUM_RECORDS + Page::PAGE_SIZE - 1) / Page::PAGE_SIZE;

    pub fn new(pages_per_collection: usize, first_pid: PidRange, table_id: usize, buffer_pool_req_sender: Sender<Pid>) -> Self {
        let mut init_range: Vec<PageCollection> = Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        init_range.push(PageCollection::new(first_pid, table_id, buffer_pool_req_sender.clone()));

        Self {
            range: init_range,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
            table_id,
            buffer_pool_req_sender
        }
    }

    //Append assumes metadata has been pre-calculated (allData)
    //All it does is write to the current offset
    //allData cols must be in correct places
    //Idk how to format closures
    fn append<F>(&mut self, all_data: Vec<Option<i64>>, pid_alloc_closure: &mut F) -> Result<PhysicalAddress, DbError>
        where F: FnMut() -> PidRange,
    {
        //get next addr
        let addr = self.next_addr.next().unwrap();

        //Lazily create page collection and associated pages
        self.lazy_create_page_collection(addr.collection_num, pid_alloc_closure);

        let collection = &mut self.range[addr.collection_num];
        for (i, data) in all_data.iter().enumerate() {
            collection.write_col(i, *data)?;
        }

        Ok(addr) //return addr (from here add this addr to a page_dir)
        //Note that you should deal with RID elsewhere (imo) --> isn't a PageRange Construct.
        //By this point it will have been generated and be in data.
    }

    //iterators make this so cleannnnn
    fn lazy_create_page_collection<F>(&mut self, page: usize, alloc_pid: &mut F)
    where
        F: FnMut() -> PidRange,
    {
        while self.range.len() <= page {
            let next_pid = alloc_pid();
            self.range
                .push(PageCollection::new(next_pid, self.table_id, self.buffer_pool_req_sender.clone()));
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
    pub fn write_meta_col(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col: MetaPage,
    ) -> Result<(), PageError> {
        self.range[addr.collection_num].update_meta_col(addr.offset, val, col)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col_type : MetaPage) -> Result<Option<i64>, PageError>{
        Ok(self.range[addr.collection_num].read_meta_col(addr.offset, col_type)?)
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
    pid_iter: PidRangeIterator,
}

impl PageRanges {
    pub fn new(pages_per_collection: usize, table_id: usize, buffer_pool_req_sender: Sender<Pid>) -> Self {
        let mut PidRangeIterator = PidRangeIterator::new(pages_per_collection);
        Self {
            tail: PageRange::new(pages_per_collection, PidRangeIterator.next().unwrap(), table_id, buffer_pool_req_sender.clone()),
            base: PageRange::new(pages_per_collection, PidRangeIterator.next().unwrap(), table_id, buffer_pool_req_sender.clone()),
            pid_iter: PidRangeIterator,
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
        let mut alloc_pid = || self.pid_iter.next().unwrap();
        self.base.append(data_cols, &mut alloc_pid)
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
        let mut alloc_pid = || self.pid_iter.next().unwrap();
        self.tail.append(data_cols, &mut alloc_pid)
    }

    #[inline]
    pub fn read_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
        range: WhichRange
    ) -> Result<Option<i64>, DbError> {
        match range {
            WhichRange::Base => self.base.read_single(column, addr),
            WhichRange::Tail => self.tail.read_single(column, addr),
        }
    }

    #[inline]
    pub fn read_tail_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, DbError> {
        self.tail.read_single(column, addr)
    }

    #[inline]
    pub fn write_indirection(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        range: WhichRange
    ) -> Result<(), PageError> {
        match range {
            WhichRange::Base => self.base.write_meta_col(addr, val, MetaPage::IndirectionCol),
            WhichRange::Tail => self.tail.write_meta_col(addr, val, MetaPage::IndirectionCol),
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

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col_type : MetaPage, range: WhichRange) -> Result<Option<i64>, PageError>{
        match range {
            WhichRange::Base => self.base.read_meta_col(addr, col_type),
            WhichRange::Tail => self.tail.read_meta_col(addr, col_type),
        }
    }
}

