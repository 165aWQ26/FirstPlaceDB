use std::sync::Arc;
use crate::iterators::{PhysicalAddress, PhysicalAddressIterator, PidRange, PidRangeIterator};
use crate::page::{Page};
use crate::page_collection::{MetaPage, PageCollection};
use crate::table::Table;
use crate::bufferpool::{BufferPool, BufferPoolError};


pub struct PageRange {
    range: Vec<PageCollection>,
    next_addr: PhysicalAddressIterator,
    pages_per_collection: usize,
    table_id: usize,
    bufferpool: Arc<BufferPool>,
    pid_iterator: Arc<PidRangeIterator>,
    bp_lookup_map: Arc<BufferPoolFrameMap>,
    pub tps: Vec<i64>,
    pub pages_since_merge: usize,
}

impl PageRange {
    //Assumes equal base page and tail page num collections which is suboptimal. Better to over alloc
    //These optimizations are more for fun than anything.
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize =
        (Table::PROJECTED_NUM_RECORDS + Page::PAGE_SIZE - 1) / Page::PAGE_SIZE;

    pub fn new(pages_per_collection: usize, first_pid: PidRange, table_id: usize, bufferpool: Arc<BufferPool>, pid_iterator: Arc<PidRangeIterator>) -> Self {
        let mut init_range: Vec<PageCollection> = Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        init_range.push(PageCollection::new(first_pid, table_id, bufferpool.clone()));

        Self {
            range: init_range,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
            table_id,
            bufferpool,
            pid_iterator,
            bp_lookup_map,
            tps: Vec::new(),
            pages_since_merge: 0,
        }
    }

    fn append (&mut self, all_data: Vec<Option<i64>>) -> Result<PhysicalAddress, BufferPoolError> {
        //get next addr
        let addr = self.next_addr.next();

        //Lazily create page collection and associated pages
        self.lazy_create_page_collection(addr.collection_num);

        self.range[addr.collection_num].write_cols(addr.offset, all_data)?;

        Ok(addr) //return addr (from here add this addr to a page_dir)
    }

    //iterators make this so cleannnnn
    fn lazy_create_page_collection(&mut self, page: usize) {
        while self.range.len() <= page {
            self.range
                .push(PageCollection::new(self.pid_iterator.next(), self.table_id, self.bufferpool.clone()));
        }
    }

    fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, BufferPoolError> {
        self.range[addr.collection_num].read_all(addr.offset)
    }

    #[inline]
    fn read_single(&self, col: usize, addr: &PhysicalAddress) -> Result<Option<i64>, BufferPoolError> {
        //given single column, return value in row x column
        Ok(self.range[addr.collection_num].read_col(col, addr.offset)?)
    }

    #[inline]
    pub fn write_meta_col(
        &self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col: MetaPage,
    ) -> Result<(), BufferPoolError> {
        self.range[addr.collection_num].update_meta_col(col, addr.offset, val)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col : MetaPage) -> Result<Option<i64>, BufferPoolError>{
        Ok(self.range[addr.collection_num].read_meta_col(col, addr.offset)?)
    }

    fn read_projected(
        &self,
        projected: &[i64],
        addr: &PhysicalAddress,
    ) -> Result<Vec<Option<i64>>, BufferPoolError> {
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
    pub fn new(pages_per_collection: usize, table_id: usize, bufferpool: Arc<BufferPool>) -> Self {
        let pid_range_iter = Arc::new(PidRangeIterator::new(pages_per_collection));
        Self {
            tail: PageRange::new(pages_per_collection, pid_range_iter.next(), table_id, bufferpool.clone(), pid_range_iter.clone()),
            base: PageRange::new(pages_per_collection, pid_range_iter.next(), table_id, bufferpool, pid_range_iter),
        }
    }

    // For inserts: stages metadata (rid, indirection=rid, schema=0) then appends to base
    pub fn append_base(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(rid)); // indirection (self for new base record)
        data_cols.push(Some(0)); // schema_encoding (no updates)
        data_cols.push(None);
        self.base.append(data_cols)
    }

    // mirror of append_base --> caller instead supplies indir and schema_encoding
    // for use in table.merge() to write base record w/o fucking wit indir.
    pub fn append_merged_base(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, DbError> {
        data_cols.push(Some(rid));          // RID
        data_cols.push(Some(indirection));  // indirection --> preserved from before merge
        data_cols.push(schema_encoding);    // Some(0) for live record, None for deleted
        data_cols.push(None);               // start time [unused for now ig]
        //let mut alloc_pid = || self.pid_iter.next().unwrap(); --> should we be unwrapping here??
        let mut alloc_pid = || self.pid_iter.next();
        self.base.append(data_cols, &mut alloc_pid)
    }

    // For updates: caller provides indirection (previous version) and schema_encoding (which cols updated)
    pub fn append_tail(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, BufferPoolError> {
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
        range: WhichRange
    ) -> Result<Option<i64>, BufferPoolError> {
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
    ) -> Result<Option<i64>, BufferPoolError> {
        self.tail.read_single(column, addr)
    }

    #[inline]
    pub fn write_indirection(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        range: WhichRange
    ) -> Result<(), BufferPoolError> {
        match range {
            WhichRange::Base => self.base.write_meta_col(addr, val, MetaPage::IndirectionCol),
            WhichRange::Tail => self.tail.write_meta_col(addr, val, MetaPage::IndirectionCol),
        }
    }


    #[inline]
    pub fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, BufferPoolError> {
        self.base.read(addr)
    }

    #[inline]
    pub fn read_projected(
        &self,
        projected: &[i64],
        addr: &PhysicalAddress,
    ) -> Result<Vec<Option<i64>>, BufferPoolError> {
        self.base.read_projected(projected, addr)
    }

    pub fn read_meta_col(&self, addr: &PhysicalAddress, col_type : MetaPage, range: WhichRange) -> Result<Option<i64>, BufferPoolError>{
        match range {
            WhichRange::Base => self.base.read_meta_col(addr, col_type),
            WhichRange::Tail => self.tail.read_meta_col(addr, col_type),
        }
    }
}

