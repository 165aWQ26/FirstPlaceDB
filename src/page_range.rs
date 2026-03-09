use std::sync::Arc;
use crate::iterators::{PhysicalAddress, PhysicalAddressIterator, PidRange, PidRangeIterator};
use crate::page::Page;
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
}

impl PageRange {
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize =
        (Table::PROJECTED_NUM_RECORDS + Page::PAGE_SIZE - 1) / Page::PAGE_SIZE;

    pub fn new(
        pages_per_collection: usize,
        first_pid: PidRange,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        pid_iterator: Arc<PidRangeIterator>,
    ) -> Self {
        let mut init_range: Vec<PageCollection> =
            Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        init_range.push(PageCollection::new(first_pid, table_id, bufferpool.clone()));

        Self {
            range: init_range,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
            table_id,
            bufferpool,
            pid_iterator,
        }
    }

    fn append(&mut self, all_data: Vec<Option<i64>>) -> Result<PhysicalAddress, BufferPoolError> {
        let addr = self.next_addr.next();
        self.lazy_create_page_collection(addr.collection_num);
        self.range[addr.collection_num].write_cols(addr.offset, all_data)?;
        Ok(addr)
    }

    fn lazy_create_page_collection(&mut self, page: usize) {
        while self.range.len() <= page {
            self.range.push(PageCollection::new(
                self.pid_iterator.next(),
                self.table_id,
                self.bufferpool.clone(),
            ));
        }
    }

    fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, BufferPoolError> {
        self.range[addr.collection_num].read_all(addr.offset)
    }

    #[inline]
    fn read_single(&self, col: usize, addr: &PhysicalAddress) -> Result<Option<i64>, BufferPoolError> {
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

    pub fn read_meta_col(
        &self,
        addr: &PhysicalAddress,
        col: MetaPage,
    ) -> Result<Option<i64>, BufferPoolError> {
        Ok(self.range[addr.collection_num].read_meta_col(col, addr.offset)?)
    }

    /// Returns the TPS watermark for the collection that contains `addr`.
    #[inline]
    pub fn get_tps(&self, addr: &PhysicalAddress) -> i64 {
        self.range
            .get(addr.collection_num)
            .map(|c| c.get_tps())
            .unwrap_or(i64::MIN)
    }

    /// Advances the TPS watermark for the collection that contains `addr`.
    #[inline]
    pub fn update_tps(&self, addr: &PhysicalAddress, new_tps: i64) {
        if let Some(collection) = self.range.get(addr.collection_num) {
            collection.update_tps(new_tps);
        }
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
    pub(crate) tail: PageRange,
    pub(crate) base: PageRange,
}

impl PageRanges {
    pub fn new(pages_per_collection: usize, table_id: usize, bufferpool: Arc<BufferPool>) -> Self {
        let pid_range_iter = Arc::new(PidRangeIterator::new(pages_per_collection));
        Self {
            tail: PageRange::new(
                pages_per_collection,
                pid_range_iter.next(),
                table_id,
                bufferpool.clone(),
                pid_range_iter.clone(),
            ),
            base: PageRange::new(
                pages_per_collection,
                pid_range_iter.next(),
                table_id,
                bufferpool,
                pid_range_iter,
            ),
        }
    }

    /// For inserts: new base record with indirection pointing to itself.
    pub fn append_base(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(rid)); // indirection (self — no updates yet)
        data_cols.push(Some(0));   // schema_encoding
        data_cols.push(None);      // start_time
        self.base.append(data_cols)
    }

    /// For merge: consolidated base record preserving the existing indirection pointer.
    /// Merge never resets indirection — readers use TPS to know what is already baked in.
    pub fn append_base_merged(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid));       // RID
        data_cols.push(Some(indirection)); // indirection preserved from existing base
        data_cols.push(schema_encoding); // None = deleted, Some(0) = live
        data_cols.push(None);            // start_time
        self.base.append(data_cols)
    }

    /// For updates: append a tail record pointing back to the previous version.
    pub fn append_tail(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid));       // RID
        data_cols.push(Some(indirection)); // indirection (points to prev version)
        data_cols.push(schema_encoding); // schema_encoding: None = deletion
        data_cols.push(None);            // start_time
        self.tail.append(data_cols)
    }

    #[inline]
    pub fn read_single(
        &self,
        column: usize,
        addr: &PhysicalAddress,
        range: WhichRange,
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
        range: WhichRange,
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

    pub fn read_meta_col(
        &self,
        addr: &PhysicalAddress,
        col_type: MetaPage,
        range: WhichRange,
    ) -> Result<Option<i64>, BufferPoolError> {
        match range {
            WhichRange::Base => self.base.read_meta_col(addr, col_type),
            WhichRange::Tail => self.tail.read_meta_col(addr, col_type),
        }
    }

    #[inline]
    pub fn get_tps(&self, addr: &PhysicalAddress) -> i64 {
        self.base.get_tps(addr)
    }

    #[inline]
    pub fn update_tps(&self, addr: &PhysicalAddress, new_tps: i64) {
        self.base.update_tps(addr, new_tps);
    }
}