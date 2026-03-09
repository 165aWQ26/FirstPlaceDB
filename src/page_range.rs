use crate::bufferpool::{BufferPool, BufferPoolError};
use crate::iterators::{PhysicalAddress, PhysicalAddressIterator, PidRange, PidRangeIterator};
use crate::page::Page;
use crate::page_collection::{MetaPage, PageCollection};
use crate::table::Table;
use dashmap::DashMap;
use std::sync::Arc;

pub struct PageRange {
    range: DashMap<usize, PageCollection>,
    pub(crate) next_addr: PhysicalAddressIterator,
    pages_per_collection: usize,
    table_id: usize,
    bufferpool: Arc<BufferPool>,
    pub(crate) pid_iterator: Arc<PidRangeIterator>,
}

impl PageRange {
    pub const PROJECTED_NUM_PAGE_COLLECTIONS: usize =
        Table::PROJECTED_NUM_RECORDS.div_ceil(Page::PAGE_SIZE);

    pub fn new(
        pages_per_collection: usize,
        first_pid: PidRange,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        pid_iterator: Arc<PidRangeIterator>,
    ) -> Self {
        // let mut init_range: DashMap<usize, PageCollection> =
        //     Vec::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        // init_range.push(PageCollection::new(first_pid, table_id, bufferpool.clone()));
        let range: DashMap<usize, PageCollection> =
            DashMap::with_capacity(PageRange::PROJECTED_NUM_PAGE_COLLECTIONS);
        range.insert(
            0,
            PageCollection::new(first_pid, table_id, bufferpool.clone()),
        );

        Self {
            range,
            next_addr: PhysicalAddressIterator::default(),
            pages_per_collection,
            table_id,
            bufferpool,
            pid_iterator,
        }
    }

    pub fn restore(
        pages_per_collection: usize,
        collections: Vec<(usize, usize)>,
        next_addr_val: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        pid_iterator: Arc<PidRangeIterator>,
    ) -> Self {
        let range: DashMap<usize, PageCollection> = DashMap::with_capacity(collections.len());
        for (i, (start, end)) in collections.into_iter().enumerate() {
            range.insert(
                i,
                PageCollection::new(PidRange { start, end }, table_id, bufferpool.clone()),
            );
        }
        let next_addr = PhysicalAddressIterator::new();
        next_addr.restore(next_addr_val);
        Self {
            range,
            next_addr,
            pages_per_collection,
            table_id,
            bufferpool,
            pid_iterator,
        }
    }

    fn append(&self, all_data: Vec<Option<i64>>) -> Result<PhysicalAddress, BufferPoolError> {
        let addr = self.next_addr.next();

        if let Some(collection) = self.range.get(&addr.collection_num) {
            return collection.write_cols(addr.offset, all_data).map(|_| addr);
        }

        self.range
            .entry(addr.collection_num)
            .or_insert_with(|| {
                PageCollection::new(
                    self.pid_iterator.next(),
                    self.table_id,
                    self.bufferpool.clone(),
                )
            })
            .write_cols(addr.offset, all_data)?;

        Ok(addr)
    }

    // fn lazy_create_page_collection(&mut self, page: &usize) {
    //     // while self.range.len() <= page {
    //     //     self.range.push(PageCollection::new(
    //     //         self.pid_iterator.next(),
    //     //         self.table_id,
    //     //         self.bufferpool.clone(),
    //     //     ));
    //     // }
    // }

    fn read(&self, addr: &PhysicalAddress) -> Result<Vec<Option<i64>>, BufferPoolError> {
        self.range
            .get(&addr.collection_num)
            .ok_or(BufferPoolError::PidNotInFrame)?
            .read_all(addr.offset)
    }

    #[inline]
    fn read_single(
        &self,
        col: usize,
        addr: &PhysicalAddress,
    ) -> Result<Option<i64>, BufferPoolError> {
        self.range
            .get(&addr.collection_num)
            .ok_or(BufferPoolError::PidNotInFrame)?
            .read_col(col, addr.offset)
    }

    #[inline]
    pub fn write_meta_col(
        &self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col: MetaPage,
    ) -> Result<(), BufferPoolError> {
        self.range
            .get(&addr.collection_num)
            .ok_or(BufferPoolError::PidNotInFrame)?
            .update_meta_col(col, addr.offset, val)
    }

    pub fn read_meta_col(
        &self,
        addr: &PhysicalAddress,
        col: MetaPage,
    ) -> Result<Option<i64>, BufferPoolError> {
        self.range
            .get(&addr.collection_num)
            .ok_or(BufferPoolError::PidNotInFrame)?
            .read_meta_col(col, addr.offset)
    }

    /// Returns the TPS watermark for the collection that contains `addr`.
    #[inline]
    pub fn get_tps(&self, addr: &PhysicalAddress) -> i64 {
        self.range
            .get(&addr.collection_num)
            .map(|c| c.get_tps())
            .ok_or(BufferPoolError::PidNotInFrame)
            .unwrap_or(i64::MIN)
    }

    /// Advances the TPS watermark for the collection that contains `addr`.
    #[inline]
    pub fn update_tps(&self, addr: &PhysicalAddress, new_tps: i64) {
        if let Some(collection) = self.range.get(&addr.collection_num) {
            collection.update_tps(new_tps)
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

    pub fn collection_pid_ranges(&self) -> Vec<(usize, usize)> {
        let mut pairs: Vec<(usize, usize, usize)> = self
            .range
            .iter()
            .map(|e| (*e.key(), e.value().pid_range.start, e.value().pid_range.end))
            .collect();
        pairs.sort_unstable_by_key(|&(idx, _, _)| idx);
        pairs.into_iter().map(|(_, s, e)| (s, e)).collect()
    }

    pub fn next_addr_value(&self) -> usize {
        self.next_addr.current()
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

    pub fn restore(
        pages_per_collection: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        base_collection: Vec<(usize, usize)>,
        tail_collection: Vec<(usize, usize)>,
        base_next_addr: usize,
        tail_next_addr: usize,
        pid_next_start: usize,
    ) -> Self {
        let pid_iterator = Arc::new(PidRangeIterator::restore(pid_next_start, pages_per_collection, ));
        Self {
            tail: PageRange::restore(
                pages_per_collection,
                tail_collection,
                tail_next_addr,
                table_id,
                bufferpool.clone(),
                pid_iterator.clone(),
            ),
            base: PageRange::restore(
                pages_per_collection,
                base_collection,
                base_next_addr,
                table_id,
                bufferpool.clone(),
                pid_iterator.clone(),
            ),
        }
    }

    /// For inserts: new base record with indirection pointing to itself.
    pub fn append_base(
        &self,
        data_cols: &Vec<Option<i64>>,
        rid: i64,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        let mut all_cols = data_cols.clone();
        all_cols.push(Some(rid)); // RID
        all_cols.push(Some(rid)); // indirection (self — no updates yet)
        all_cols.push(Some(0)); // schema_encoding
        all_cols.push(None); // start_time

        self.base.append(all_cols)
    }

    /// For merge: consolidated base record preserving the existing indirection pointer.
    /// Merge never resets indirection — readers use TPS to know what is already baked in.
    pub fn append_base_merged(
        &self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(indirection)); // indirection preserved from existing base
        data_cols.push(schema_encoding); // None = deleted, Some(0) = live
        data_cols.push(None); // start_time
        self.base.append(data_cols)
    }

    /// For updates: append a tail record pointing back to the previous version.
    pub fn append_tail(
        &self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(indirection)); // indirection (points to prev version)
        data_cols.push(schema_encoding); // schema_encoding: None = deletion
        data_cols.push(None); // start_time
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
        &self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        range: WhichRange,
    ) -> Result<(), BufferPoolError> {
        match range {
            WhichRange::Base => self
                .base
                .write_meta_col(addr, val, MetaPage::Indirection),
            WhichRange::Tail => self
                .tail
                .write_meta_col(addr, val, MetaPage::Indirection),
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

    pub fn base_collection_pid_ranges(&self) -> Vec<(usize, usize)> {
        self.base.collection_pid_ranges()
    }

    pub fn tail_collection_pid_ranges(&self) -> Vec<(usize, usize)> {
        self.tail.collection_pid_ranges()
    }

    pub fn base_next_addr(&self) -> usize {
        self.base.next_addr_value()
    }

    pub fn tail_next_addr(&self) -> usize {
        self.tail.next_addr_value()
    }

    pub fn pid_next_start(&self) -> usize {
        self.base.pid_iterator.current()
    }
}
