use crate::bufferpool::{BufferPool, MetaPage};
use crate::bufferpool_context::{PageLocation, TableContext};
use crate::db_error::DbError;
use crate::page::{Page, PageError};
use crate::table::Table;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct PageRange {
    next_addr: PhysicalAddressIterator,
}

impl PageRange {
    pub fn new() -> Self {
        Self {
            next_addr: PhysicalAddressIterator::default(),
        }
    }

    pub fn next_addr(&mut self) -> PhysicalAddress {
        self.next_addr.next().unwrap()
    }

    pub fn position(&self) -> (usize, usize) {
        (
            self.next_addr.current.offset,
            self.next_addr.current.collection_num,
        )
    }

    pub fn set_position(&mut self, offset: usize, collection_num: usize) {
        self.next_addr.current.offset = offset;
        self.next_addr.current.collection_num = collection_num;
    }
}

#[derive(PartialEq, Eq)]
pub enum WhichRange {
    Base,
    Tail,
}
#[derive(Clone)]
pub struct PageRanges {
    pub(crate) tail: PageRange,
    pub(crate) base: PageRange,
    bufferpool: Arc<Mutex<BufferPool>>,
}

impl PageRanges {
    pub fn new(bufferpool: Arc<Mutex<BufferPool>>) -> Self {
        Self {
            tail: PageRange::new(),
            base: PageRange::new(),
            bufferpool,
        }
    }

    // For inserts: stages metadata (rid, indirection=rid, schema=0) then appends to base
    pub fn append_base(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        table_ctx: &TableContext,
    ) -> Result<PhysicalAddress, DbError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(rid)); // indirection (self for new base record)
        data_cols.push(Some(0)); // schema_encoding (no updates)
        data_cols.push(None);
        self.append(data_cols, WhichRange::Base, &table_ctx)
    }

    // For updates: caller provides indirection (previous version) and schema_encoding (which cols updated)
    pub fn append_tail(
        &mut self,
        mut data_cols: Vec<Option<i64>>,
        rid: i64,
        indirection: i64,
        schema_encoding: Option<i64>,
        table_ctx: &TableContext,
    ) -> Result<PhysicalAddress, DbError> {
        data_cols.push(Some(rid)); // RID
        data_cols.push(Some(indirection)); // indirection (points to prev version)
        data_cols.push(schema_encoding); // schema_encoding: None = deletion, Some(bitmask) = update
        data_cols.push(None);
        self.append(data_cols, WhichRange::Tail, table_ctx)
    }

    #[inline]
    pub fn read_single(
        &mut self,
        column: usize,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<Option<i64>, DbError> {
        Ok(self
            .bufferpool
            .lock()
            .read_col(column, &page_location, table_ctx)?)
    }

    fn append(
        &mut self,
        all_data: Vec<Option<i64>>,
        range: WhichRange,
        table_ctx: &TableContext,
    ) -> Result<PhysicalAddress, DbError> {
        let addr = match range {
            WhichRange::Base => self.base.next_addr(),
            WhichRange::Tail => self.tail.next_addr(),
        };

        let page_location = PageLocation::new(addr, range);
        self.bufferpool
            .lock()
            .append(all_data, &page_location, table_ctx)?;
        Ok(addr)
    }

    #[inline]
    pub fn write_indirection(
        &mut self,
        val: Option<i64>,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<(), PageError> {
        self.bufferpool.lock().update_meta_col(
            val,
            MetaPage::IndirectionCol,
            page_location,
            table_ctx,
        )
    }

    #[inline]
    pub fn read(
        &self,
        addr: &PhysicalAddress,
        table_ctx: &TableContext,
    ) -> Result<Vec<Option<i64>>, DbError> {
        let num_data_cols = table_ctx.total_cols - Table::NUM_META_PAGES;
        let page_location = PageLocation::base(*addr);
        let mut buff = self.bufferpool.lock();
        (0..num_data_cols)
            .map(|col| Ok(buff.read_col(col, &page_location, table_ctx)?))
            .collect()
    }

    pub fn read_meta_col(
        &mut self,
        col_type: MetaPage,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<Option<i64>, PageError> {
        self.bufferpool
            .lock()
            .read_meta_col(col_type, &page_location, table_ctx)
    }
}

//Possibly put here & below into its own file
//This iterator automatically manages where you write to.
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Default)]
pub struct PhysicalAddress {
    pub(crate) offset: usize,
    pub(crate) collection_num: usize,
}

#[derive(Default, Clone)]
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
