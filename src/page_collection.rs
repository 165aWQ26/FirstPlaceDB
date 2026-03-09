use crate::iterators::PidRange;
use crate::page::{Page, PageError};
use crate::table::Table;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use crate::bufferpool::{BufferPool, BufferPoolError};

#[repr(usize)]
pub enum MetaPage {
    RidCol = 0,
    IndirectionCol = 1,
    SchemaEncodingCol = 2,
    StartTimeCol = 3,
}

pub struct PageCollection {
    pid_range: PidRange,
    table_id: usize,
    bufferpool: Arc<BufferPool>,
    num_pages: usize,
    tps: AtomicI64,
}

impl PageCollection {
    pub fn new(pid_range: PidRange, table_id: usize, bufferpool: Arc<BufferPool>) -> PageCollection {
        Self {
            num_pages: pid_range.end - pid_range.start,
            pid_range,
            table_id,
            bufferpool,
            tps: AtomicI64::new(i64::MIN),
        }
    }

    #[inline]
    pub fn get_tps(&self) -> i64 {
        self.tps.load(Ordering::Acquire)
    }

    #[inline]
    pub fn update_tps(&self, new_tps: i64) {
        self.tps.fetch_max(new_tps, Ordering::Release);
    }

    #[inline]
    pub fn write_col(&self, col: usize, offset: usize, val: Option<i64>) -> Result<(), BufferPoolError> {
        self.bufferpool.write(self.make_pid(col), val, offset)
    }

    pub fn write_cols(&self, offset: usize, vals: Vec<Option<i64>>) -> Result<(), BufferPoolError> {
        (0..self.num_pages)
            .try_for_each(|i| self.write_col(i, offset, vals[i]))
    }

    #[inline]
    pub fn read_col(&self, col: usize, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        self.bufferpool.read(self.make_pid(col), offset)
    }

    #[inline]
    pub fn update_meta_col(
        &self,
        col: MetaPage,
        offset: usize,
        val: Option<i64>,
    ) -> Result<(), BufferPoolError> {
        match col {
            MetaPage::IndirectionCol => {
                let actual_col = self.num_pages - Table::NUM_META_PAGES + col as usize;
                self.bufferpool.update(self.make_pid(actual_col), offset, val)
            },
            MetaPage::SchemaEncodingCol => panic!("Cannot update schema encoding"),
            MetaPage::StartTimeCol => panic!("Cannot update start time"),
            MetaPage::RidCol => panic!("Cannot update RID"),
        }
    }

    #[inline]
    pub fn read_meta_col(&self, col: MetaPage, offset: usize) -> Result<Option<i64>, BufferPoolError> {
        self.read_col(self.num_pages - Table::NUM_META_PAGES + col as usize, offset)
    }

    #[inline]
    pub fn read_all(&self, offset: usize) -> Result<Vec<Option<i64>>, BufferPoolError> {
        (0..self.num_pages).map(|i| self.read_col(i, offset)).collect()
    }

    #[inline]
    pub fn make_pid(&self, col: usize) -> PageId {
        PageId::new(col + self.pid_range.start, self.table_id)
    }
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub struct PageId {
    pub(crate) page_num: usize,
    pub(crate) table_id: usize,
}

impl PageId {
    pub fn new(page_num: usize, table_id: usize) -> PageId {
        PageId { page_num, table_id }
    }
}