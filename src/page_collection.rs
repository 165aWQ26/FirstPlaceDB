use std::sync::Arc;
use dashmap::DashMap;
use crate::iterators::{BufferPoolFrameMap, PidRange};
use crate::page::{Page, PageError};
use crate::table::Table;

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
    bp_lookup_map: Arc<BufferPoolFrameMap>
}
impl PageCollection {
    pub fn new(pid_range: PidRange, table_id: usize, bp_lookup_map: Arc<BufferPoolFrameMap>) -> PageCollection {
        for x in pid_range.start..pid_range.end {
            bp_lookup_map.insert(Pid::new(x, table_id))
        }

        Self {
            pid_range,
            table_id,
            bp_lookup_map,
        }
    }

    //Todo: delete all my comments when done please!
    #[inline]
    pub fn write_col(&mut self, col: usize, val: Option<i64>) -> Result<(), PageError> {
        //write an individual column by getting start + col, table_id from bufferpool, then writing to the page.
        //This works because our pages are append only (no need to remember what offset to write to --> always write to the end)
        //self.pages[col].write(val)
    }

    #[inline]
    pub fn read_col(&self, col: usize, offset: usize) -> Result<Option<i64>, PageError> {
        //write an individual column by getting start + col, table_id from bufferpool, then reading the page at offset.

        //self.pages[col].read(offset)
    }

    #[inline]
    pub fn update_meta_col(
        &mut self,
        offset: usize,
        val: Option<i64>,
        col : MetaPage
    ) -> Result<(), PageError> {
        match col {
            MetaPage::IndirectionCol =>  {
                let actual_col = self.pages.len() - Table::NUM_META_PAGES + col as usize;
                self.pages[actual_col].update(offset, val)
            },
            MetaPage::SchemaEncodingCol => panic!("Cannot update schema encoding"),
            MetaPage::StartTimeCol => panic!("Cannot update start time"),
            MetaPage::RidCol => panic!("Cannot update RID"),
        }
    }

    // Returns a reference to the metadata page at the given column index
    #[inline]
    fn meta_record(&self, col: MetaPage) -> &Page {
        let meta_start = self.pages.len() - Table::NUM_META_PAGES;
        &self.pages[meta_start + col as usize]
    }

    #[inline]
    pub fn read_meta_col(&self, offset: usize, col_type: MetaPage) -> Result<Option<i64>, PageError> {
        self.meta_record(col_type).read(offset)
    }

    pub fn get_page(&self, col: usize) -> Result<&Page, PageError> {
        let pid = Pid::new(col + self.pid_range.start, self.table_id);
        //Todo: Whoops this can't be done here
    }
}


#[derive(Hash, Eq, PartialEq)]
pub struct Pid {
    page_num: usize,
    table_id: usize,
}

impl Pid {
    pub fn new(page_num: usize, table_id: usize) -> Pid {
        Pid { page_num, table_id }
    }
}