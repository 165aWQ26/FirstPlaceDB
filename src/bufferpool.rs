use std::num::NonZeroUsize;

use crate::page::{Page, PageError};
use crate::page_range::{PhysicalAddress, WhichRange};
use crate::table::Table;
use lru::LruCache;


#[repr(usize)]
pub enum MetaPage {
    RidCol = 0,
    IndirectionCol = 1,
    SchemaEncodingCol = 2,
    StartTimeCol = 3,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BufferPoolError{
    ZeroPid,
    Full,
}

//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct BufferPool {
    frames: LruCache<i64, Page>,
    size: usize,
    total_cols: usize,
    path: String
}

impl BufferPool {
    pub const NUMBER_OF_FRAMES: usize = 32;
    
    pub fn set_total_cols(){
        
    }

    //Done
    #[inline]
    pub fn write_col(&mut self, addr: PhysicalAddress, range: WhichRange, val: i64) -> Result<(), PageError> {
        
        self.frames.get(pid).write(val);
    }

    //Done
    #[inline]
    pub fn read_col(&self, col: usize, offset: usize) -> Result<Option<i64>, PageError> {
        
        self.pages[col].read(offset)
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



}

impl Default for BufferPool {
    fn default() -> Self {
        Self {
            frames: LruCache::new(NonZeroUsize::new(BufferPool::NUMBER_OF_FRAMES).unwrap()),
            size: 0,
            total_cols: 0, // need setter
            path: String::from(""), // need setter
        }
    }
}


pub struct Pid {
    pid: i64,
}

impl Pid{
    fn new (i : i64){

    }

    fn next(&mut self, i: i64) -> Result<Option<i64>,BufferPollError> {
        if self.pid > 0 {
            self.pid += i;
        }
        else if self.pid < 0 {
            self.pid += i;
        }
        else{
            Err(BufferPollError::ZeroPid);
        }
        Some(self.pid);
    }
}