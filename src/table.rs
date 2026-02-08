use std::ptr::addr_of_mut;
use rustc_hash::FxHashMap;

use crate::page::Page;
use crate::page_directory::PageDirectory;
use crate::page_range::PageRanges;

pub struct Table {

    pub name: String,

    pub pageRanges: PageRanges,

    pub pageDirectory: PageDirectory,

    pub rid : std::ops::RangeFrom<usize>,

}

impl Table {
    pub const PROJECTED_NUM_RECORDS : usize = 1200;
    pub const NUM_META_PAGES: usize = 3;
    //data_pages_per_collection is the total number of pages in a pagedir
    pub fn new(&mut self, tableName: String, data_pages_per_collection: usize) -> Table {
        Self {
            name: tableName,
            pageRanges : PageRanges::new(data_pages_per_collection),
            pageDirectory : PageDirectory::default(),
            rid : 0..,
        }
    }
}
