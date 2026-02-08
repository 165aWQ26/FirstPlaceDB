use rustc_hash::FxHashMap;
use std::ptr::addr_of_mut;

use crate::page::Page;
use crate::page_directory::PageDirectory;
use crate::page_range::PageRanges;

pub struct Table {
    pub name: String,

    pub page_ranges: PageRanges,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<usize>,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 1200;
    pub const NUM_META_PAGES: usize = 3;
    //data_pages_per_collection is the total number of pages in a pagedir
    pub fn new(table_name: String, data_pages_per_collection: usize, _key_index: usize) -> Table {
        Self {
            name: table_name,
            page_ranges: PageRanges::new(data_pages_per_collection),
            page_directory: PageDirectory::default(),
            rid: 0..,
        }
    }
}
