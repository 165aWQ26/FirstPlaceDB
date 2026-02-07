use std::ptr::addr_of_mut;
use rustc_hash::FxHashMap;

use crate::page::Page;
use crate::iterator::BasicIterator;
use crate::page_directory::PageDirectory;
use crate::page_range::PageRanges;

pub struct Table {

    pub name: String,

    pub pageRanges: PageRanges,

    pub pageDirectory: PageDirectory,

    pub rid : BasicIterator
}

impl Table {
    pub const PROJECTED_NUM_RECORDS : usize = 1200;
    pub fn new(&mut self, tableName: String) -> Table {
        Self {
            name: tableName,
            pageRanges : PageRanges::default(),
            pageDirectory : PageDirectory::default(),
            rid : BasicIterator::default()

        }
    }
}
