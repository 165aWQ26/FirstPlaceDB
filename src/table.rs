use crate::page_directory::PageDirectory;
use crate::page_range::PageRanges;

pub struct Table {
    pub name: String,

    pub page_ranges: PageRanges,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<u64>,

    pub num_columns: usize,

    pub key_index: usize,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 1200;
    pub const NUM_META_PAGES: usize = 4;
    //data_pages_per_collection is the total number of pages in a PageDirectory
    pub fn new(table_name: String, data_pages_per_collection: usize, key_index: usize) -> Table {
        Self {
            name: table_name,
            page_ranges: PageRanges::new(data_pages_per_collection),
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index: key_index,
            num_columns: num_columns,
        }
    }
}
