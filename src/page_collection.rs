use crate::page::{Page, PageError};
use crate::table::Table;

//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct PageCollection {
    pages: Vec<Page>,
}
impl PageCollection {
    pub fn new(pages_per_collection: usize) -> PageCollection {
        Self {
            pages: vec![Page::default(); pages_per_collection], //Creates actual pages
        }
    }

    //different iterators for all, meta, and data cols
    pub fn iter(&mut self) -> impl Iterator<Item = &mut Page> {
        self.pages.iter_mut()
    }

    //Panics when you didn't alloc enough pages per page collection
    // Some exception handling thingy should handle when pages per collection < NUM_META_PAGES
    //.saturating_sub() fails silently
    pub fn iter_data(&mut self) -> impl Iterator<Item = &mut Page> {
        let end = self.pages.len() - Table::NUM_META_PAGES;
        self.pages[..end].iter_mut()
    }

    //Panics when you didn't alloc enough pages per page collection see above
    pub fn iter_meta(&mut self) -> impl Iterator<Item = &mut Page> {
        let beg = self.pages.len() - Table::NUM_META_PAGES;
        self.pages[beg..].iter_mut()
    }

    const RID_COL: usize = 0;
    const INDIRECTION_COL: usize = 1;
    const SCHEMA_ENCODING_COL: usize = 2;
    const START_TIME_COL: usize = 3;

    // Returns a reference to the metadata page at the given column index
    fn meta_page(&self, col: usize) -> &Page {
        let meta_start = self.pages.len() - Table::NUM_META_PAGES;
        &self.pages[meta_start + col]
    }

    pub fn get_rid(&self, offset: usize) -> Result<Option<i64>, PageError> {
        self.meta_page(Self::RID_COL).read(offset)
    }

    pub fn get_indirection(&self, offset: usize) -> Result<Option<i64>, PageError> {
        self.meta_page(Self::INDIRECTION_COL).read(offset)
    }

    pub fn get_schema_encoding(&self, offset: usize) -> Result<Option<i64>, PageError> {
        self.meta_page(Self::SCHEMA_ENCODING_COL).read(offset)
    }

    pub fn get_start_time(&self, offset: usize) -> Result<Option<i64>, PageError> {
        self.meta_page(Self::START_TIME_COL).read(offset)
    }
}
