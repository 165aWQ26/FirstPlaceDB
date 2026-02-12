use crate::page::{Page, PageError};
use crate::table::Table;

#[repr(usize)]
pub enum MetaPage {
    RID_COL = 0,
    INDIRECTION_COL = 1,
    SCHEMA_ENCODING_COL = 2,
    START_TIME_COL = 3,
}


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

    #[inline]
    pub fn write_column(&mut self, col: usize, val: Option<i64>) -> Result<(), PageError> {
        self.pages[col].write(val)
    }

    #[inline]
    pub fn read_column(&self, col: usize, offset: usize) -> Result<Option<i64>, PageError> {
        self.pages[col].read(offset)
    }

    #[inline]
    pub fn update_column(
        &mut self,
        col: usize,
        offset: usize,
        val: Option<i64>,
    ) -> Result<(), PageError> {
        self.pages[col].update(offset, val)
    }

    // Returns a reference to the metadata page at the given column index
    #[inline]
    fn meta_record(&self, col: MetaPage) -> &Page {
        let meta_start = self.pages.len() - Table::NUM_META_PAGES;
        &self.pages[meta_start + col as usize]
    }

    #[inline]
    pub fn get_meta_page_page_collection(&self, offset: usize, colType: MetaPage) -> Result<Option<i64>, PageError> {
        self.meta_record(colType).read(offset)
    }
}
