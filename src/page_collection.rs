use crate::page::{Page, PageError};
use crate::table::Table;

#[repr(usize)]
pub enum MetaPage {
    RidCol = 0,
    IndirectionCol = 1,
    SchemaEncodingCol = 2,
    StartTimeCol = 3,
    BaseRidCol = 4
}


//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct PageCollection {
    pub(crate) pages: Vec<Page>,
}
impl PageCollection {
    pub fn new(pages_per_collection: usize) -> PageCollection {
        Self {
            pages: vec![Page::default(); pages_per_collection], //Creates actual pages
        }
    }

    #[inline]
    pub fn write_col(&mut self, col: usize, val: Option<i64>) -> Result<(), PageError> {
        self.pages[col].write(val)
    }

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
            MetaPage::BaseRidCol => panic!("Cannot update BaseRID")
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
