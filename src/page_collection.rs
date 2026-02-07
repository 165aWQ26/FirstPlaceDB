use crate::page::Page;
use crate::table::Table;


//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct PageCollection {
    pages: Vec<Page>
}
impl PageCollection {
    
    pub fn new(pages_per_collection: usize) -> PageCollection {
        Self {
            pages: vec![Page::default(); pages_per_collection] //Creates actual pages
        }
    }

    //different iterators for all, meta, and data cols
    pub fn iter(&mut self) -> impl Iterator<Item=&mut Page> {
        self.pages.iter_mut()
    }

    //Panics when you didn't alloc enough pages per page collection
    // Some exception handling thingy should handle when pages per collection < NUM_META_PAGES
    //.saturating_sub() fails silently
    pub fn iter_data(&mut self) -> impl Iterator<Item=&mut Page> {
        let end = self.pages.len() - Table::NUM_META_PAGES;
        self.pages[..end].iter_mut()
    }

    //Panics when you didn't alloc enough pages per page collection see above
    pub fn iter_meta(&mut self) -> impl Iterator<Item=&mut Page> {
        let beg = self.pages.len() - Table::NUM_META_PAGES;
        self.pages[beg..].iter_mut()
    }

    //TODO: Write getters for specific metaDataCols
    
}
