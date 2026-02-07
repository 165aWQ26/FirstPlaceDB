use crate::page::Page;

pub struct PageCollection {
    pages: Vec<Page>
}
impl PageCollection {
    pub const NUM_META_PAGES: usize = 3;
    pub const NUM_PAGES: usize = PageCollection::NUM_META_PAGES + 4; //TODO: PLACEHOLDER MAKE COR NUM PAGES
    pub fn iter_data(&mut self) -> impl Iterator<Item=&mut Page> {
        let end = self.pages.len() - Self::NUM_META_PAGES;
        self.pages[..end].iter_mut()
    }

    pub fn new(numPages: usize) -> PageCollection {
        Self {
            pages: vec![Page::default(); Self::NUM_PAGES]
        }
    }

    pub fn iter(&mut self) -> impl Iterator<Item=&mut Page> {
        self.pages.iter_mut()
    }

    pub fn iter_meta(&mut self) -> impl Iterator<Item=&mut Page> {
        let beg = self.pages.len() - Self::NUM_META_PAGES;
        self.pages[beg..].iter_mut()
    }

    //TODO: Write getters for specific metadatacols

}
