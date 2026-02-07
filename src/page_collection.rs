use crate::page::Page;

//TODO:!!
pub struct PageCollection {
    pages: Vec<Page>
}
impl PageCollection {
    pub const NUM_META_PAGES: usize = 3;
    pub const NUM_PAGES: usize = PageCollection::NUM_META_PAGES + 4; //TODO: PLACEHOLDER MAKE COR NUM PAGES
    pub fn iter(&mut self) -> impl Iterator<Item = &mut Page> {
        let end = self.pages.len() - Self::NUM_META_PAGES;
        self.pages[..end].iter_mut()
    }
}
impl Default for PageCollection {
    fn default() -> Self {
        Self {
            pages: vec![Page::default(); Self::NUM_PAGES], //TODO: IMPORTANT LINKED TO ABOVE THIS ALLOCATES PAGES
                                                            //THIS DOESN'T just alloc space for a page, it creates the page
        }
    }
}
