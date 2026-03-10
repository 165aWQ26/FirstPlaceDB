#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
}

#[derive(Clone, Debug)]
pub struct Page {
    data: [Option<i64>; Page::PAGE_SIZE],
    num_records: usize,
}

impl Page {
    pub const PAGE_SIZE: usize = 512;

    #[inline]
    pub fn has_capacity(&self) -> bool {
        self.num_records < Page::PAGE_SIZE
    }

    #[inline]
    pub fn write(&mut self, val: Option<i64>, offset: usize) -> Result<(), PageError> {
        if offset >= Page::PAGE_SIZE {
            return Err(PageError::Full);
        }
        self.data[offset] = val;
        self.num_records += 1;
        Ok(())
    }

    #[inline]
    pub fn read(&self, index: usize) -> Result<Option<i64>, PageError> {
        self.data
            .get(index)
            .copied()
            .ok_or(PageError::IndexOutOfBounds(index))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_records
    }

    #[inline]
    pub fn update(&mut self, index: usize, val: Option<i64>) -> Result<(), PageError> {
        if index >= self.data.len() {
            return Err(PageError::IndexOutOfBounds(index));
        }
        self.data[index] = val;
        Ok(())
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [None; Page::PAGE_SIZE],
            num_records: 0,
        }
    }
}