#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
}

#[derive(Clone, Debug)]
pub struct Page {
    data: Vec<Option<i64>>,
}

impl Page {
    pub const PAGE_SIZE: usize = 512;

    #[inline]
    pub fn has_capacity(&self) -> bool {
        self.data.len() < Page::PAGE_SIZE
    }

    #[inline]
    pub fn write(&mut self, val: Option<i64>) -> Result<(), PageError> {
        if !self.has_capacity() {
            return Err(PageError::Full);
        }

        self.data.push(val);
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
            data: Vec::with_capacity(Page::PAGE_SIZE),
        }
    }
}
