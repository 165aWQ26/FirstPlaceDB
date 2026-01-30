const MAX_RECORDS: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
}

#[derive(Clone, Debug)]
pub struct Page {
    data: Vec<i64>,
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: Vec::with_capacity(MAX_RECORDS),
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.data.len() < MAX_RECORDS
    }

    pub fn write(&mut self, val: i64) -> Result<(), PageError> {
        if self.has_capacity() {
            self.data.push(val);
            Ok(())
        } else {
            Err(PageError::Full)
        }
    }

    // I don't think a page is allowed to be updated. Should be append only but I'll leave it here for now.
    // TODO: Remove this?
    pub fn update(&mut self, index: usize, val: i64) -> Result<(), PageError> {
        if index >= self.data.len() {
            Err(PageError::IndexOutOfBounds(index))
        } else {
            self.data[index] = val;
            Ok(())
        }
    }

    pub fn read(&self, index: usize) -> Result<i64, PageError> {
        self.data
            .get(index)
            .copied()
            .ok_or(PageError::IndexOutOfBounds(index))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
