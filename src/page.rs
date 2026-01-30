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


impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page_is_empty() {
        let page = Page::new();
        assert_eq!(page.len(), 0);
        assert!(page.data.capacity() >= MAX_RECORDS);
    }

    #[test]
    fn test_has_capacity_when_empty() {
        let page = Page::new();
        assert!(page.has_capacity());
    }

    #[test]
    fn test_write_single_value() {
        let mut page = Page::new();
        let result = page.write(42);
        assert!(result.is_ok());
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), 42);
    }

    #[test]
    fn test_write_until_full() {
        let mut page = Page::new();

        // Fill the page
        for i in 0..MAX_RECORDS {
            assert!(page.has_capacity());
            assert!(page.write(i as i64).is_ok());
        }

        assert_eq!(page.len(), MAX_RECORDS);
        assert!(!page.has_capacity());
    }

    #[test]
    fn test_write_beyond_capacity_fails() {
        let mut page = Page::new();

        // Fill the page
        for i in 0..MAX_RECORDS {
            page.write(i as i64).unwrap();
        }

        // Try to write one more
        let result = page.write(999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), PageError::Full);
    }

    #[test]
    fn test_read_valid_index() {
        let mut page = Page::new();
        page.write(10).unwrap();
        page.write(20).unwrap();
        page.write(30).unwrap();

        assert_eq!(page.read(0).unwrap(), 10);
        assert_eq!(page.read(1).unwrap(), 20);
        assert_eq!(page.read(2).unwrap(), 30);
    }

    #[test]
    fn test_read_invalid_index() {
        let mut page = Page::new();
        page.write(42).unwrap();

        assert!(page.read(1).is_err());
        assert!(page.read(100).is_err());
    }

    #[test]
    fn test_update_valid_index() {
        let mut page = Page::new();
        page.write(10).unwrap();
        page.write(20).unwrap();

        assert!(page.update(0, 99).is_ok());
        assert_eq!(page.read(0).unwrap(), 99);
        assert_eq!(page.read(1).unwrap(), 20);
    }

    #[test]
    fn test_update_invalid_index() {
        let mut page = Page::new();
        page.write(42).unwrap();

        assert!(page.update(1, 99).is_err());
        assert!(page.update(100, 99).is_err());
    }

    #[test]
    fn test_len_updates_correctly() {
        let mut page = Page::new();
        assert_eq!(page.len(), 0);

        page.write(1).unwrap();
        assert_eq!(page.len(), 1);

        page.write(2).unwrap();
        assert_eq!(page.len(), 2);
    }
}
