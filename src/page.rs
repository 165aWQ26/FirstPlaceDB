const MAX_RECORDS: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
}

pub trait GenericPage<T: Copy> {
    fn data_mut(&mut self) -> &mut Vec<T>;
    fn data(&self) -> &Vec<T>;

    fn new() -> Self
    where
        Self: Sized,
    {
        Self::with_capacity(MAX_RECORDS)
    }

    fn with_capacity(capacity: usize) -> Self
    where
        Self: Sized;

    fn has_capacity(&self) -> bool {
        self.data().len() < MAX_RECORDS
    }

    fn write(&mut self, val: T) -> Result<(), PageError> {
        if self.has_capacity() {
            self.data_mut().push(val);
            Ok(())
        } else {
            Err(PageError::Full)
        }
    }

    fn read(&self, index: usize) -> Result<T, PageError> {
        self.data()
            .get(index)
            .copied()
            .ok_or(PageError::IndexOutOfBounds(index))
    }

    fn len(&self) -> usize {
        self.data().len()
    }
}

#[derive(Clone, Debug)]
pub struct I64Page {
    data: Vec<i64>,
}

impl GenericPage<i64> for I64Page {
    fn new() -> Self {
        I64Page {
            data: Vec::with_capacity(MAX_RECORDS),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        I64Page {
            data: Vec::with_capacity(capacity),
        }
    }

    fn data_mut(&mut self) -> &mut Vec<i64> {
        &mut self.data
    }

    fn data(&self) -> &Vec<i64> {
        &self.data
    }
}

impl Default for I64Page {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct NullableI64Page {
    data: Vec<Option<i64>>,
}

impl GenericPage<Option<i64>> for NullableI64Page {
    fn new() -> Self {
        NullableI64Page {
            data: Vec::with_capacity(MAX_RECORDS),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        NullableI64Page {
            data: Vec::with_capacity(capacity),
        }
    }

    fn data_mut(&mut self) -> &mut Vec<Option<i64>> {
        &mut self.data
    }

    fn data(&self) -> &Vec<Option<i64>> {
        &self.data
    }
}

impl Default for NullableI64Page {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // I64Page Tests

    #[test]
    fn test_new_page_is_empty() {
        let page = I64Page::new();
        assert_eq!(page.len(), 0);
        assert!(page.data().capacity() >= MAX_RECORDS);
    }

    #[test]
    fn test_has_capacity_when_empty() {
        let page = I64Page::new();
        assert!(page.has_capacity());
    }

    // Write

    #[test]
    fn test_write_single_value() {
        let mut page = I64Page::new();
        let result = page.write(42);
        assert!(result.is_ok());
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), 42);
    }

    #[test]
    fn test_write_until_full() {
        let mut page = I64Page::new();
        for i in 0..MAX_RECORDS {
            assert!(page.has_capacity());
            assert!(page.write(i as i64).is_ok());
        }
        assert_eq!(page.len(), MAX_RECORDS);
        assert!(!page.has_capacity());
    }

    #[test]
    fn test_write_beyond_capacity_fails() {
        let mut page = I64Page::new();
        for i in 0..MAX_RECORDS {
            page.write(i as i64).unwrap();
        }
        let result = page.write(999);
        assert_eq!(result.unwrap_err(), PageError::Full);
    }

    // Read

    #[test]
    fn test_read_valid_index() {
        let mut page = I64Page::new();
        page.write(10).unwrap();
        page.write(20).unwrap();
        page.write(30).unwrap();
        assert_eq!(page.read(0).unwrap(), 10);
        assert_eq!(page.read(1).unwrap(), 20);
        assert_eq!(page.read(2).unwrap(), 30);
    }

    #[test]
    fn test_read_invalid_index() {
        let mut page = I64Page::new();
        page.write(42).unwrap();
        assert_eq!(page.read(1).unwrap_err(), PageError::IndexOutOfBounds(1));
        assert_eq!(page.read(100).unwrap_err(), PageError::IndexOutOfBounds(100));
    }

    // Len

    #[test]
    fn test_len_updates_correctly() {
        let mut page = I64Page::new();
        assert_eq!(page.len(), 0);
        page.write(1).unwrap();
        assert_eq!(page.len(), 1);
        page.write(2).unwrap();
        assert_eq!(page.len(), 2);
    }
}
