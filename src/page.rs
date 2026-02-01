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
    fn test_with_capacity() {
        let page = I64Page::with_capacity(64);
        assert_eq!(page.len(), 0);
        assert!(page.data().capacity() >= 64);
    }

    #[test]
    fn test_default_matches_new() {
        let page_new = I64Page::new();
        let page_default = I64Page::default();
        assert_eq!(page_new.len(), page_default.len());
        assert_eq!(page_new.data().capacity(), page_default.data().capacity());
    }

    #[test]
    fn test_has_capacity_when_empty() {
        let page = I64Page::new();
        assert!(page.has_capacity());
    }

    #[test]
    fn test_has_capacity_transitions() {
        let mut page = I64Page::new();
        assert!(page.has_capacity());

        // Fill to one slot remaining
        for i in 0..MAX_RECORDS - 1 {
            page.write(i as i64).unwrap();
        }
        assert!(page.has_capacity());

        // Fill last slot
        page.write(0).unwrap();
        assert!(!page.has_capacity());
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
    fn test_write_multiple_values() {
        let mut page = I64Page::new();
        page.write(10).unwrap();
        page.write(20).unwrap();
        page.write(30).unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(page.read(0).unwrap(), 10);
        assert_eq!(page.read(1).unwrap(), 20);
        assert_eq!(page.read(2).unwrap(), 30);
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

    #[test]
    fn test_write_negative_values() {
        let mut page = I64Page::new();
        page.write(-1).unwrap();
        page.write(-100).unwrap();
        assert_eq!(page.read(0).unwrap(), -1);
        assert_eq!(page.read(1).unwrap(), -100);
    }

    #[test]
    fn test_write_boundary_values() {
        let mut page = I64Page::new();
        page.write(i64::MAX).unwrap();
        page.write(i64::MIN).unwrap();
        page.write(0).unwrap();
        assert_eq!(page.read(0).unwrap(), i64::MAX);
        assert_eq!(page.read(1).unwrap(), i64::MIN);
        assert_eq!(page.read(2).unwrap(), 0);
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
    fn test_read_empty_page() {
        let page = I64Page::new();
        assert_eq!(page.read(0).unwrap_err(), PageError::IndexOutOfBounds(0));
    }

    #[test]
    fn test_read_invalid_index() {
        let mut page = I64Page::new();
        page.write(42).unwrap();
        assert_eq!(page.read(1).unwrap_err(), PageError::IndexOutOfBounds(1));
        assert_eq!(page.read(100).unwrap_err(), PageError::IndexOutOfBounds(100));
    }

    #[test]
    fn test_read_last_valid_index() {
        let mut page = I64Page::new();
        for i in 0..10 {
            page.write(i as i64).unwrap();
        }
        assert_eq!(page.read(9).unwrap(), 9);
        assert_eq!(page.read(10).unwrap_err(), PageError::IndexOutOfBounds(10));
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

    // Clone

    #[test]
    fn test_clone_is_independent() {
        let mut page = I64Page::new();
        page.write(10).unwrap();
        page.write(20).unwrap();

        let mut cloned = page.clone();
        cloned.write(30).unwrap();

        // Original is unchanged
        assert_eq!(page.len(), 2);
        assert_eq!(page.read(0).unwrap(), 10);
        assert_eq!(page.read(1).unwrap(), 20);

        // Clone has the new write
        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.read(2).unwrap(), 30);
    }

    // NullableI64Page Tests

    #[test]
    fn test_nullable_new_page_is_empty() {
        let page = NullableI64Page::new();
        assert_eq!(page.len(), 0);
        assert!(page.data().capacity() >= MAX_RECORDS);
    }

    #[test]
    fn test_nullable_default_matches_new() {
        let page_new = NullableI64Page::new();
        let page_default = NullableI64Page::default();
        assert_eq!(page_new.len(), page_default.len());
    }

    // Write

    #[test]
    fn test_nullable_write_some() {
        let mut page = NullableI64Page::new();
        page.write(Some(42)).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), Some(42));
    }

    #[test]
    fn test_nullable_write_none() {
        let mut page = NullableI64Page::new();
        page.write(None).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), None);
    }

    #[test]
    fn test_nullable_write_mixed() {
        let mut page = NullableI64Page::new();
        page.write(Some(10)).unwrap();
        page.write(None).unwrap();
        page.write(Some(30)).unwrap();
        page.write(None).unwrap();
        assert_eq!(page.read(0).unwrap(), Some(10));
        assert_eq!(page.read(1).unwrap(), None);
        assert_eq!(page.read(2).unwrap(), Some(30));
        assert_eq!(page.read(3).unwrap(), None);
    }

    #[test]
    fn test_nullable_write_until_full() {
        let mut page = NullableI64Page::new();
        for i in 0..MAX_RECORDS {
            let val = if i % 2 == 0 { Some(i as i64) } else { None };
            assert!(page.write(val).is_ok());
        }
        assert_eq!(page.len(), MAX_RECORDS);
        assert!(!page.has_capacity());
    }

    #[test]
    fn test_nullable_write_beyond_capacity_fails() {
        let mut page = NullableI64Page::new();
        for i in 0..MAX_RECORDS {
            page.write(Some(i as i64)).unwrap();
        }
        assert_eq!(page.write(Some(999)).unwrap_err(), PageError::Full);
        assert_eq!(page.write(None).unwrap_err(), PageError::Full);
    }

    // Read

    #[test]
    fn test_nullable_read_invalid_index() {
        let mut page = NullableI64Page::new();
        page.write(Some(42)).unwrap();
        assert_eq!(page.read(1).unwrap_err(), PageError::IndexOutOfBounds(1));
    }

    #[test]
    fn test_nullable_read_empty_page() {
        let page = NullableI64Page::new();
        assert_eq!(page.read(0).unwrap_err(), PageError::IndexOutOfBounds(0));
    }

    // Clone

    #[test]
    fn test_nullable_clone_is_independent() {
        let mut page = NullableI64Page::new();
        page.write(Some(10)).unwrap();
        page.write(None).unwrap();

        let mut cloned = page.clone();
        cloned.write(Some(30)).unwrap();

        // Original unchanged
        assert_eq!(page.len(), 2);
        assert_eq!(page.read(0).unwrap(), Some(10));
        assert_eq!(page.read(1).unwrap(), None);

        // Clone has the new write
        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.read(2).unwrap(), Some(30));
    }
}
