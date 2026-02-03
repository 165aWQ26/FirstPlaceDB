const MAX_RECORDS: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    I64(i64),
    F64(f64),
    Null,
    // TODO: String support - not Copy, will need special handling
}

#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
    TypeMismatch { expected: ValueType, got: ValueType },
    NullViolation,
}

/// Represents the expected type of a column, used for type checking at write time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    I64,
    F64,
    // TODO: String
}

impl Value {
    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            Value::I64(_) => Some(ValueType::I64),
            Value::F64(_) => Some(ValueType::F64),
            Value::Null => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Page {
    data: Vec<Value>,
    nullable: bool,
    expected_type: ValueType,
}

impl Page {
    pub fn new(expected_type: ValueType, nullable: bool) -> Self {
        Self {
            data: Vec::with_capacity(MAX_RECORDS),
            nullable,
            expected_type,
        }
    }

    pub fn with_capacity(capacity: usize, expected_type: ValueType, nullable: bool) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            nullable,
            expected_type,
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.data.len() < MAX_RECORDS
    }

    pub fn write(&mut self, val: Value) -> Result<(), PageError> {
        if !self.has_capacity() {
            return Err(PageError::Full);
        }

        if val == Value::Null {
            if !self.nullable {
                return Err(PageError::NullViolation);
            }
        } else if let Some(val_type) = val.value_type() {
            if val_type != self.expected_type {
                return Err(PageError::TypeMismatch {
                    expected: self.expected_type,
                    got: val_type,
                });
            }
        }

        self.data.push(val);
        Ok(())
    }

    pub fn read(&self, index: usize) -> Result<Value, PageError> {
        self.data
            .get(index)
            .copied()
            .ok_or(PageError::IndexOutOfBounds(index))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn expected_type(&self) -> ValueType {
        self.expected_type
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(ValueType::I64, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic

    #[test]
    fn test_new_page_is_empty() {
        let page = Page::new(ValueType::I64, false);
        assert_eq!(page.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let page = Page::with_capacity(64, ValueType::I64, false);
        assert_eq!(page.len(), 0);
    }

    #[test]
    fn test_default() {
        let page = Page::default();
        assert_eq!(page.len(), 0);
        assert!(!page.nullable());
        assert_eq!(page.expected_type(), ValueType::I64);
    }

    #[test]
    fn test_nullable_and_expected_type() {
        let page = Page::new(ValueType::F64, true);
        assert!(page.nullable());
        assert_eq!(page.expected_type(), ValueType::F64);
    }

    // Capacity

    #[test]
    fn test_has_capacity_when_empty() {
        let page = Page::new(ValueType::I64, false);
        assert!(page.has_capacity());
    }

    #[test]
    fn test_has_capacity_transitions() {
        let mut page = Page::new(ValueType::I64, false);
        for _ in 0..MAX_RECORDS - 1 {
            assert!(page.has_capacity());
            page.write(Value::I64(0)).unwrap();
        }
        assert!(page.has_capacity());
        page.write(Value::I64(0)).unwrap();
        assert!(!page.has_capacity());
    }

    // Write

    #[test]
    fn test_write_single_value() {
        let mut page = Page::new(ValueType::I64, false);
        let result = page.write(Value::I64(42));
        assert!(result.is_ok());
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), Value::I64(42));
    }

    #[test]
    fn test_write_multiple_values() {
        let mut page = Page::new(ValueType::I64, false);
        page.write(Value::I64(10)).unwrap();
        page.write(Value::I64(20)).unwrap();
        page.write(Value::I64(30)).unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(page.read(0).unwrap(), Value::I64(10));
        assert_eq!(page.read(1).unwrap(), Value::I64(20));
        assert_eq!(page.read(2).unwrap(), Value::I64(30));
    }

    #[test]
    fn test_write_until_full() {
        let mut page = Page::new(ValueType::I64, false);
        for i in 0..MAX_RECORDS {
            assert!(page.has_capacity());
            assert!(page.write(Value::I64(i as i64)).is_ok());
        }
        assert_eq!(page.len(), MAX_RECORDS);
        assert!(!page.has_capacity());
    }

    #[test]
    fn test_write_beyond_capacity_fails() {
        let mut page = Page::new(ValueType::I64, false);
        for i in 0..MAX_RECORDS {
            page.write(Value::I64(i as i64)).unwrap();
        }
        assert_eq!(
            page.write(Value::I64(999)).unwrap_err(),
            PageError::Full
        );
    }

    #[test]
    fn test_write_negative_values() {
        let mut page = Page::new(ValueType::I64, false);
        page.write(Value::I64(-1)).unwrap();
        page.write(Value::I64(-100)).unwrap();
        assert_eq!(page.read(0).unwrap(), Value::I64(-1));
        assert_eq!(page.read(1).unwrap(), Value::I64(-100));
    }

    #[test]
    fn test_write_boundary_values() {
        let mut page = Page::new(ValueType::I64, false);
        page.write(Value::I64(i64::MAX)).unwrap();
        page.write(Value::I64(i64::MIN)).unwrap();
        page.write(Value::I64(0)).unwrap();
        assert_eq!(page.read(0).unwrap(), Value::I64(i64::MAX));
        assert_eq!(page.read(1).unwrap(), Value::I64(i64::MIN));
        assert_eq!(page.read(2).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_write_f64() {
        let mut page = Page::new(ValueType::F64, false);
        page.write(Value::F64(3.14)).unwrap();
        assert_eq!(page.read(0).unwrap(), Value::F64(3.14));
    }

    // Write Errors

    #[test]
    fn test_write_null_to_non_nullable_fails() {
        let mut page = Page::new(ValueType::I64, false);
        assert_eq!(
            page.write(Value::Null).unwrap_err(),
            PageError::NullViolation
        );
    }

    #[test]
    fn test_write_type_mismatch_i64_to_f64_fails() {
        let mut page = Page::new(ValueType::F64, false);
        assert_eq!(
            page.write(Value::I64(42)).unwrap_err(),
            PageError::TypeMismatch {
                expected: ValueType::F64,
                got: ValueType::I64,
            }
        );
    }

    #[test]
    fn test_write_type_mismatch_f64_to_i64_fails() {
        let mut page = Page::new(ValueType::I64, false);
        assert_eq!(
            page.write(Value::F64(3.14)).unwrap_err(),
            PageError::TypeMismatch {
                expected: ValueType::I64,
                got: ValueType::F64,
            }
        );
    }

    #[test]
    fn test_write_beyond_capacity_fails_for_null() {
        let mut page = Page::new(ValueType::I64, true);
        for i in 0..MAX_RECORDS {
            page.write(Value::I64(i as i64)).unwrap();
        }
        assert_eq!(
            page.write(Value::Null).unwrap_err(),
            PageError::Full
        );
    }

    // Nullable Write

    #[test]
    fn test_write_null_to_nullable() {
        let mut page = Page::new(ValueType::I64, true);
        page.write(Value::Null).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page.read(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_write_mixed_nullable() {
        let mut page = Page::new(ValueType::I64, true);
        page.write(Value::I64(10)).unwrap();
        page.write(Value::Null).unwrap();
        page.write(Value::I64(30)).unwrap();
        page.write(Value::Null).unwrap();
        assert_eq!(page.read(0).unwrap(), Value::I64(10));
        assert_eq!(page.read(1).unwrap(), Value::Null);
        assert_eq!(page.read(2).unwrap(), Value::I64(30));
        assert_eq!(page.read(3).unwrap(), Value::Null);
    }

    #[test]
    fn test_nullable_write_until_full() {
        let mut page = Page::new(ValueType::I64, true);
        for i in 0..MAX_RECORDS {
            let val = if i % 2 == 0 {
                Value::I64(i as i64)
            } else {
                Value::Null
            };
            assert!(page.write(val).is_ok());
        }
        assert_eq!(page.len(), MAX_RECORDS);
        assert!(!page.has_capacity());
    }

    // Read

    #[test]
    fn test_read_empty_page() {
        let page = Page::new(ValueType::I64, false);
        assert_eq!(
            page.read(0).unwrap_err(),
            PageError::IndexOutOfBounds(0)
        );
    }

    #[test]
    fn test_read_invalid_index() {
        let mut page = Page::new(ValueType::I64, false);
        page.write(Value::I64(42)).unwrap();
        assert_eq!(
            page.read(1).unwrap_err(),
            PageError::IndexOutOfBounds(1)
        );
        assert_eq!(
            page.read(100).unwrap_err(),
            PageError::IndexOutOfBounds(100)
        );
    }

    #[test]
    fn test_read_last_valid_index() {
        let mut page = Page::new(ValueType::I64, false);
        for i in 0..10 {
            page.write(Value::I64(i)).unwrap();
        }
        assert_eq!(page.read(9).unwrap(), Value::I64(9));
        assert_eq!(
            page.read(10).unwrap_err(),
            PageError::IndexOutOfBounds(10)
        );
    }

    // Len

    #[test]
    fn test_len_updates_correctly() {
        let mut page = Page::new(ValueType::I64, false);
        assert_eq!(page.len(), 0);
        page.write(Value::I64(1)).unwrap();
        assert_eq!(page.len(), 1);
        page.write(Value::I64(2)).unwrap();
        assert_eq!(page.len(), 2);
    }

    // Clone

    #[test]
    fn test_clone_is_independent() {
        let mut page = Page::new(ValueType::I64, false);
        page.write(Value::I64(10)).unwrap();
        page.write(Value::I64(20)).unwrap();

        let mut cloned = page.clone();
        cloned.write(Value::I64(30)).unwrap();

        // Original unchanged
        assert_eq!(page.len(), 2);
        assert_eq!(page.read(0).unwrap(), Value::I64(10));
        assert_eq!(page.read(1).unwrap(), Value::I64(20));

        // Clone has the new write
        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.read(2).unwrap(), Value::I64(30));
    }

    #[test]
    fn test_nullable_clone_is_independent() {
        let mut page = Page::new(ValueType::I64, true);
        page.write(Value::I64(10)).unwrap();
        page.write(Value::Null).unwrap();

        let mut cloned = page.clone();
        cloned.write(Value::I64(30)).unwrap();

        // Original unchanged
        assert_eq!(page.len(), 2);
        assert_eq!(page.read(0).unwrap(), Value::I64(10));
        assert_eq!(page.read(1).unwrap(), Value::Null);

        // Clone has the new write
        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.read(2).unwrap(), Value::I64(30));
    }
}