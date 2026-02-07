
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    I64(i64),
    F64(f64),
    None,
    // TODO: String support - not Copy, will need special handling
}

#[derive(Debug, Clone, PartialEq)]
pub enum PageError {
    Full,
    IndexOutOfBounds(usize),
    TypeMismatch { expected: ValueType, got: ValueType },
    NullViolation,
}

// Represents the expected type of a column, used for type checking at write time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    I64,
    F64,
    None,
    // TODO: String
}

impl Value {
    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            Value::I64(_) => Some(ValueType::I64),
            Value::F64(_) => Some(ValueType::F64),
            Value::None => None,
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
    const DEFAULT_SIZE: usize = 512;

    fn new_internal(expected_type: ValueType, nullable: bool, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            nullable,
            expected_type
        }
    }

    pub fn new_f64() -> Self {
        Self::new_internal(ValueType::F64, false, Self::DEFAULT_SIZE)
    }

    pub fn new() -> Self {
        Self::new_internal(ValueType::I64, false, Self::DEFAULT_SIZE)
    }

    pub fn new_nullable() -> Self {
        Self::new_internal(ValueType::I64, true, Self::DEFAULT_SIZE)
    }

    pub fn new_nullable_f64() -> Self {
        Self::new_internal(ValueType::F64, true, Self::DEFAULT_SIZE)
    }

    pub fn has_capacity(&self) -> bool {
        self.data.len() < self.data.capacity()
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn write(&mut self, val: Value) -> Result<(), PageError> {
        if !self.has_capacity() {
            return Err(PageError::Full);
        }

        if val == Value::None {
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
        Self::new()
    }
}
