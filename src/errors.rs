use crate::page::PageError;
use std::fmt;
use crate::bufferpool::BufferPoolError;

#[derive(Debug)]
pub enum DbError {
    Page(PageError),
    Storage(BufferPoolError),
    RecordNotFound(i64), // No such RID
    KeyNotFound(i64),    // Index look up return ()
    DuplicateKey(i64),   // Insertion is done with duplicate primary key
    NullValue(usize),    // Column was None when value is expected
    WriteTableFailed,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Page(e) => write!(f, "page error: {:?}", e),
            DbError::Storage(e) => write!(f, "storage error: {:?}", e),
            DbError::RecordNotFound(rid) => write!(f, "record not found: RID {}", rid),
            DbError::KeyNotFound(key) => write!(f, "key not found: {}", key),
            DbError::DuplicateKey(key) => write!(f, "duplicate key: {}", key),
            DbError::NullValue(col) => write!(f, "unexpected null in column {}", col),
            DbError::WriteTableFailed => write!(f, "write table failed"),
        }
    }
}

impl From<PageError> for DbError {
    fn from(e: PageError) -> Self {
        DbError::Page(e)
    }
}

impl From<BufferPoolError> for DbError {
    fn from(e: BufferPoolError) -> Self {
        DbError::Storage(e)
    }
}
