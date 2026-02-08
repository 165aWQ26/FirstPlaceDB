use crate::page::{Page, PageError};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DbError {
    Page(PageError),
    RecordNotFound(i64), // No such RID
    KeyNotFound(i64),    // Index look up return ()
    DuplicateKey(i64),   // Insertion is done with duplicate primary key
    NullValue(usize),    // Column was None when value is expected
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Page(e) => write!(f, "page error: {:?}", e),
            DbError::RecordNotFound(rid) => write!(f, "record not found: RID {}", rid),
            DbError::KeyNotFound(key) => write!(f, "key not found: {}", key),
            DbError::DuplicateKey(key) => write!(f, "duplicate key: {}", key),
            DbError::NullValue(col) => write!(f, "unexpected null in column {}", col),

        }
    }
}

impl From<PageError> for DbError {
    fn from(e: PageError) -> Self {
        DbError::Page(e)
    }
}