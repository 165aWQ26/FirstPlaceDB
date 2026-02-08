use crate::page::PageError;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DbError {
    Page(PageError),
    RecordNotFound(i64), // No such RID
    KeyNotFound(i64),    // Index look up return ()
    DuplicateKey(i64),   // Insertion is done with duplicate primary key
    NullValue(usize),    // Column was None when value is expected
}