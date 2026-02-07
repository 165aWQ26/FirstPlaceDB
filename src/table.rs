use rustc_hash::FxHashMap;

use crate::page::Page;

#[derive(Clone, Copy, Debug)]
pub struct Address {
    /// Which PageRange this record belongs to.
    pub range: usize,
    /// Which page within that PageRange holds this record
    pub page: usize,
    /// The row position within that page
    pub offset: usize,
}

#[derive(Copy, Clone, Debug)]
pub struct Record {
    /// Identifies the record. Used in PageDirectory to get the address.
    pub rid: usize,
    /// Points to the actual page. Read/writing is done though buffer pool.
    pub address: Address,
}

pub struct PageDirectory {
    /// RID -> Address
    pub directory: FxHashMap<i64, Address>
}

pub struct Table {
    /// Name of the table
    pub name: String,
    /// Total number of columns (including metadata columns)
    pub num_columns: usize,
    /// Index into the primary key column for client records.
    pub key_column: usize,
    /// The columns for this table, each stored as its own Page.
    /// To get the page from record, use the offset
    pub columns: Vec<Page>,
    /// The PageDirectory for this table
    pub page_directory: PageDirectory,
}