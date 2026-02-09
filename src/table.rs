use crate::error::DbError;
use crate::index::Index;
use crate::page_collection::{PageCollection, SCHEMA_ENCODING_COL};
use crate::page_directory::PageDirectory;
use crate::page_range::PageRanges;

pub struct Table {
    pub name: String,

    pub page_ranges: PageRanges,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<i64>,

    pub num_columns: usize,

    pub key_index: usize,

    pub indices: Vec<Index>,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 1200;
    pub const NUM_META_PAGES: usize = 4;
    //data_pages_per_collection is the total number of pages in a PageDirectory
    pub fn new(
        table_name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Table {
        Self {
            name: table_name,
            page_ranges: PageRanges::new(num_columns),
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index,
            num_columns,
            indices: (0..num_columns).map(|_| Index::new()).collect(),
        }
    }
    /// Returns all the columns of the record
    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read(&addr)
    }

    /// Like read but you choose col
    pub fn read_single(&self, rid: i64, column: usize) -> Result<Option<i64>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read_single(column, &addr)
    }

    pub fn read_projected(&self, projected: &[i64], rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read_projected(projected, &addr)
    }

    pub fn read_latest(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let mut result = self.read(rid)?;
        let indirection = self.page_ranges.get_indirection(&base_addr)?;

        // If indirection is None, no updates
        match indirection {
            Some(ind_rid) if ind_rid == rid => return Ok(result),
            None => Ok(result),
            Some(tail_rid) => {
                let mut current_tail_rid = tail_rid;
                let mut accumulated_schema: i64 = 0;
                loop {
                    let tail_addr = self.page_directory.get(current_tail_rid)?;
                    let tail_schema = self
                        .page_ranges
                        .get_tail_schema_encoding(&tail_addr)?
                        .unwrap_or(0); // None = deletion tail, no columns updated

                    // Columns updated in this tail but not yet seen in newer tail
                    let new_cols = tail_schema & !accumulated_schema;

                    for col in 0..self.num_columns {
                        if (new_cols >> col) & 1 == 1 {
                            result[col] = self.page_ranges.read_tail_single(col, &tail_addr)?;
                        }
                    }
                    accumulated_schema |= tail_schema;

                    // Follow indirection to next (older tail record)
                    let next_rid = self.page_ranges.get_tail_indirection(&tail_addr)?;
                    match next_rid {
                        Some(next) if next == rid => break,
                        Some(next) => current_tail_rid = next,
                        None => break,
                    }
                }
                Ok(result)
            }
        }
    }

    pub fn read_latest_single(&self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        // ? What is this for?
        let mut result = self.read(rid)?;
        let indirection = self.page_ranges.get_indirection(&base_addr)?;

        match indirection {
            Some(ind_rid) if ind_rid == rid => self.page_ranges.read_single(col, &base_addr),
            None => self.page_ranges.read_single(col, &base_addr),
            Some(tail_rid) => {
                let mut current_tail_rid = tail_rid;
                loop {
                    let tail_addr = self.page_directory.get(current_tail_rid)?;
                    let tail_schema = self
                        .page_ranges
                        .get_tail_schema_encoding(&tail_addr)?
                        .unwrap_or(0); // None = deletion tail, no columns updated

                    if (tail_schema >> col) & 1 == 1 {
                        // Newest tail that updates this column
                        return self.page_ranges.read_tail_single(col, &tail_addr);
                    }

                    let next_rid = self.page_ranges.get_tail_indirection(&tail_addr)?;
                    match next_rid {
                        Some(next) if next == rid => break,
                        Some(next) => current_tail_rid = next,
                        None => break,
                    }
                }
                // Column never updated in any tail
                self.page_ranges.read_single(col, &base_addr)
            }
        }
    }

    pub fn read_latest_projected(
        &self,
        projected: &[i64],
        rid: i64,
    ) -> Result<Vec<Option<i64>>, DbError> {
        let full = self.read_latest(rid)?;
        Ok(projected
            .iter()
            .enumerate()
            .map(|(col, &flag)| if flag == 1 { full[col] } else { None })
            .collect())
    }

    /// Check if a base RID's latest tail has schema_encoding == None (deletion marker).
    pub fn is_deleted(&self, rid: i64) -> Result<bool, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.get_indirection(&base_addr)?;
        match indirection {
            None => Ok(false),
            Some(ind_rid) if ind_rid == rid => Ok(false),
            Some(tail_rid) => {
                let tail_addr = self.page_directory.get(tail_rid)?;
                let schema = self.page_ranges.get_tail_schema_encoding(&tail_addr)?;
                Ok(schema.is_none())
            }
        }
    }

    // TODO do for m2 -- what the helly do these do
    // pub fn create_index() {

    // }

    // pub fn drop_index() {

    // }
}
