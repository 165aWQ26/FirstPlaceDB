use std::fs::File;
use std::io;
use std::io::BufWriter;
use crate::bufferpool::{BufferPool, BufferPoolError, MetaPage};
use crate::error::DbError;
use crate::index::Index;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::page::PageError;

#[derive(Debug)]
pub enum TableError {
    InvalidPath,
}

#[derive(Clone)]
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
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 4;
    //data_pages_per_collection is the total number of pages in a PageDirectory
    pub fn new(table_name: String, num_columns: usize, key_index: usize, bufferpool: Arc<RwLock<BufferPool>>) -> Table {
        //! Assume that we can only make one table for now. Bufferpool can't do more than one table.
        //! Also the bufferpool reference allocation related to table should be done in db.create_table.
        Self {
            name: table_name,
            // Make new copy for PageRanges to use
            page_ranges: PageRanges::new(num_columns, bufferpool),

            // original copy here
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index,
            num_columns,
            indices: (0..1).map(|_| Index::new()).collect(),
        }
    }
    /// Returns all the columns of the record
    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read(&addr)
    }

    pub fn write_i64(&self, val: i64, writer: &mut BufWriter<File>) -> Result<(), TableError> {

    }

    pub fn write_vector(&self, writer: &mut BufWriter<File>) -> Result<(), TableError> {

    }

    pub fn write_table_data_to_disk(&self, path: &str) -> Result<(), TableError>{
        let mut file_path : String = directory_path.clone();
        file_path.push_str("table_data");

        let file = File::create(&file_path).map_err(|_| TableError::InvalidPath)?;

        let mut writer = BufWriter::new(file);

        self.write_i64(self.num_columns, &mut writer);

        self.write_i64(self.key_index, &mut writer);

        self.write_vector(self.page_directory, &mut writer);

        return Ok(());
    }


    /// Like read but you choose col
    pub fn read_single(
        &self,
        rid: i64,
        column: usize,
        range: WhichRange,
    ) -> Result<Option<i64>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read_single(column, &addr, range)
    }

    //Use index to find the rid
    pub fn rid_for_key(&self, key: i64) -> Result<i64, DbError> {
        self.indices[self.key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))
    }

    // pub fn read_projected(&self, projected: &[i64], rid: i64) -> Result<Vec<Option<i64>>, DbError> {
    //     let addr = self.page_directory.get(rid)?;
    //     self.page_ranges.read_projected(projected, &addr)
    // }

    pub fn read_latest(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let mut result = self.read(rid)?;

        //Read indirection column
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        //If no tail updates, return base values
        if indirection.is_none() || indirection == Some(rid) {
            return Ok(result);
        }

        //Tail exists, walk the chain
        let mut current_tail_rid = indirection.unwrap();
        let mut accumulated_schema: i64 = 0;

        loop {
            let tail_addr = self.page_directory.get(current_tail_rid)?;
            let tail_schema = self
                .page_ranges
                .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                .unwrap_or(0); // None = deletion tail, no columns updated

            //Columns updated in this tail but not yet seen in newer tails
            let new_cols = tail_schema & !accumulated_schema;

            for col in 0..self.num_columns {
                if (new_cols >> col) & 1 == 1 {
                    result[col] =
                        self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail)?;
                }
            }

            accumulated_schema |= tail_schema;

            //Move to next (older) tail record
            let next_rid = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::IndirectionCol,
                WhichRange::Tail,
            )?;
            if let Some(next) = next_rid {
                if next == rid {
                    break; //reached base
                }
                current_tail_rid = next; //continue down the chain
            } else {
                break; //no more tails
            }
        }

        Ok(result)
    }

    pub fn read_latest_single(&self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        if let Some(tail_rid) = indirection && tail_rid != rid {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0); //None = deletion tail, no columns updated

                if (tail_schema >> col) & 1 == 1 {
                    return self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    &tail_addr,
                    MetaPage::IndirectionCol,
                    WhichRange::Tail,
                )?;
                if let Some(next) = next_rid {
                    if next == rid {
                        break;
                    }
                    current_tail_rid = next;
                } else {
                    break;
                }
            }
        }
        self.page_ranges.read_single(col, &base_addr, WhichRange::Base)
    }

    pub fn read_version_single(
        &self,
        rid: i64,
        col: usize,
        mut relative_version: i64,
    ) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;
        if let Some(tail_rid) = indirection && tail_rid != rid  {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0); // None = deletion tail, no columns updated

                //Case where latest update if found
                if (tail_schema >> col) & 1 == 1 {
                    // Newest tail that updates this column
                    relative_version += 1;
                }
                if relative_version > 0 {
                    return self
                        .page_ranges
                        .read_single(col, &tail_addr, WhichRange::Tail);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    &tail_addr,
                    MetaPage::IndirectionCol,
                    WhichRange::Tail,
                )?;
                if let Some(next) = next_rid {
                    if next == rid {
                        break;
                    }
                    current_tail_rid = next;
                } else {
                    break;
                }
            }
        }
        self.page_ranges
            .read_single(col, &base_addr, WhichRange::Base)
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

    pub fn read_version_projected(
        &self,
        projected: &[i64],
        rid: i64,
        relative_version: i64,
    ) -> Result<Vec<Option<i64>>, DbError> {
        let mut ans: Vec<Option<i64>> = Vec::new();
        for (col, value) in projected.iter().enumerate() {
            if *value == 1 {
                ans.push(self.read_version_single(rid, col, relative_version)?);
            } else {
                ans.push(None);
            }
        }
        Ok(ans)
    }

    /// Check if a base RID's latest tail has schema_encoding == None (deletion marker).
    pub fn is_deleted(&self, rid: i64) -> Result<bool, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        if indirection.is_none() || indirection == Some(rid) {
            return Ok(false);
        }

        //Tail exists
        let tail_rid = indirection.unwrap();
        let tail_addr = self.page_directory.get(tail_rid)?;
        let schema = self.page_ranges.read_meta_col(
            &tail_addr,
            MetaPage::SchemaEncodingCol,
            WhichRange::Tail,
        )?;

        Ok(schema.is_none())
    }

    // TODO do for m2 -- what the helly do these do
    // pub fn create_index() {

    // }

    // pub fn drop_index() {

    // }
}
