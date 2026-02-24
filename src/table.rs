use crate::bufferpool::{BufferPool, MetaPage};
use crate::bufferpool_context::{PageLocation, TableContext};
use crate::db_error::DbError;
use crate::index::Index;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

#[derive(Debug)]
pub enum TableError {
    InvalidPath,
    WriteFail,
    ReadFail,
}

#[derive(Clone)]
pub struct Table {
    pub page_ranges: PageRanges,

    pub bufferpool: Arc<Mutex<BufferPool>>,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<i64>,

    pub key_index: usize,

    pub indices: Vec<Index>,

    pub table_ctx: TableContext,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 4;
    //data_pages_per_collection is the total number of pages in a PageDirectory
    pub fn new(
        table_path: String,
        num_columns: usize,
        key_index: usize,
        bufferpool: Arc<Mutex<BufferPool>>,
        table_id: usize,
    ) -> Table {
        //! Assume that we can only make one table for now. Bufferpool can't do more than one table.
        //! Also the bufferpool reference allocation related to table should be done in db.create_table.
        Self {
            // Make new copy for PageRanges to use
            bufferpool: Arc::clone(&bufferpool),

            page_ranges: PageRanges::new(bufferpool),

            // original copy here
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index,
            indices: (0..1).map(|_| Index::new()).collect(),
            table_ctx: TableContext {
                table_id,
                total_cols: num_columns + Table::NUM_META_PAGES, //Todo IMPORTANT MAKE SURE THIS IS ACCOUNTED FOR
                path: table_path,
            },
        }
    }
    /// Returns all the columns of the record
    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read(&addr, &self.table_ctx)
    }

    pub fn write_page_directory(&self, writer: &mut BufWriter<File>) -> Result<(), TableError> {
        self.page_directory
            .write_to_disk(writer, Arc::clone(&self.bufferpool))
    }

    pub fn write_to_disk(&self, path: String) -> Result<(), TableError> {
        let mut file_path: String = path;
        file_path.push_str("table_data");

        let file = File::create(&file_path).map_err(|_| TableError::InvalidPath)?;

        let mut writer = BufWriter::new(file);

        self.bufferpool
            .lock()
            .write_i64(self.table_ctx.total_cols as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        self.bufferpool
            .lock()
            .write_i64(self.key_index as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist RID counter (current value of the RangeFrom iterator)
        self.bufferpool
            .lock()
            .write_i64(self.rid.start, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist base page range iterator position
        let (base_off, base_col) = self.page_ranges.base.position();
        self.bufferpool
            .lock()
            .write_i64(base_off as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        self.bufferpool
            .lock()
            .write_i64(base_col as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist tail page range iterator position
        let (tail_off, tail_col) = self.page_ranges.tail.position();
        self.bufferpool
            .lock()
            .write_i64(tail_off as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        self.bufferpool
            .lock()
            .write_i64(tail_col as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist primary key index
        let index = &self.indices[self.key_index];
        let entries: Vec<_> = index.iter().collect();
        self.bufferpool
            .lock()
            .write_i64(entries.len() as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        for (key, rid) in &entries {
            self.bufferpool
                .lock()
                .write_i64(**key, &mut writer)
                .map_err(|_| TableError::WriteFail)?;
            self.bufferpool
                .lock()
                .write_i64(**rid, &mut writer)
                .map_err(|_| TableError::WriteFail)?;
        }

        self.write_page_directory(&mut writer)?;

        Ok(())
    }

    pub fn read_from_disk(&mut self, path: String) -> Result<(), TableError> {
        let mut file_path: String = path.clone();
        file_path.push_str("table_data");

        let file = File::open(&file_path).map_err(|_| TableError::InvalidPath)?;

        let mut reader = BufReader::new(file);

        let mut buffer = [0u8; 8];

        //Todo: Make sure this returns total num cols including metacols!
        self.table_ctx.total_cols = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

        self.key_index = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

        // Restore RID counter
        let rid_start = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)? as i64;
        self.rid = rid_start..;

        // Restore base page range iterator position
        let base_off = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        let base_col = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        self.page_ranges.base.set_position(base_off, base_col);

        // Restore tail page range iterator position
        let tail_off = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        let tail_col = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        self.page_ranges.tail.set_position(tail_off, tail_col);

        // Restore primary key index
        let index_count = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        for _ in 0..index_count {
            let key = self
                .bufferpool
                .lock()
                .read_usize(&mut buffer, &mut reader)
                .map_err(|_| TableError::ReadFail)? as i64;
            let rid = self
                .bufferpool
                .lock()
                .read_usize(&mut buffer, &mut reader)
                .map_err(|_| TableError::ReadFail)? as i64;
            self.indices[self.key_index].insert(key, rid);
        }

        self.page_directory
            .read_from_disk(&mut buffer, &mut reader, self.bufferpool.clone())?;

        Ok(())
    }

    /// Like read but you choose col
    pub fn read_single(
        &mut self,
        rid: i64,
        column: usize,
        range: WhichRange,
    ) -> Result<Option<i64>, DbError> {
        let page_location = PageLocation::new(self.page_directory.get(rid)?, range);
        self.page_ranges
            .read_single(column, &page_location, &self.table_ctx)
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

    pub fn read_latest(&mut self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let mut result = self.read(rid)?;

        //Read indirection column
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::IndirectionCol,
            &base_location,
            &self.table_ctx,
        )?;

        //If no tail updates, return base values
        if indirection.is_none() || indirection == Some(rid) {
            return Ok(result);
        }

        //Tail exists, walk the chain
        let mut current_tail_rid = indirection.unwrap();
        let mut accumulated_schema: i64 = 0;

        loop {
            let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
            let tail_schema = self
                .page_ranges
                .read_meta_col(MetaPage::SchemaEncodingCol, &tail_location, &self.table_ctx)?
                .unwrap_or(0); // None = deletion tail, no columns updated

            //Columns updated in this tail but not yet seen in newer tails
            let new_cols = tail_schema & !accumulated_schema;

            for (col, val) in result
                .iter_mut()
                .enumerate()
                .take(self.table_ctx.total_cols - Table::NUM_META_PAGES)
            {
                // val = self.page_ranges.read_single(col, &tail_location, &self.table_ctx);
                if (new_cols >> col) & 1 == 1 {
                    *val = self
                        .page_ranges
                        .read_single(col, &tail_location, &self.table_ctx)?
                }
            }

            accumulated_schema |= tail_schema;

            //Move to next (older) tail record
            let next_rid = self.page_ranges.read_meta_col(
                MetaPage::IndirectionCol,
                &tail_location,
                &self.table_ctx,
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

    pub fn read_latest_single(&mut self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::IndirectionCol,
            &base_location,
            &self.table_ctx,
        )?;

        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(MetaPage::SchemaEncodingCol, &tail_location, &self.table_ctx)?
                    .unwrap_or(0); //None = deletion tail, no columns updated

                if (tail_schema >> col) & 1 == 1 {
                    return self
                        .page_ranges
                        .read_single(col, &tail_location, &self.table_ctx);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    MetaPage::IndirectionCol,
                    &tail_location,
                    &self.table_ctx,
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
            .read_single(col, &base_location, &self.table_ctx)
    }

    pub fn read_version_single(
        &mut self,
        rid: i64,
        col: usize,
        mut relative_version: i64,
    ) -> Result<Option<i64>, DbError> {
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::IndirectionCol,
            &base_location,
            &self.table_ctx,
        )?;
        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(MetaPage::SchemaEncodingCol, &tail_location, &self.table_ctx)?
                    .unwrap_or(0); // None = deletion tail, no columns updated

                //Case where latest update if found
                if (tail_schema >> col) & 1 == 1 {
                    // Newest tail that updates this column
                    relative_version += 1;
                }
                if relative_version > 0 {
                    return self
                        .page_ranges
                        .read_single(col, &tail_location, &self.table_ctx);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    MetaPage::IndirectionCol,
                    &tail_location,
                    &self.table_ctx,
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
            .read_single(col, &base_location, &self.table_ctx)
    }

    pub fn read_latest_projected(
        &mut self,
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
        &mut self,
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
    pub fn is_deleted(&mut self, rid: i64) -> Result<bool, DbError> {
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::IndirectionCol,
            &base_location,
            &self.table_ctx,
        )?;

        if indirection.is_none() || indirection == Some(rid) {
            return Ok(false);
        }

        //Tail exists
        let tail_rid = indirection.unwrap();
        let tail_location = PageLocation::tail(self.page_directory.get(tail_rid)?);
        let schema = self.page_ranges.read_meta_col(
            MetaPage::SchemaEncodingCol,
            &tail_location,
            &self.table_ctx,
        )?;

        Ok(schema.is_none())
    }

    // TODO do for m2 -- what the helly do these do
    // pub fn create_index() {

    // }

    // pub fn drop_index() {

    // }
}
