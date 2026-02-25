use crate::bufferpool::{BufferPool, MetaPage};
use crate::bufferpool_context::{PageLocation, TableContext};
use crate::db_error::DbError;
use crate::index::Index;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use parking_lot::Mutex;
use std::collections::HashMap;
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

    pub tail_count: usize,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 5;
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
            // Make default copy for PageRanges to use
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
            tail_count: 0,
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

        // Persist tail_count
        self.bufferpool
            .lock()
            .write_i64(self.tail_count as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

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

        self.tail_count = self
            .bufferpool
            .lock()
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

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
            MetaPage::Indirection,
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
                .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
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
                MetaPage::Indirection,
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
            MetaPage::Indirection,
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
                    .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                    .unwrap_or(0); //None = deletion tail, no columns updated

                if (tail_schema >> col) & 1 == 1 {
                    return self
                        .page_ranges
                        .read_single(col, &tail_location, &self.table_ctx);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    MetaPage::Indirection,
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
        relative_version: i64,
    ) -> Result<Option<i64>, DbError> {
        // relative_version: 0 = latest, -1 = one update before latest, etc.
        // We traverse tail chain from newest to oldest.
        // Each tail that updates `col` counts as one version step.
        // We want the tail at position `relative_version` (0-indexed from latest).
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
            &base_location,
            &self.table_ctx,
        )?;
        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            // version_counter starts at 0 (latest) and decrements toward relative_version
            let mut version_counter: i64 = 0;
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                    .unwrap_or(0); // None = deletion tail, no columns updated

                if (tail_schema >> col) & 1 == 1 {
                    // This tail updates `col`
                    if version_counter == relative_version {
                        // This is the version we want
                        return self
                            .page_ranges
                            .read_single(col, &tail_location, &self.table_ctx);
                    }
                    version_counter -= 1;
                }

                let next_rid = self.page_ranges.read_meta_col(
                    MetaPage::Indirection,
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
        // Fell off the chain — return base value
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
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let tails_to_skip = (-relative_version).max(0) as usize;

        // Collect all tail RIDs newest → oldest
        let mut tail_rids: Vec<i64> = Vec::new();
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
            &base_location,
            &self.table_ctx,
        )?;
        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            let mut current = tail_rid;
            loop {
                tail_rids.push(current);
                let tail_location = PageLocation::tail(self.page_directory.get(current)?);
                let next = self.page_ranges.read_meta_col(
                    MetaPage::Indirection,
                    &tail_location,
                    &self.table_ctx,
                )?;
                match next {
                    Some(next_rid) if next_rid != rid => current = next_rid,
                    _ => break,
                }
            }
        }

        // Start with base record
        let mut result = self.page_ranges.read(&base_location.addr, &self.table_ctx)?;

        // Skip the newest `tails_to_skip` tails, then apply the rest newest → oldest
        let relevant = if tails_to_skip >= tail_rids.len() {
            &tail_rids[tail_rids.len()..] // empty — return base
        } else {
            &tail_rids[tails_to_skip..]
        };

        let mut accumulated_schema: i64 = 0;
        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;
        for &tail_rid in relevant {
            let tail_location = PageLocation::tail(self.page_directory.get(tail_rid)?);
            let tail_schema = self
                .page_ranges
                .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                .unwrap_or(0);
            let new_cols = tail_schema & !accumulated_schema;
            for col in 0..num_data_cols {
                if (new_cols >> col) & 1 == 1 {
                    result[col] = self
                        .page_ranges
                        .read_single(col, &tail_location, &self.table_ctx)?;
                }
            }
            accumulated_schema |= tail_schema;
        }

        Ok(projected
            .iter()
            .enumerate()
            .map(|(col, &flag)| if flag == 1 { result[col] } else { None })
            .collect())
    }

    /// Check if a base RID's latest tail has schema_encoding == None (deletion marker).
    pub fn is_deleted(&mut self, rid: i64) -> Result<bool, DbError> {
        let base_location = PageLocation::base(self.page_directory.get(rid)?);
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
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
            MetaPage::SchemaEncoding,
            &tail_location,
            &self.table_ctx,
        )?;

        Ok(schema.is_none())
    }

    pub fn merge(&mut self) -> Result<(), DbError> {
        let mut max_tail_per_collection: HashMap<usize, i64> = HashMap::default();

        let base_rids: Vec<i64> = self.indices[self.key_index]
            .iter()
            .map(|(_, &rid)| rid)
            .collect();

        for base_rid in base_rids {
            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };
            let base_location = PageLocation::base(base_addr);
            let indirection = match self.page_ranges.read_meta_col(
                MetaPage::Indirection,
                &base_location,
                &self.table_ctx,
            ) {
                Ok(Some(ind)) if ind != base_rid => ind,
                _ => continue,
            };

            let tail_rid = indirection;

            let tail_addr = match self.page_directory.get(tail_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            let tail_location = PageLocation::tail(tail_addr);

            let schema = match self.page_ranges.read_meta_col(
                MetaPage::SchemaEncoding,
                &tail_location,
                &self.table_ctx,
            ) {
                Ok(val) => val,
                Err(_) => continue,
            };

            if schema.is_none() {
                self.bufferpool.lock().update_meta_col(
                    Option::from(-1),
                    MetaPage::SchemaEncoding,
                    &base_location,
                    &self.table_ctx,
                )?;
            }

            max_tail_per_collection
                .entry(base_location.addr.collection_num)
                .and_modify(|v| *v = (*v).max(tail_rid))
                .or_insert(tail_rid);
        }

        // grow tps to match default range length if needed
        for (collection_num, max_rid) in max_tail_per_collection {
            if collection_num >= self.page_ranges.base.tps.len() {
                self.page_ranges
                    .base
                    .tps
                    .resize(collection_num + 1, i64::MAX);
            }
            self.page_ranges.base.tps[collection_num] = max_rid;
        }
        Ok(())
    }
}