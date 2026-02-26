use crate::bufferpool::{BufferPool, MetaPage};
use crate::bufferpool_context::{PageLocation, TableContext};
use crate::db_error::DbError;
use crate::index::{Index, NonUniqueIndex, TableIndex, UniqueIndex};
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

#[derive(Debug)]
pub enum TableError {
    InvalidPath,
    InvalidKeyIndex,
    WriteFail,
    ReadFail,
}

pub(crate) fn write_i64(
    val: impl Into<Option<i64>>,
    writer: &mut BufWriter<File>,
) -> Result<(), TableError> {
    const SENTINEL: i64 = i64::MAX;
    let bytes = match val.into() {
        Some(val) => val.to_be_bytes(),
        None => SENTINEL.to_be_bytes(),
    };
    writer.write_all(&bytes).map_err(|_| TableError::WriteFail)
}

pub(crate) fn read_i64(buf: &mut [u8; 8], reader: &mut BufReader<File>) -> Result<i64, TableError> {
    reader.read_exact(buf).map_err(|_| TableError::ReadFail)?;
    Ok(i64::from_be_bytes(*buf))
}

pub(crate) fn read_usize(
    buf: &mut [u8; 8],
    reader: &mut BufReader<File>,
) -> Result<usize, TableError> {
    Ok(read_i64(buf, reader)? as usize)
}

#[derive(Clone)]
pub struct Table {
    pub page_ranges: PageRanges,

    pub bufferpool: Arc<Mutex<BufferPool>>,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<i64>,

    pub key_index: usize,

    pub primary_index: UniqueIndex,

    pub indices: Vec<Index>,

    pub table_ctx: TableContext,
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
        let indices = (0..num_columns)
            .map(|col| -> Index {
                if col == key_index {
                    Index::Unique(UniqueIndex::new())
                } else {
                    Index::NonUnique(NonUniqueIndex::new())
                }
            })
            .collect();
        Self {
            // Make default copy for PageRanges to use
            bufferpool: Arc::clone(&bufferpool),

            page_ranges: PageRanges::new(bufferpool),

            // original copy here
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index,
            primary_index: UniqueIndex::new(),
            indices,
            table_ctx: TableContext {
                table_id,
                total_cols: num_columns + Table::NUM_META_PAGES,
                path: table_path,
            },
        }
    }

    pub fn get_index(&self, col_idx: usize) -> &dyn TableIndex {
        if col_idx == self.key_index {
            &self.primary_index
        } else {
            &self.indices[col_idx]
        }
    }

    pub fn get_index_mut(&mut self, col_idx: usize) -> &mut dyn TableIndex {
        if col_idx == self.key_index {
            &mut self.primary_index
        } else {
            &mut self.indices[col_idx]
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

        let bp_lock = self.bufferpool.lock();

        bp_lock
            .write_i64(self.table_ctx.total_cols as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        bp_lock
            .write_i64(self.key_index as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist RID counter (current value of the RangeFrom iterator)
        bp_lock
            .write_i64(self.rid.start, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist base page range iterator position
        let (base_off, base_col) = self.page_ranges.base.position();
        bp_lock
            .write_i64(base_off as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        bp_lock
            .write_i64(base_col as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        // Persist tail page range iterator position
        let (tail_off, tail_col) = self.page_ranges.tail.position();
        bp_lock
            .write_i64(tail_off as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        bp_lock
            .write_i64(tail_col as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;

        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;
        for i in 0..num_data_cols {
            let index = self.get_index(i);
            let count = index.len() as i64;

            bp_lock
                .write_i64(count, &mut writer)
                .map_err(|_| TableError::WriteFail)?;

            if i == self.key_index {
                for (k, r) in self.primary_index.iter() {
                    bp_lock
                        .write_i64(k, &mut writer)
                        .map_err(|_| TableError::WriteFail)?;
                    bp_lock
                        .write_i64(r, &mut writer)
                        .map_err(|_| TableError::WriteFail)?;
                }
            } else {
                // Pattern matching here just for posterity
                if let Index::NonUnique(s) = &self.indices[i] {
                    for (k, rids) in s.iter_raw() {
                        bp_lock
                            .write_i64(*k, &mut writer)
                            .map_err(|_| TableError::WriteFail)?;
                        bp_lock
                            .write_i64(rids.len() as i64, &mut writer)
                            .map_err(|_| TableError::WriteFail)?;
                        for rid in rids {
                            bp_lock
                                .write_i64(*rid, &mut writer)
                                .map_err(|_| TableError::WriteFail)?;
                        }
                    }
                }
            }
        }

        self.write_page_directory(&mut writer)?;

        //Persist TPS
        let tps = &self.page_ranges.base.tps;
        bp_lock
            .write_i64(tps.len() as i64, &mut writer)
            .map_err(|_| TableError::WriteFail)?;
        for &watermark in tps {
            self.bufferpool
                .lock()
                .write_i64(watermark, &mut writer)
                .map_err(|_| TableError::WriteFail)?;
        }

        Ok(())
    }

    pub fn read_from_disk(&mut self, path: String) -> Result<(), TableError> {
        let mut file_path: String = path.clone();
        file_path.push_str("table_data");

        let file = File::open(&file_path).map_err(|_| TableError::InvalidPath)?;

        let mut reader = BufReader::new(file);

        let mut buffer = [0u8; 8];

        let mut bp_lock = self.bufferpool.lock();

        //Todo: Make sure this returns total num cols including metacols!
        self.table_ctx.total_cols = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

        self.key_index = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

        let num_data_cols = self.table_ctx.total_cols - Self::NUM_META_PAGES;

        self.primary_index = UniqueIndex::new(); // Reset primary
        self.indices = (0..num_data_cols)
            .map(|col| {
                if col == self.key_index {
                    Index::NonUnique(NonUniqueIndex::new())
                } else {
                    Index::NonUnique(NonUniqueIndex::new())
                }
            })
            .collect();

        let rid_start = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)? as i64;
        self.rid = rid_start..;

        for i in 0..num_data_cols {
            let index_count = self
                .bufferpool
                .lock()
                .read_usize(&mut buffer, &mut reader)
                .map_err(|_| TableError::ReadFail)?;

            if i == self.key_index {
                // Populate Primary Index
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
                    self.primary_index.insert_unique(key, rid);
                }
            } else {
                // Populate Secondary Index
                // We know it is NonUnique based on our initialization logic
                if let Index::NonUnique(s) = &mut self.indices[i] {
                    for _ in 0..index_count {
                        let key = self
                            .bufferpool
                            .lock()
                            .read_usize(&mut buffer, &mut reader)
                            .map_err(|_| TableError::ReadFail)?
                            as i64;
                        let num_rids = self
                            .bufferpool
                            .lock()
                            .read_usize(&mut buffer, &mut reader)
                            .map_err(|_| TableError::ReadFail)?
                            as i64;
                        for _ in 0..num_rids {
                            let rid = self
                                .bufferpool
                                .lock()
                                .read_usize(&mut buffer, &mut reader)
                                .map_err(|_| TableError::ReadFail)?
                                as i64;
                            s.insert(key, rid);
                        }
                    }
                }
            }
        }

        // Restore RID counter
        let rid_start = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)? as i64;
        self.rid = rid_start..;

        // Restore base page range iterator position
        let base_off = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        let base_col = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        self.page_ranges.base.set_position(base_off, base_col);

        // Restore tail page range iterator position
        let tail_off = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        let tail_col = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;
        self.page_ranges.tail.set_position(tail_off, tail_col);

        self.page_directory
            .read_from_disk(&mut buffer, &mut reader, self.bufferpool.clone())?;

        // Restore TPS watermarks
        let tps_len = bp_lock
            .read_usize(&mut buffer, &mut reader)
            .map_err(|_| TableError::ReadFail)?;

        self.page_ranges.base.tps = Vec::with_capacity(tps_len);
        for _ in 0..tps_len {
            let watermark = self
                .bufferpool
                .lock()
                .read_usize(&mut buffer, &mut reader)
                .map_err(|_| TableError::ReadFail)? as i64;
            self.page_ranges.base.tps.push(watermark);
        }

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

    /// Locate secondary index val
    pub fn locate(&self, key: i64, col_index: usize) -> Result<Vec<i64>, DbError> {
        let rids = self.get_index(col_index).locate(key);
        if rids.is_empty() {
            Err(DbError::KeyNotFound(key))
        } else {
            Ok(rids)
        }
    }

    /// Locate RID
    pub fn locate_primary(&self, key: i64) -> Result<i64, DbError> {
        // We know key_index returns at most 1 item
        self.locate(key, self.key_index).map(|mut v| v.remove(0))
    }

    // pub fn read_projected(&self, projected: &[i64], rid: i64) -> Result<Vec<Option<i64>>, DbError> {
    //     let addr = self.page_directory.get(rid)?;
    //     self.page_ranges.read_projected(projected, &addr)
    // }

    pub fn read_latest(&mut self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let base_location = PageLocation::base(base_addr);
        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;

        let mut bp_lock = self.bufferpool.lock();

        let indirection =
            bp_lock.read_meta_col(MetaPage::Indirection, &base_location, &self.table_ctx)?;

        if indirection.is_none() || indirection == Some(rid) {
            // No tails - read all data columns from base
            return Ok((0..num_data_cols)
                .map(|col| bp_lock.read_col(col, &base_location, &self.table_ctx))
                .collect::<Result<_, _>>()?);
        }

        let indirection_rid = indirection.unwrap();
        let collection = base_addr.collection_num;
        let tps_watermark = self
            .page_ranges
            .base
            .tps
            .get(collection)
            .copied()
            .unwrap_or(i64::MIN);

        // TPS short-circuit: base already holds consolidated values
        if indirection_rid <= tps_watermark {
            return Ok((0..num_data_cols)
                .map(|col| bp_lock.read_col(col, &base_location, &self.table_ctx))
                .collect::<Result<_, _>>()?);
        }

        // Walk tails first
        let all_cols_mask = (1i64 << num_data_cols) - 1;
        let mut resolved: i64 = 0;
        let mut result: Vec<Option<i64>> = vec![None; num_data_cols];
        let mut current_tail_rid = indirection_rid;

        loop {
            let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
            let tail_schema = bp_lock
                .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                .unwrap_or(0);

            let new_cols = tail_schema & !resolved;
            for col in 0..num_data_cols {
                if (new_cols >> col) & 1 == 1 {
                    result[col] = bp_lock.read_col(col, &tail_location, &self.table_ctx)?;
                }
            }
            resolved |= tail_schema;

            // Early exit: all data columns already resolved from tails
            if resolved & all_cols_mask == all_cols_mask {
                return Ok(result);
            }

            let next_rid =
                bp_lock.read_meta_col(MetaPage::Indirection, &tail_location, &self.table_ctx)?;
            match next_rid {
                Some(next) if next == rid => break, // reached original base
                Some(next) if next <= tps_watermark => break, // reached merged region
                Some(next) => current_tail_rid = next,
                None => break,
            }
        }

        // Read only unresolved columns from base
        for col in 0..num_data_cols {
            if (resolved >> col) & 1 == 0 {
                result[col] = bp_lock.read_col(col, &base_location, &self.table_ctx)?;
            }
        }

        Ok(result)
    }

    pub fn read_latest_single(&mut self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let base_location = PageLocation::base(base_addr);

        let mut bp_lock = self.bufferpool.lock();

        let indirection =
            bp_lock.read_meta_col(MetaPage::Indirection, &base_location, &self.table_ctx)?;

        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            // TPS short-circuit: base is already consolidated up to watermark.
            let collection = base_addr.collection_num;
            let tps_watermark = self
                .page_ranges
                .base
                .tps
                .get(collection)
                .copied()
                .unwrap_or(i64::MIN);
            if tail_rid <= tps_watermark {
                return Ok(bp_lock.read_col(col, &base_location, &self.table_ctx)?);
            }

            let mut current_tail_rid = tail_rid;
            loop {
                let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
                let tail_schema = bp_lock
                    .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                    .unwrap_or(0);

                if (tail_schema >> col) & 1 == 1 {
                    return Ok(bp_lock.read_col(col, &tail_location, &self.table_ctx)?);
                }

                let next_rid = bp_lock.read_meta_col(
                    MetaPage::Indirection,
                    &tail_location,
                    &self.table_ctx,
                )?;
                match next_rid {
                    Some(next) if next == rid => break,
                    Some(next) if next <= tps_watermark => break,
                    Some(next) => current_tail_rid = next,
                    None => break,
                }
            }
        }
        Ok(bp_lock.read_col(col, &base_location, &self.table_ctx)?)
    }

    pub fn read_version_single(
        &mut self,
        rid: i64,
        col: usize,
        relative_version: i64,
    ) -> Result<Option<i64>, DbError> {
        // relative_version: 0 = latest, -1 = one update before latest, etc.
        // We traverse tail chain from newest to oldest.
        // tail that updates col counts as one version step.
        // want the tail at position relative_version
        let base_addr = self.page_directory.get(rid)?;
        let base_location = PageLocation::base(base_addr);
        let collection = base_addr.collection_num;
        let tps_watermark = self
            .page_ranges
            .base
            .tps
            .get(collection)
            .copied()
            .unwrap_or(i64::MIN);

        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
            &base_location,
            &self.table_ctx,
        )?;
        if let Some(tail_rid) = indirection
            && tail_rid != rid
            && tail_rid > tps_watermark
        {
            let mut version_counter: i64 = 0;
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_location = PageLocation::tail(self.page_directory.get(current_tail_rid)?);
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(MetaPage::SchemaEncoding, &tail_location, &self.table_ctx)?
                    .unwrap_or(0);

                if (tail_schema >> col) & 1 == 1 {
                    if version_counter == relative_version {
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
                match next_rid {
                    Some(next) if next == rid => break,
                    Some(next) if next <= tps_watermark => break, // merged region - base is the floor
                    Some(next) => current_tail_rid = next,
                    None => break,
                }
            }
        }
        // Fell off the chain or reached TPS - consolidated base is oldest version.
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
        let base_addr = self.page_directory.get(rid)?;
        let base_location = PageLocation::base(base_addr);
        let collection = base_addr.collection_num;
        let tps_watermark = self
            .page_ranges
            .base
            .tps
            .get(collection)
            .copied()
            .unwrap_or(i64::MIN);

        let tails_to_skip = (-relative_version).max(0) as usize;

        let mut tail_rids: Vec<i64> = Vec::new();
        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
            &base_location,
            &self.table_ctx,
        )?;
        if let Some(tail_rid) = indirection
            && tail_rid != rid
            && tail_rid > tps_watermark
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
                    Some(next_rid) if next_rid != rid && next_rid > tps_watermark => {
                        current = next_rid
                    }
                    _ => break,
                }
            }
        }

        // Start with base record
        let mut result = self
            .page_ranges
            .read(&base_location.addr, &self.table_ctx)?;

        // Skip the newest tails, then apply the rest newest --> oldest
        let relevant = if tails_to_skip >= tail_rids.len() {
            &tail_rids[tail_rids.len()..] // empty - return base
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
                    result[col] =
                        self.page_ranges
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

    pub fn is_record_deleted(&mut self, rid: i64) -> Result<bool, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let base_location = PageLocation::base(base_addr);

        let indirection = self.page_ranges.read_meta_col(
            MetaPage::Indirection,
            &base_location,
            &self.table_ctx,
        )?;

        let indirection_rid = match indirection {
            None | Some(_) if indirection == Some(rid) => return Ok(false), // no tails
            Some(ind) => ind,
            None => return Ok(false),
        };

        // Check if record has been merged (indirection_rid <= TPS).
        let collection = base_addr.collection_num;
        let tps_watermark = self
            .page_ranges
            .base
            .tps
            .get(collection)
            .copied()
            .unwrap_or(i64::MIN);

        if indirection_rid <= tps_watermark {
            // check schema_encoding on the consolidated base page.
            let base_schema = self.page_ranges.read_meta_col(
                MetaPage::SchemaEncoding,
                &base_location,
                &self.table_ctx,
            )?;
            return Ok(base_schema.is_none());
        }

        let tail_location = PageLocation::tail(self.page_directory.get(indirection_rid)?);
        let schema = self.page_ranges.read_meta_col(
            MetaPage::SchemaEncoding,
            &tail_location,
            &self.table_ctx,
        )?;
        Ok(schema.is_none())
    }

    pub fn merge(&mut self) -> Result<(), DbError> {
        let base_rids: Vec<i64> = self.primary_index.iter().map(|(_, rid)| rid).collect();
        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;

        for base_rid in base_rids {
            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };
            let base_location = PageLocation::base(base_addr);
            // Only process records that have an unmerged tail chain.
            let indirection = match self.page_ranges.read_meta_col(
                MetaPage::Indirection,
                &base_location,
                &self.table_ctx,
            ) {
                Ok(Some(ind)) if ind != base_rid => ind,
                _ => continue,
            };

            // Check whether the latest tail is a deletion marker.
            let tail_addr = match self.page_directory.get(indirection) {
                Ok(addr) => addr,
                Err(_) => continue,
            };
            let tail_location = PageLocation::tail(tail_addr);
            let latest_schema = match self.page_ranges.read_meta_col(
                MetaPage::SchemaEncoding,
                &tail_location,
                &self.table_ctx,
            ) {
                Ok(val) => val,
                Err(_) => continue,
            };
            let (consolidated_data, schema_encoding) = if latest_schema.is_none() {
                // Deleted: null data, schema_encoding = None (deletion marker on base)
                (vec![None; num_data_cols], None)
            } else {
                // Updated: latest values, schema_encoding = Some(0) (reads don't use
                // base schema_encoding, only tail schema_encoding matters for reads)
                let latest = self.read_latest(base_rid)?;
                (latest[..num_data_cols].to_vec(), Some(0i64))
            };

            let new_addr = self.page_ranges.append_merged_base(
                consolidated_data,
                base_rid,
                indirection, // preserved — merge never modifies indirection
                schema_encoding,
                &self.table_ctx,
            )?;
            self.page_directory.add(base_rid, new_addr);

            let col = base_addr.collection_num;
            if col >= self.page_ranges.base.tps.len() {
                self.page_ranges.base.tps.resize(col + 1, i64::MIN);
            }
            self.page_ranges.base.tps[col] = self.page_ranges.base.tps[col].max(indirection);
        }

        self.page_ranges.tail.reset_merge_counter();
        Ok(())
    }
}
