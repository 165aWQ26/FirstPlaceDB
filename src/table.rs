use crate::bufferpool::{BufferPool, MetaPage};
use crate::bufferpool_context::{PageLocation, TableContext};
use crate::db_error::DbError;
use crate::index::{Index, NonUniqueIndex, TableIndex, UniqueIndex};
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;
use crate::io_helper;
use crate::io_helper::{read_i64, read_usize};

#[derive(Debug)]
pub enum TableError {
    InvalidPath,
    InvalidKeyIndex,
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
            .write_to_disk(writer)
    }

    pub fn write_to_disk(&self, path: String) -> Result<(), TableError> {
        let mut file_path = path;
        file_path.push_str("table_data");

        let file = File::create(&file_path).map_err(|_| TableError::InvalidPath)?;
        let mut w = BufWriter::new(file);

        // 1-3: scalars
        io_helper::write_i64(self.table_ctx.total_cols as i64, &mut w)?;
        io_helper::write_i64(self.key_index as i64, &mut w)?;
        io_helper::write_i64(self.rid.start, &mut w)?;

        // 4-7: page range positions
        let (base_off, base_col) = self.page_ranges.base.position();
        io_helper::write_i64(base_off as i64, &mut w)?;
        io_helper::write_i64(base_col as i64, &mut w)?;

        let (tail_off, tail_col) = self.page_ranges.tail.position();
        io_helper::write_i64(tail_off as i64, &mut w)?;
        io_helper::write_i64(tail_col as i64, &mut w)?;

        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;
        for i in 0..num_data_cols {
            if i == self.key_index {
                io_helper::write_i64(self.primary_index.len() as i64, &mut w)?;
                for (k, r) in self.primary_index.iter() {
                    io_helper::write_i64(k, &mut w)?;
                    io_helper::write_i64(r, &mut w)?;
                }
            } else if let Index::NonUnique(s) = &self.indices[i] {
                io_helper::write_i64(s.len() as i64, &mut w)?; // number of distinct keys
                for (k, rids) in s.iter_raw() {
                    io_helper::write_i64(*k, &mut w)?;
                    io_helper::write_i64(rids.len() as i64, &mut w)?;
                    for rid in rids {
                        io_helper::write_i64(*rid, &mut w)?;
                    }
                }
            } else {
                io_helper::write_i64(0i64, &mut w)?; // Unique non-primary — shouldn't occur
            }
        }

        self.page_directory
            .write_to_disk(&mut w)
            .map_err(|_| TableError::WriteFail)?;

        let tps = &self.page_ranges.base.tps;
        io_helper::write_i64(tps.len() as i64, &mut w)?;
        for &wm in tps {
            io_helper::write_i64(wm, &mut w)?;
        }

        Ok(())
    }

    pub fn read_from_disk(&mut self, path: String) -> Result<(), TableError> {
        let mut file_path = path;
        file_path.push_str("table_data");

        let file = File::open(&file_path).map_err(|_| TableError::InvalidPath)?;
        let mut reader = BufReader::new(file);
        let mut buf = [0u8; 8];

        self.table_ctx.total_cols = read_usize(&mut buf, &mut reader)?;
        self.key_index             = read_usize(&mut buf, &mut reader)?;
        let rid_start              = read_i64(&mut buf, &mut reader)?;
        self.rid                   = rid_start..;

        let num_data_cols = self.table_ctx.total_cols - Self::NUM_META_PAGES;


        let base_off = read_usize(&mut buf, &mut reader)?;
        let base_col = read_usize(&mut buf, &mut reader)?;
        self.page_ranges.base.set_position(base_off, base_col);

        let tail_off = read_usize(&mut buf, &mut reader)?;
        let tail_col = read_usize(&mut buf, &mut reader)?;
        self.page_ranges.tail.set_position(tail_off, tail_col);


        self.primary_index = UniqueIndex::new();
        self.indices = (0..num_data_cols)
            .map(|col| {
                // indices[key_index] must be Unique to match how Table::new() sets it up,
                // so any code that pattern-matches on it directly doesn't get MismatchedIndex.
                if col == self.key_index {
                    Index::Unique(UniqueIndex::new())
                } else {
                    Index::NonUnique(NonUniqueIndex::new())
                }
            })
            .collect();

        for i in 0..num_data_cols {
            let count = read_usize(&mut buf, &mut reader)?;

            if i == self.key_index {
                for _ in 0..count {
                    let key = read_i64(&mut buf, &mut reader)?;
                    let rid = read_i64(&mut buf, &mut reader)?;
                    self.primary_index.insert_unique(key, rid);
                }
            } else if let Index::NonUnique(s) = &mut self.indices[i] {
                for _ in 0..count {
                    let key      = read_i64(&mut buf, &mut reader)?;
                    let num_rids = read_usize(&mut buf, &mut reader)?;
                    for _ in 0..num_rids {
                        let rid = read_i64(&mut buf, &mut reader)?;
                        s.insert(key, rid);
                    }
                }
            }
        }


        self.page_directory
            .read_from_disk(&mut buf, &mut reader)
            .map_err(|_| TableError::ReadFail)?;


        let tps_len = read_usize(&mut buf, &mut reader)?;
        self.page_ranges.base.tps = Vec::with_capacity(tps_len);
        for _ in 0..tps_len {
            let wm = read_i64(&mut buf, &mut reader)?;
            self.page_ranges.base.tps.push(wm);
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

    /*
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
    */

    pub fn merge(&mut self) -> Result<(), DbError> {
        let base_rids: Vec<i64> = self.primary_index.iter().map(|(_, rid)| rid).collect();
        let num_data_cols = self.table_ctx.total_cols - Table::NUM_META_PAGES;

        for base_rid in base_rids {
            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            // computing the TPS for record's curr col. b4 skip
            let tps_watermark = self
                .page_ranges
                .base
                .tps
                .get(base_addr.collection_num)
                .copied()
                .unwrap_or(i64::MIN);

            let base_location = PageLocation::base(base_addr);

            // only process recs that have an unmerged tail chain.
            // ind != base_rid: skip recs w/ no tails
            // ind > tps_watermark: skip recs from prior merge
            let indirection = match self.page_ranges.read_meta_col(
                MetaPage::Indirection,
                &base_location,
                &self.table_ctx,
            ) {
                Ok(Some(ind)) if ind != base_rid && ind > tps_watermark => ind,
                _ => continue,
            };

            // check if latest tail is del.
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
                // data, schema_encoding = None
                (vec![None; num_data_cols], None)
            } else {
                // latest vals, schema_encoding = Some(0)
                let latest = self.read_latest(base_rid)?;
                (latest[..num_data_cols].to_vec(), Some(0i64))
            };

            let new_addr = self.page_ranges.append_merged_base(
                consolidated_data,
                base_rid,
                indirection, // merge doesn't modify indirection, leave as is
                schema_encoding,
                &self.table_ctx,
            )?;
            self.page_directory.add(base_rid, new_addr);

            // use new_addr.collection_num instead of base_addr.collection_num.
            // append_merged_base writes to base.next_addr() which might put it in a diff col. than old base
            // after page_directory.add(), read_latest and read_version_single look up tps[new_addr.collection_num]
            let new_col = new_addr.collection_num;
            if new_col >= self.page_ranges.base.tps.len() {
                self.page_ranges.base.tps.resize(new_col + 1, i64::MIN);
            }
            self.page_ranges.base.tps[new_col] =
                self.page_ranges.base.tps[new_col].max(indirection);
        }

        self.page_ranges.tail.reset_merge_counter();
        Ok(())
    }
}
