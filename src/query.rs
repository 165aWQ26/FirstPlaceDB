use crate::db_error::DbError;
use crate::table::Table;

//May want to put MetaPage somewhere that isn't the bufferpool
use crate::bufferpool::MetaPage;
use crate::bufferpool_context::PageLocation;
use crate::index::{Index, TableIndex};

pub struct Query<'a> {
    pub table: &'a mut Table,
    num_data_cols: usize,
}

impl<'a> Query<'a> {
    pub const DEFAULT_MERGE_THRESHOLD: usize = 10;
    pub const DEFAULT_INDIRECTION: Option<i64> = None;
    pub const DEFAULT_SCHEMA_ENCODING: Option<i64> = Some(0);

    pub fn new(table: &'a mut Table) -> Self {
        let cols = table.table_ctx.total_cols - Table::NUM_META_PAGES;
        Self {
            table,
            num_data_cols: cols,
        }
    }

    pub fn insert(&mut self, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = self.table.rid.next().unwrap();
        let pk_val =
            record[self.table.key_index].ok_or(DbError::NullValue(self.table.key_index))?;
        if !self.table.primary_index.insert_unique(pk_val, rid) {
            return Ok(false); // Duplicate primary key
        }

        for (i, key_opt) in record.iter().enumerate() {
            if i == self.table.key_index {
                continue; // Already handled
            }

            if let Some(key) = key_opt {
                // Use unified accessor to insert into secondary indices
                self.table.get_index_mut(i).insert(*key, rid);
            }
        }

        // Write record (append_base handles all 4 metadata columns)
        let address = self
            .table
            .page_ranges
            .append_base(record, rid, &self.table.table_ctx)?;
        self.table.page_directory.add(rid, address);
        Ok(true)
    }

    pub fn select(
        &mut self,
        key: i64,
        search_key_index: usize,
        projected_columns_index: &[i64],
    ) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let rids = self.table.locate(key, search_key_index)?;

        let mut result = Vec::with_capacity(rids.len());

        for rid in rids {
            if self.table.is_record_deleted(rid)? {
                continue;
            }
            result.push(
                self.table
                    .read_latest_projected(projected_columns_index, rid)?,
            );
        }

        Ok(result)
    }

    pub fn select_version(
        &mut self,
        key: i64,
        search_key_index: usize,
        projected_columns_index: &[i64],
        relative_version: i64,
    ) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let rids = self.table.locate(key, search_key_index)?;

        let mut result = Vec::with_capacity(rids.len());

        for rid in rids {
            if self.table.is_record_deleted(rid)? {
                continue;
            }
            result.push(self.table.read_version_projected(
                projected_columns_index,
                rid,
                relative_version,
            )?);
        }

        Ok(result)
    }

    pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = match self.table.locate_primary(key) {
            Ok(rid) => rid,
            _ => return Ok(false),
        };

        let base_location = PageLocation::base(self.table.page_directory.get(rid)?);

        // Get current indirection (points to latest tail, or self if no updates)
        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(MetaPage::Indirection, &base_location, &self.table.table_ctx)?
            .ok_or(DbError::NullValue(404))?;

        // Check pk update
        if record[self.table.key_index].is_some() {
            return Err(DbError::DuplicateKey(key));
        }

        let current_values = self.table.read_latest(rid)?;

        for (i, key_opt) in record.iter().enumerate() {
            if i == self.table.key_index {
                continue;
            }

            if let Some(new_key) = key_opt {
                let old_key_opt = current_values[i];

                let index = self.table.get_index_mut(i);

                if let Some(old_key) = old_key_opt {
                    index.remove(old_key, rid);
                }

                index.insert(*new_key, rid);
            }
        }

        // Build schema encoding for this tail record
        let mut schema_encoding: i64 = 0;
        for (i, val) in record.iter().enumerate() {
            if val.is_some() {
                schema_encoding |= 1 << i;
            }
        }

        // Append tail record
        let next_rid = self.table.rid.next().unwrap();
        let address = self.table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            Some(schema_encoding),
            Option::from(rid),
            &self.table.table_ctx,
        )?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection
        self.table.page_ranges.write_indirection(
            Some(next_rid),
            &base_location,
            &self.table.table_ctx,
        )?;

        // Merge check
        if self.table.page_ranges.tail.full_pages_since_merge >= Query::DEFAULT_MERGE_THRESHOLD {
            self.table.merge()?;
        }

        Ok(true)
    }

    pub fn delete(&mut self, key: i64) -> Result<bool, DbError> {
        let rid = self.table.locate_primary(key)?;
        self.table.primary_index.remove(key, rid);

        let base_location = PageLocation::base(self.table.page_directory.get(rid)?);

        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(MetaPage::Indirection, &base_location, &self.table.table_ctx)?
            .ok_or(DbError::NullValue(404))?;

        // Append deletion tail (schema_encoding = None marks deletion)
        let next_rid = self.table.rid.next().unwrap();
        let tail_record = vec![None; self.num_data_cols];
        let address = self.table.page_ranges.append_tail(
            tail_record,
            next_rid,
            current_indirection,
            None,
            Option::from(rid),
            &self.table.table_ctx,
        )?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection to point to deletion tail
        self.table.page_ranges.write_indirection(
            Some(next_rid),
            &base_location,
            &self.table.table_ctx,
        )?;

        if self.table.page_ranges.tail.full_pages_since_merge >= Query::DEFAULT_MERGE_THRESHOLD {
            self.table.merge()?;
        }
        Ok(true)
    }

    pub fn sum(&mut self, start_range: i64, end_range: i64, col: usize) -> Result<i64, DbError> {
        let rids = self
            .table
            .primary_index
            .locate_range(start_range, end_range);

        if rids.is_empty() {
            return Err(DbError::KeyNotFound(start_range));
        }

        let mut sum: i64 = 0;
        for rid in rids {
            if self.table.is_record_deleted(rid)? {
                continue;
            }
            sum += self
                .table
                .read_latest_single(rid, col)?
                .ok_or(DbError::NullValue(col))?;
        }
        Ok(sum)
    }

    pub fn sum_version(
        &mut self,
        start_range: i64,
        end_range: i64,
        col: usize,
        relative_version: i64,
    ) -> Result<i64, DbError> {
        let rids = self
            .table
            .primary_index
            .locate_range(start_range, end_range);

        if rids.is_empty() {
            return Err(DbError::KeyNotFound(start_range));
        }

        let mut sum: i64 = 0;
        for rid in rids {
            sum += self
                .table
                .read_version_single(rid, col, relative_version)?
                .ok_or(DbError::NullValue(col))?;
        }
        Ok(sum)
    }

    pub fn increment(&mut self, key: i64, col: usize) -> Result<bool, DbError> {
        // Reject primary key or metadata column increments
        if col == self.table.key_index || col >= self.num_data_cols {
            return Ok(false);
        }

        // Use locate_primary for single RID
        let rid = self.table.locate_primary(key)?;

        let mut record: Vec<Option<i64>> = vec![None; self.num_data_cols];

        let temp = self
            .table
            .read_latest_single(rid, col)?
            .ok_or(DbError::NullValue(col))?
            + 1;
        record[col] = Some(temp);

        self.update(key, record)
    }
}
