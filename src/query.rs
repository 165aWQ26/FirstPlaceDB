use crate::db_error::DbError;
use crate::page_range::WhichRange;
use crate::table::Table;

//May want to put MetaPage somewhere that isn't the bufferpool
use crate::bufferpool::MetaPage;
use crate::bufferpool_context::PageLocation;

pub struct Query<'a> {
    pub table: &'a mut Table,
    num_data_cols: usize,
}

impl<'a> Query<'a> {
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
        let key = record[self.table.key_index].ok_or(DbError::NullValue(self.table.key_index))?;

        // Single-traversal uniqueness check + insert
        if !self.table.indices[self.table.key_index].insert_unique(key, rid) {
            return Ok(false);
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
        let rid = self.table.indices[search_key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))?;

        //for rid in Rids
        if self.table.is_deleted(rid)? {
            return Err(DbError::KeyNotFound(key));
        }
        Ok(vec![
            self.table
                .read_latest_projected(projected_columns_index, rid)?,
        ])
    }

    pub fn select_version(
        &mut self,
        key: i64,
        _search_key_index: usize,
        projected_columns_index: &[i64],
        relative_version: i64,
    ) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let rid = self.table.rid_for_key(key)?;

        if self.table.is_deleted(rid)? {
            return Err(DbError::KeyNotFound(key));
        }

        Ok(vec![self.table.read_version_projected(
            projected_columns_index,
            rid,
            relative_version,
        )?])
    }

    pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = match self.table.rid_for_key(key) {
            Ok(rid) => rid,
            _ => return Ok(false),
        };

        let base_location = PageLocation::base(self.table.page_directory.get(rid)?);

        // Get current indirection (points to latest tail, or self if no updates)
        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(
                MetaPage::IndirectionCol,
                &base_location,
                &self.table.table_ctx,
            )?
            .ok_or(DbError::NullValue(404))?;

        // Build schema encoding for this tail record
        let mut schema_encoding: i64 = 0;
        for (i, val) in record.iter().enumerate() {
            if val.is_some() {
                schema_encoding |= 1 << i;
            }
        }

        // TODO fix secondary indices
        // remove the previous tail from the index
        if let Some(new_key) = record[self.table.key_index] {
            self.table.indices[self.table.key_index].remove(key, rid);
            self.table.indices[self.table.key_index].insert_unique(new_key, rid);
        }

        let next_rid = self.table.rid.next().unwrap();

        // Append tail record
        let address = self.table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            Some(schema_encoding),
            &self.table.table_ctx,
        )?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection
        self.table.page_ranges.write_indirection(
            Some(next_rid),
            &PageLocation::new(address, WhichRange::Base),
            &self.table.table_ctx,
        )?;

        Ok(true)
    }

    pub fn delete(&mut self, key: i64) -> Result<bool, DbError> {
        let rid = self.table.rid_for_key(key)?;

        // Only remove from primary key index; secondary indices are filtered lazily
        self.table.indices[self.table.key_index].remove(key, rid);

        let base_location = PageLocation::base(self.table.page_directory.get(rid)?);

        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(
                MetaPage::IndirectionCol,
                &base_location,
                &self.table.table_ctx,
            )?
            .ok_or(DbError::NullValue(404))?;

        // Append deletion tail (schema_encoding = None marks deletion)
        let next_rid = self.table.rid.next().unwrap();
        let tail_record = vec![None; self.num_data_cols];
        let address = self.table.page_ranges.append_tail(
            tail_record,
            next_rid,
            current_indirection,
            None,
            &self.table.table_ctx,
        )?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection to point to deletion tail
        self.table.page_ranges.write_indirection(
            Some(next_rid),
            &base_location,
            &self.table.table_ctx,
        )?;
        Ok(true)
    }

    pub fn sum(&mut self, start_range: i64, end_range: i64, col: usize) -> Result<i64, DbError> {
        if let Some(rids) =
            self.table.indices[self.table.key_index].locate_range(start_range, end_range)
        {
            let mut sum: i64 = 0;

            for rid in rids {
                if self.table.is_deleted(rid)? {
                    continue;
                }
                sum += self
                    .table
                    .read_latest_single(rid, col)?
                    .ok_or(DbError::NullValue(col))?;
            }
            Ok(sum)
        } else {
            Err(DbError::KeyNotFound(start_range))
        }
    }

    pub fn sum_version(
        &mut self,
        start_range: i64,
        end_range: i64,
        col: usize,
        relative_version: i64,
    ) -> Result<i64, DbError> {
        if let Some(rids) =
            self.table.indices[self.table.key_index].locate_range(start_range, end_range)
        {
            // cumulative sum of all columns
            let mut sum: i64 = 0;

            for rid in rids {
                sum += self
                    .table
                    .read_version_single(rid, col, relative_version)?
                    .ok_or(DbError::NullValue(col))?;
            }
            Ok(sum)
        } else {
            Err(DbError::KeyNotFound(start_range))
        }
    }

    pub fn increment(&mut self, key: i64, col: usize) -> Result<bool, DbError> {
        // Reject primary key or metadata column increments
        if col == self.table.key_index || col >= self.num_data_cols {
            return Ok(false);
        }

        let rid = self.table.indices[self.table.key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))?;

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
