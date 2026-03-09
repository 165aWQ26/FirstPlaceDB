use crate::errors::DbError;
use crate::page_collection::MetaPage;
use crate::page_range::WhichRange;
use crate::table::Table;

pub struct Query {
    pub table: Table,
}

impl Query {
    pub const DEFAULT_INDIRECTION: Option<i64> = None;
    pub const DEFAULT_SCHEMA_ENCODING: Option<i64> = Some(0);

    pub fn new(table: Table) -> Self {
        Self { table }
    }

    pub fn insert(&mut self, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = self.table.rid.next();
        let key = record[self.table.key_index].ok_or(DbError::NullValue(self.table.key_index))?;

        if !self.table.indices[self.table.key_index].insert_unique(key, rid) {
            return Ok(false);
        }

        let address = self.table.page_ranges.append_base(record, rid)?;
        self.table.page_directory.add(rid, address);
        Ok(true)
    }

    pub fn select(
        &self,
        key: i64,
        search_key_index: usize,
        projected_columns_index: &[i64],
    ) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let rid = self.table.indices[search_key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))?;

        if self.table.is_deleted(rid)? {
            return Err(DbError::KeyNotFound(key));
        }

        Ok(vec![self.table.read_latest_projected(projected_columns_index, rid)?])
    }

    pub fn select_version(
        &self,
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

        let base_addr = self.table.page_directory.get(rid)?;

        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?
            .ok_or(DbError::NullValue(404))?;

        let mut schema_encoding: i64 = 0;
        for (i, val) in record.iter().enumerate() {
            if val.is_some() {
                schema_encoding |= 1 << i;
            }
        }

        let new_key = record[self.table.key_index];
        if new_key.is_some() {
            let current_values = self.table.read_latest(rid)?;
            if let Some(old_key) = current_values[0] {
                self.table.indices[0].remove(old_key, rid);
            }
            self.table.indices[0].insert(new_key.ok_or(DbError::NullValue(0))?, rid);
        }

        let next_rid = self.table.rid.next();

        let address = self.table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            Some(schema_encoding),
        )?;

        self.table.page_directory.add(next_rid, address);

        self.table
            .page_ranges
            .write_indirection(&base_addr, Some(next_rid), WhichRange::Base)?;

        // Mark this base RID as having unmerged tail data.
        // DashSet deduplicates automatically so repeated updates to the same
        // record are cheap and don't inflate the dirty set.
        self.table.dirty_base_rids.insert(rid);

        Ok(true)
    }

    pub fn delete(&mut self, key: i64) -> Result<bool, DbError> {
        let rid = self.table.rid_for_key(key)?;

        self.table.indices[self.table.key_index].remove(key, rid);

        let base_addr = self.table.page_directory.get(rid)?;

        let current_indirection = self
            .table
            .page_ranges
            .read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?
            .ok_or(DbError::NullValue(404))?;

        let next_rid = self.table.rid.next();
        let tail_record = vec![None; self.table.num_data_columns];
        let address = self.table.page_ranges.append_tail(
            tail_record,
            next_rid,
            current_indirection,
            None, // schema_encoding = None is the deletion marker
        )?;

        self.table.page_directory.add(next_rid, address);

        self.table
            .page_ranges
            .write_indirection(&base_addr, Some(next_rid), WhichRange::Base)?;

        // Deleted records also need to be merged so the base page reflects
        // the deletion, is_deleted can skip the tail
        self.table.dirty_base_rids.insert(rid);

        Ok(true)
    }

    pub fn sum(&self, start_range: i64, end_range: i64, col: usize) -> Result<i64, DbError> {
        let rids = self.table.indices[self.table.key_index].locate_range(start_range, end_range);
        if rids.is_empty() {
            return Err(DbError::KeyNotFound(start_range));
        }
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
    }

    pub fn sum_version(
        &self,
        start_range: i64,
        end_range: i64,
        col: usize,
        relative_version: i64,
    ) -> Result<i64, DbError> {
        let rids = self.table.indices[self.table.key_index].locate_range(start_range, end_range);
        if rids.is_empty() {
            return Err(DbError::KeyNotFound(start_range));
        }

        // cumulative sum of all columns
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
        if col == self.table.key_index || col >= self.table.num_data_columns {
            return Ok(false);
        }

        let rid = self.table.indices[self.table.key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))?;

        let mut record: Vec<Option<i64>> = vec![None; self.table.num_data_columns];
        let temp = self
            .table
            .read_latest_single(rid, col)?
            .ok_or(DbError::NullValue(col))?
            + 1;
        record[col] = Some(temp);

        self.update(key, record)
    }
}