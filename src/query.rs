use crate::error::DbError;
use crate::page_collection::INDIRECTION_COL;
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

    // TODO
    // M1: primary key index only. For M2: restore secondary indices here.
    // Old secondary index code:
    // for (i, val) in record.iter().enumerate() {
    //     self.table.indices[i].insert(val.unwrap(), rid);
    // }
    pub fn insert(&mut self, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = self.table.rid.next().unwrap();
        let key = record[self.table.key_index].ok_or(DbError::NullValue(self.table.key_index))?;

        // Single-traversal uniqueness check + insert
        if !self.table.indices[self.table.key_index].insert_unique(key, rid) {
            return Ok(false);
        }

        // Write record (append_base handles all 4 metadata columns)
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

        Ok(vec![
            self.table
                .read_latest_projected(projected_columns_index, rid)?,
        ])
    }
    // TODO m3
    //     pub fn select_version(&self, key: i64, search_key_index:usize,
    //                   projected_columns_index: &[i64], relative_version:i64) -> Result<Vec<Record>, bool> {

    // if let Some(indirection_pointer) = self.table.read_single(rid, record.len() + 2) {
    //             // set to previous tail page
    //         } else if let Some(indirection_pointer) = self.table.read_single(rid, record.len() + 1) {
    //             // first update --> set to base page
    //         } else {
    //         }
    //         // TODO: UPDATE INDIRECTION COLUMN
    //     }

    // pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> bool {

    //     if let Some(rids) = self.table.indices[self.table.key_index].locate(key) {
    //         let rid = rids[0];
    //         let base_addr = self.table.page_directory.get(rid);
    //         if let indirection = self
    //             .table
    //             .page_ranges
    //             .get_indirection(&base_addr)
    //             .unwrap() {

    //             } else if self.table.page_ranges.get_indirection(addr)

    //         let mut schema_encoding: i64 =
    //             self.table.read_single(rid, record.len() + INDIRECTION_COL).unwrap_or(0);

    //         //Updating index for all value that have been changed
    //         for i in 0..record.len() {
    //             if record[i].is_some() {
    //                 self.table.indices[i].remove(self.table.read_single(rid, i).unwrap(), rid);
    //                 //Updates schema encoding
    //                 self.table.indices[i].insert(record[i].unwrap(), rid);
    //                 schema_encoding |= 1 << i;
    //             }
    //         }
    //         //Appending rid, schema, then the indirection pointer to the end of
    //         //  it
    //         let next_rid = self.table.rid.next().unwrap();
    //         let address =
    //             self.table
    //                 .page_ranges
    //                 .append_tail(record, next_rid, indirection, schema_encoding);
    //         self.table.page_directory.add(next_rid, address);
    //         let indirection_col = self.table.num_columns + INDIRECTION_COL;
    //         self.table
    //             .page_ranges
    //             .write_single(indirection_col, &base_addr, Some(next_rid));
    //         return true;
    //     };
    //     //let key: Option<i64> = record[self.table.key_index];
    //     false
    // }
    pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let rid = match self.table.indices[self.table.key_index].locate(key) {
            Some(rid) => rid,
            None => return Ok(false),
        };

        let base_addr = self.table.page_directory.get(rid)?;

        // Get current indirection (points to latest tail, or self if no updates)
        let current_indirection = self
            .table
            .page_ranges
            .get_indirection(&base_addr)?
            .unwrap_or(rid);

        // Build schema encoding for this tail record
        let mut schema_encoding: i64 = 0;
        for (i, val) in record.iter().enumerate() {
            if val.is_some() {
                schema_encoding |= 1 << i;
            }
        }

        // remove the previous tail from the index

        //// DELETE THIS WHEN MOVING ONTO MILESTONE 2:
        let current_values = self.table.read_latest(rid)?;
        let key = record[self.table.key_index];
        if key.is_some() {
            if current_values[0].is_some() {
                self.table.indices[0].remove(current_values[0].unwrap(), rid);
            }
            self.table.indices[0].insert(key.ok_or(DbError::NullValue(0))?, rid);
        }
        ////

        // let current_values = self.table.read_latest(rid)?;
        // for (i, val) in record.iter().enumerate() {
        //     if val.is_some() {
        //         if let Some(old_val) = current_values[i] {
        //             self.table.indices[i].remove(old_val, rid);
        //         }
        //         self.table.indices[i].insert(val.ok_or(DbError::NullValue(i))?, rid);
        //     }
        // }

        let next_rid = self.table.rid.next().unwrap();

        // Append tail record
        let address = self.table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            Some(schema_encoding),
        )?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection
        let indirection_col = self.table.num_columns + INDIRECTION_COL;
        self.table
            .page_ranges
            .write_single(indirection_col, &base_addr, Some(next_rid))?;

        Ok(true)
    }

    pub fn delete(&mut self, key: i64) -> Result<bool, DbError> {
        let rid = match self.table.indices[self.table.key_index].locate(key) {
            Some(rid) => rid,
            None => return Err(DbError::KeyNotFound(key)),
        };

        // Only remove from primary key index; secondary indices are filtered lazily
        self.table.indices[self.table.key_index].remove(key, rid);

        let base_addr = self.table.page_directory.get(rid)?;

        let current_indirection = self
            .table
            .page_ranges
            .get_indirection(&base_addr)?
            .unwrap_or(rid);

        // Append deletion tail (schema_encoding = None marks deletion)
        let next_rid = self.table.rid.next().unwrap();
        let tail_record = vec![None; self.table.num_columns];
        let address =
            self.table
                .page_ranges
                .append_tail(tail_record, next_rid, current_indirection, None)?;

        self.table.page_directory.add(next_rid, address);

        // Update base indirection to point to deletion tail
        let indirection_col = self.table.num_columns + INDIRECTION_COL;
        self.table
            .page_ranges
            .write_single(indirection_col, &base_addr, Some(next_rid))?;

        Ok(true)
    }

    pub fn sum(&self, start_range: i64, end_range: i64, col: usize) -> Result<i64, DbError> {
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
        &self,
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

                // rid to iterate with
                let mut curr_rid = rid;

                // check against relative_version to ensure proper value
                let mut count: i64 = 0;

                // base record address for termination cases
                let base_addr = self.table.page_directory.get(rid)?;

                loop {

                    // get addr of current base/tail
                    let addr = self.table.page_directory.get(curr_rid)?;
                    
                    // if val is not None
                    if let Some(val) = self.table.page_ranges.read_single(col, &addr)? {

                        // if we have reached relative version
                        if count == relative_version.abs() {

                            // add val to sum and continue to next RID
                            sum += val;
                            break;
                        }
                    }
                    else{

                        // subtracting 1 from count because val is None
                        count -= 1;
                    }
                    
                    // iterate to next tail
                    let next_rid = self.table.page_ranges.get_tail_indirection(&addr)?;
                    
                    match next_rid {

                        // if next tail exists and points to base --> add val of base to sum continue to next RID
                        Some(next) if next == rid => {
                            if let Some(val) = self.table.page_ranges.read_single(col, &base_addr)?{
                                sum += val;
                                break;
                            }
                        },

                        // if tail does not exist, then we are at base --> add val of base to sum continue to next RID
                        None => {
                            if let Some(val) = self.table.page_ranges.read_single(col, &base_addr)?{
                                sum += val;
                                break;
                            }
                        },

                        // otherwise, iterate to next tail, increment count, continue in loop
                        Some(next) => {
                            curr_rid = next;
                            count += 1;
                            continue;
                        },
                    }
                }
            }
            Ok(sum)
        }
        
        else {
            Err(DbError::KeyNotFound(start_range))
        }
    }

    pub fn increment(&mut self, key: i64, col: usize) -> Result<bool, DbError> {
        let rid = self.table.indices[self.table.key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))?;

        let mut record: Vec<Option<i64>> = vec![None; self.table.num_columns];

        let temp = self
            .table
            .read_latest_single(rid, col)?
            .ok_or(DbError::NullValue(col))?
            + 1;
        record[col] = Some(temp);

        self.update(key, record)
    }
}
