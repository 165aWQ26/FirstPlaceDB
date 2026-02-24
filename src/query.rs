use crate::error::DbError;
use crate::page_collection::{MetaPage};
use crate::page_range::WhichRange;
use crate::table::Table;
use std::sync::{Arc, Mutex};

/*
pub struct Query {
    // TODO: make this wrapped in Arc<Mutex<Table>>
    pub table: Table,
}
*/
pub struct Query {
    pub table: Arc<Mutex<Table>>,
}

impl Query {
    pub const DEFAULT_INDIRECTION: Option<i64> = None;
    pub const DEFAULT_SCHEMA_ENCODING: Option<i64> = Some(0);

    /*
    pub fn new(table: Table) -> Self {
        Self { table }
    }
    */
    pub fn new(table: Arc<Mutex<Table>>) -> Self {
        Self { table }
    }

    // TODO
    // M1: primary key index only. For M2: restore secondary indices here.
    // Old secondary index code:
    // for (i, val) in record.iter().enumerate() {
    //     table.indices[i].insert(val.unwrap(), rid);
    // }

    pub fn insert(&mut self, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let mut table = self.table.lock().unwrap();
        let rid = table.rid.next().unwrap();
        //let key = record[table.key_index].ok_or(DbError::NullValue(table.key_index))?;

        for (i, key) in record.iter().enumerate() {
            if i == table.key_index {
                // very smart lil d
                // Single-traversal uniqueness check + insert
                if !table.indices[i].insert_unique(key.unwrap(), rid) {
                    return Ok(false);
                }
            } else {
                table.indices[i].insert(key.unwrap(), rid);
            }
        }

        // Write record (append_base handles all 4 metadata columns)
        let address = table.page_ranges.append_base(record, rid)?;
        table.page_directory.add(rid, address);
        Ok(true)
    }

    pub fn select(
        &self,
        key: i64,
        search_key_index: usize,
        projected_columns_index: &[i64],
    ) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let table = self.table.lock().unwrap();
        let rids = match table.rids_for_key(key, search_key_index) {
            Ok(rids) => rids,
            Err(e) => return Err(e),
        };

        let mut result = vec![];

        for rid in rids {
            // TODO: What behavior should happen when user tries selecting deleted RID?
            // for m1, we returned an error when trying to select
            // oh we could never have dup primary.
            // wait wtf? why couldnt we insert a new primary key after old one was deleted?
            // lets assume we cant bc that's weird -- selecting now will ignore deleted records
            if !table.is_deleted(*rid)? {
                result
                    .push(table
                        .read_latest_projected(projected_columns_index, *rid)?);
            }
        }
        Ok(result)
    }

    pub fn select_version(&self, key: i64, search_key_index:usize,
            projected_columns_index: &[i64], relative_version:i64) -> Result<Vec<Vec<Option<i64>>>, DbError> {
        let table = self.table.lock().unwrap();
        let rids = match table.rids_for_key(key, search_key_index) {
            Ok(rids) => rids,
            Err(e) => return Err(e),
        };

        let mut result = vec![];

        for rid in rids {
            let deleted = table.is_deleted(*rid)?;

            if deleted && relative_version < 0 {
                return Err(DbError::KeyNotFound(key));
            }

            if !deleted {
                result
                    .push(table
                        .read_version_projected(projected_columns_index, *rid, relative_version)?);
            }
        }
        Ok(result)
    }

    // unique key only
    pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> Result<bool, DbError> {
        let mut table = self.table.lock().unwrap();
        let rid = match table.rid_for_unique_key(key) {
            Ok(rid) => rid,
            _ => return Ok(false),
        };

        let base_addr = table.page_directory.get(rid)?;

        // Get current indirection (points to latest tail, or self if no updates)
        let current_indirection = table
            .page_ranges
            .read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?
            .ok_or(DbError::NullValue(404))?;


        // Build schema encoding for this tail record
        let mut schema_encoding: i64 = 0;
        for (i, val) in record.iter().enumerate() {
            if val.is_some() {
                schema_encoding |= 1 << i;
            }
        }

        // when updating,
        // if key == primary key, return error
        // otherwise, remove from index and add new one

        // remove the previous tail from the index
        let current_values = table.read_latest(rid)?;
        for (i, key) in record.iter().enumerate() {
            if i == table.key_index && record[i] != None {
                return Err(DbError::DuplicateKey(key.unwrap()));
            }
            if key.is_some() {
                if current_values[i].is_some() {
                    table.indices[i].remove(current_values[i].unwrap(), rid);
                }
                table.indices[i].insert(key.ok_or(DbError::NullValue(0))?, rid);
            }
        }

        let next_rid = table.rid.next().unwrap();

        // Append tail record
        let address = table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            Some(schema_encoding),
            rid,
        )?;

        table.page_directory.add(next_rid, address);

        // Update base indirection
        table
            .page_ranges
            .write_indirection(&base_addr, Some(next_rid), WhichRange::Base)?;

        // Check for Merge
        table.tail_count += 1;
        //if table.tail_count % 10 == 0 {
        //    table.merge();
        //}

        Ok(true)
    }

    // unique key only
    pub fn delete(&mut self, key: i64) -> Result<bool, DbError> {
        let mut table = self.table.lock().unwrap();
        let rid = table.rid_for_unique_key(key)?;

        // Remove primary + secondaries from index
        let current_values = table.read_latest(rid)?;

        for (i, key) in current_values.iter().enumerate() {
            table.indices[i].remove(key.unwrap(), rid);
        }

        let base_addr = table.page_directory.get(rid)?;

        let current_indirection = table
            .page_ranges
            .read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?
            .ok_or(DbError::NullValue(404))?;

        // Append deletion tail (schema_encoding = None marks deletion)
        let next_rid = table.rid.next().unwrap();
        let tail_record = vec![None; table.num_columns];
        let address =
            table
                .page_ranges
                .append_tail(tail_record, next_rid, current_indirection, None, rid)?;

        table.page_directory.add(next_rid, address);

        // Update base indirection to point to deletion tail
        table
            .page_ranges
            .write_indirection(&base_addr, Some(next_rid), WhichRange::Base)?;

        // Check for Merge
        table.tail_count += 1;
        //if table.tail_count % 10 == 0 {
        //    table.merge();
        //}

        Ok(true)
    }

    // unique key only
    pub fn sum(&self, start_range: i64, end_range: i64, col: usize) -> Result<i64, DbError> {
        let table = self.table.lock().unwrap();
        if let Some(rids) =
            table.indices[table.key_index].locate_range(start_range, end_range)
        {
            let mut sum: i64 = 0;

            for rid in rids {
                if table.is_deleted(rid)? {
                    continue;
                }
                sum += table
                    .read_latest_single(rid, col)?
                    .ok_or(DbError::NullValue(col))?;
            }
            Ok(sum)
        } else {
            Err(DbError::KeyNotFound(start_range))
        }
    }

    // unique key only
    pub fn sum_version(
        &self,
        start_range: i64,
        end_range: i64,
        col: usize,
        relative_version: i64,
    ) -> Result<i64, DbError> {
        let table = self.table.lock().unwrap();
        if let Some(rids) =
            table.indices[table.key_index].locate_range(start_range, end_range)
        {
            // cumulative sum of all columns
            let mut sum: i64 = 0;

            for rid in rids {
                sum += table
                    .read_version_single(rid,col,relative_version)?
                    .ok_or(DbError::NullValue(col))?;
            }
            Ok(sum)
        }
        else {
            Err(DbError::KeyNotFound(start_range))
        }
    }

    // rid of unique key
    pub fn increment(&mut self, key: i64, col: usize) -> Result<bool, DbError> {
        let (record, key_copy) = {
            let table = self.table.lock().unwrap();

            if col == table.key_index || col >= table.num_columns {
                return Ok(false);
            }

            let rid = table.rid_for_unique_key(key)?;

            let mut record: Vec<Option<i64>> = vec![None; table.num_columns];

            let temp = table
                .read_latest_single(rid, col)?
                .ok_or(DbError::NullValue(col))?
                + 1;

            record[col] = Some(temp);

            (record, key)
        };
        self.update(key, record)
    }
}


//

// TODO m3
// if let Some(indirection_pointer) = table.read_single(rid, record.len() + 2) {
//             // set to previous tail page
//         } else if let Some(indirection_pointer) = table.read_single(rid, record.len() + 1) {
//             // first update --> set to base page
//         } else {
//         }
//         // TODO: UPDATE INDIRECTION COLUMN
//     }

// pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> bool {

//     if let Some(rids) = table.indices[table.key_index].locate(key) {
//         let rid = rids[0];
//         let base_addr = table.page_directory.get(rid);
//         if let indirection = self
//             .table
//             .page_ranges
//             .get_indirection(&base_addr)
//             .unwrap() {

//             } else if table.page_ranges.get_indirection(addr)

//         let mut schema_encoding: i64 =
//             table.read_single(rid, record.len() + IndirectionCol).unwrap_or(0);

//         //Updating index for all value that have been changed
//         for i in 0..record.len() {
//             if record[i].is_some() {
//                 table.indices[i].remove(table.read_single(rid, i).unwrap(), rid);
//                 //Updates schema encoding
//                 table.indices[i].insert(record[i].unwrap(), rid);
//                 schema_encoding |= 1 << i;
//             }
//         }
//         //Appending rid, schema, then the indirection pointer to the end of
//         //  it
//         let next_rid = table.rid.next().unwrap();
//         let address =
//             table
//                 .page_ranges
//                 .append_tail(record, next_rid, indirection, schema_encoding);
//         table.page_directory.add(next_rid, address);
//         let indirection_col = table.num_columns + IndirectionCol;
//         table
//             .page_ranges
//             .write_meta_col(indirection_col, &base_addr, Some(next_rid));
//         return true;
//     };
//     //let key: Option<i64> = record[table.key_index];
//     false
// }
//}


//WAS IN UPDATE:         ////
//
//         // let current_values = table.read_latest(rid)?;
//         // for (i, val) in record.iter().enumerate() {
//         //     if val.is_some() {
//         //         if let Some(old_val) = current_values[i] {
//         //             table.indices[i].remove(old_val, rid);
//         //         }
//         //         table.indices[i].insert(val.ok_or(DbError::NullValue(i))?, rid);
//         //     }
//         // }