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

    //Need to get the values
    pub fn insert(&mut self, mut record: Vec<Option<i64>>) -> bool {
        let rid = self.table.rid.next().unwrap();

        let key: Option<i64> = record[self.table.key_index];

        if self.table.indices[self.table.key_index]
            .locate(key.unwrap())
            .is_some()
        {
            return false;
        }
        //Update indices
        for i in 0..record.len() {
            self.table.indices[i].insert(record[i].unwrap(), rid);
        }
        // Write record
        record.push(Some(rid));
        record.push(Query::DEFAULT_INDIRECTION);
        record.push(Query::DEFAULT_SCHEMA_ENCODING);

        //Write record
        let address = self.table.page_ranges.base.append(record);

        //Add to page directory
        self.table.page_directory.add(rid, address);

        true
    }

    pub fn select(
        &self,
        key: i64,
        search_key_index: usize,
        projected_columns_index: &mut [i64],
    ) -> Result<Vec<Vec<Option<i64>>>, bool> {
        if let Some(rids) = self.table.indices[search_key_index].locate(key) {
            let mut records: Vec<Vec<Option<i64>>> = Vec::new();

            for rid in rids {
                //logic to sub None for col. dropping
                records.push(
                    self.table
                        .read_latest_projected(projected_columns_index, *rid),
                );
            }

            Ok(records)
        } else {
            Err(false)
        }
    }
    // TODO m3
    //     pub fn select_version(&self, key: i64, search_key_index:usize,
    //                   projected_columns_index: &mut [i64], relative_version:i64) -> Result<Vec<Record>, bool> {

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
    pub fn update(&mut self, key: i64, record: Vec<Option<i64>>) -> bool {
        let rid = match self.table.indices[self.table.key_index].locate(key) {
            Some(rid) => rid[0],
            None => return false,
        };

        let base_addr = self.table.page_directory.get(rid);

        // Get current indirection (points to latest tail, or self if no updates)
        let current_indirection = self
            .table
            .page_ranges
            .get_indirection(&base_addr)
            .unwrap_or(rid);

        // Build schema encoding for this tail record
        let mut schema_encoding: i64 = 0;
        for i in 0..record.len() {
            if record[i].is_some() {
                schema_encoding |= 1 << i;
            }
        }

        // remove the previous tail from the index
        for i in 0..record.len() {
            if record[i].is_some() {
                if let Some(old_val) = self.table.read_latest_single(rid, i) {
                    self.table.indices[i].remove(old_val, rid);
                }
                self.table.indices[i].insert(record[i].unwrap(), rid);
            }
        }

        let next_rid = self.table.rid.next().unwrap();

        // Append tail record
        let address = self.table.page_ranges.append_tail(
            record,
            next_rid,
            current_indirection,
            schema_encoding,
        );

        self.table.page_directory.add(next_rid, address);

        // Update base indirection
        let indirection_col = self.table.num_columns + INDIRECTION_COL;
        self.table
            .page_ranges
            .write_single(indirection_col, &base_addr, Some(next_rid));

        true
    }

    pub fn delete(&mut self, key: i64) -> bool {
        // Only
        //update() with only null values

        // let schema_encoding: i64 = 0;
        // let v: Vec<Option<i64>> = vec![None; self.table.num_columns];

        // if let Some(rid) = self.table.indices[self.table.key_index].locate(key) {
        //     let rid = rid[0];

        //     for i in 0..self.table.num_columns {
        //         if let Some(val) = self.table.read_single(rid, i) {
        //             self.table.indices[i].remove(val, rid);
        //         }
        //     }
        //     return true;
        // }
        // return false;

        if let Some(rid) = self.table.indices[self.table.key_index].locate(key) {
            let rid = rid[0];
            let record: Vec<Option<i64>> = vec![None; self.table.num_columns];

            for i in 0..self.table.num_columns {
                if let Some(val) = self.table.read_single(rid, i) {
                    self.table.indices[i].remove(val, rid);
                }
            }
            return self.update(key, record);
        }
        false
    }

    pub fn sum(&self, start_range: i64, end_range: i64, column: usize) -> Result<i64, bool> {
        if let Some(rids) =
            self.table.indices[self.table.key_index].locate_range(start_range, end_range)
        {
            let mut sum: i64 = 0;

            for rid in rids {
                sum += self.table.read_latest_single(rid, column).unwrap();
            }
            Ok(sum)
        } else {
            Err(false)
        }
    }

    /*
    pub fn sum_version(&self, search_key:i64, search_key_index:i64,
                            projected_columns_index:i64, relative_version:i64){

    }
    */

    pub fn increment(&mut self, key: i64, column: usize) -> bool {
        let rid = self.table.indices[self.table.key_index]
            .locate(key)
            .unwrap()[0];

        let mut record: Vec<Option<i64>> = vec![None; self.table.num_columns];

        let temp: i64 = self.table.read_latest_single(rid, column).unwrap() + 1;
        record[column] = Some(temp);

        return self.update(key, record);
    }
}
