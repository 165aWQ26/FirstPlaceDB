use crate::error::DbError;
use crate::index::Index;
use crate::page_collection::MetaPage;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use std::collections::HashSet;
pub struct Table {
    pub name: String,

    pub page_ranges: PageRanges,

    pub page_directory: PageDirectory,

    pub rid: std::ops::RangeFrom<i64>,

    pub num_columns: usize,

    pub key_index: usize,

    pub indices: Vec<Index>,

    pub tail_count: usize,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 5;
    //data_pages_per_collection is the total number of pages in a PageDirectory
    pub fn new(
        table_name: String,
        num_columns: usize,
        key_index: usize,
    ) -> Table {
        Self {
            name: table_name,
            page_ranges: PageRanges::new(num_columns),
            page_directory: PageDirectory::default(),
            rid: 0..,
            key_index,
            num_columns,
            indices: (0..num_columns).map(|_| Index::new()).collect(),
            tail_count: 0,
        }
    }
    /// Returns all the columns of the record
    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read(&addr)
    }

    /// Like read but you choose col
    pub fn read_single(&self, rid: i64, column: usize, range: WhichRange) -> Result<Option<i64>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read_single(column, &addr, range)
    }

    // Use primary index to find the rid
    pub fn rid_for_unique_key(&self, key: i64) -> Result<i64, DbError> {
        Some(self.indices[self.key_index]
            .locate(key)
            .unwrap()[0])
            .ok_or(DbError::KeyNotFound(key))
    }

    // get all rids for a key
    pub fn rids_for_key(&self, key: i64, search_key_index: usize) -> Result<&Vec<i64>, DbError> {
        self.indices[search_key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))
    }

    pub fn read_projected(&self, projected: &[i64], rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges.read_projected(projected, &addr)
    }

    pub fn read_latest(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let mut result = self.read(rid)?;
        let indirection = self.page_ranges.read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?;

        // If indirection is None, no updates
        match indirection {
            Some(ind_rid) if ind_rid == rid => Ok(result),
            None => Ok(result),
            Some(tail_rid) => {
                // check if already merged
                if tail_rid <= self.page_ranges.base.tps[base_addr.collection_num] {
                    let mut current_tail_rid = tail_rid;
                    let mut accumulated_schema: i64 = 0;
                    loop {
                        let tail_addr = self.page_directory.get(current_tail_rid)?;
                        let tail_schema = self.page_ranges
                            .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                            .unwrap_or(0);
                        let new_cols = tail_schema & !accumulated_schema;
                        for col in 0..self.num_columns {
                            if (new_cols >> col) & 1 == 1 {
                                result[col] = self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail)?;
                            }
                        }
                        accumulated_schema |= tail_schema;
                        let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
                        match next_rid {
                            Some(next) if next == rid => break,
                            Some(next) if next <= self.page_ranges.base.tps[base_addr.collection_num] => current_tail_rid = next,
                            _ => break,
                        }
                    }
                    return Ok(result);
                }
                let mut current_tail_rid = tail_rid;
                let mut accumulated_schema: i64 = 0;
                loop {
                    let tail_addr = self.page_directory.get(current_tail_rid)?;
                    let tail_schema = self
                        .page_ranges
                        .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                        .unwrap_or(0); // None = deletion tail, no columns updated

                    // Columns updated in this tail but not yet seen in newer tail
                    let new_cols = tail_schema & !accumulated_schema;

                    for col in 0..self.num_columns {
                        if (new_cols >> col) & 1 == 1 {
                            result[col] = self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail)?;
                        }
                    }
                    
                    accumulated_schema |= tail_schema;

                    // Follow indirection to next (older tail record)
                    let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
                    match next_rid {
                        Some(next) if next == rid => break,
                        Some(next) => current_tail_rid = next,
                        None => break,
                    }
                }
                Ok(result)
            }
        }
    }

    pub fn read_latest_single(&self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?;

        match indirection {
            Some(ind_rid) if ind_rid == rid => self.page_ranges.read_single(col, &base_addr, WhichRange::Base),
            None => self.page_ranges.read_single(col, &base_addr, WhichRange::Base),
            Some(tail_rid) => {
                // Check if already merged
                if tail_rid <= self.page_ranges.base.tps[base_addr.collection_num] {
                    let tps = self.page_ranges.base.tps[base_addr.collection_num];
                    let mut current_tail_rid = tail_rid;
                    loop {
                        let tail_addr = self.page_directory.get(current_tail_rid)?;
                        let tail_schema = self.page_ranges
                            .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                            .unwrap_or(0);
                        if (tail_schema >> col) & 1 == 1 {
                            return self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail);
                        }
                        let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
                        match next_rid {
                            Some(next) if next == rid => break,
                            Some(next) if next <= tps => current_tail_rid = next,
                            _ => break,
                        }
                    }
                    return self.page_ranges.read_single(col, &base_addr, WhichRange::Base);
                }

                let mut current_tail_rid = tail_rid;
                loop {
                    let tail_addr = self.page_directory.get(current_tail_rid)?;
                    let tail_schema = self
                        .page_ranges
                        .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                        .unwrap_or(0); // None = deletion tail, no columns updated

                    //Case where latest update if found
                    if (tail_schema >> col) & 1 == 1 {
                        // Newest tail that updates this column
                        return self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail);
                    }

                    let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
                    match next_rid {
                        Some(next) if next == rid => break,
                        Some(next) => current_tail_rid = next,
                        None => break,
                    }
                }
                // Column never updated in any tail
                self.page_ranges.read_single(col, &base_addr, WhichRange::Base)
            }
        }
    }

    /* Old read_version_single
    pub fn read_version_single(&self, rid: i64, col: usize, mut relative_version: i64) -> Result<Option<i64>, DbError> {

        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?;
        if let Some(tail_rid) = indirection && tail_rid != rid {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0); // None = deletion tail, no columns updated

                if (tail_schema >> col) & 1 == 1 {
                    relative_version += 1;
                    if relative_version == 0 {
                        return self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail);
                    }
                }

                let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
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
        // add back return if neccessary
        self.page_ranges.read_single(col, &base_addr, WhichRange::Base)
    }
     */
    
    // merge based read_version_single
    pub fn read_version_single(&self, rid: i64, col: usize, mut relative_version: i64) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?;

        if let Some(tail_rid) = indirection && tail_rid != rid {
            let mut current_tail_rid = tail_rid;
            loop {
                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0);

                if (tail_schema >> col) & 1 == 1 {
                    if relative_version == 0 {
                        return self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail);
                    }
                    relative_version += 1;
                }

                let next_rid = self.page_ranges.read_meta_col(&tail_addr, MetaPage::IndirectionCol, WhichRange::Tail)?;
                match next_rid {
                    Some(next) if next == rid => break,
                    Some(next) => current_tail_rid = next,
                    None => break,
                }
            }
        }

        self.page_ranges.read_single(col, &base_addr, WhichRange::Base)
    }
    
    pub fn read_latest_projected(
        &self,
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
        &self,
        projected: &[i64],
        rid: i64,
        relative_version: i64
    )-> Result<Vec<Option<i64>>, DbError>{
        let mut ans : Vec<Option<i64>> = Vec::new(); 
        for (col,value) in projected.iter().enumerate(){
            if *value == 1{
                ans.push(self.read_version_single(rid,col,relative_version)?);
            }
            else {
                ans.push(None);
            }
        }
        Ok(ans)
    }

    /// Check if a base RID's latest tail has schema_encoding == None (deletion marker).
    pub fn is_deleted(&self, rid: i64) -> Result<bool, DbError> {
        let base_addr = self.page_directory.get(rid)?;

        //check if alr marked deleted via merge
        let base_schema = self.page_ranges.read_meta_col(&base_addr, MetaPage::SchemaEncodingCol, WhichRange::Base)?;
        // TODO: add sentinel val to track if page is deleted
        if base_schema == Some(-1){
            return Ok(true);
        }

        let indirection = self.page_ranges.read_meta_col(&base_addr, MetaPage::IndirectionCol, WhichRange::Base)?;
        match indirection {
            None => Ok(false),
            Some(ind_rid) if ind_rid == rid => Ok(false),
            Some(tail_rid) => {
                let tail_addr = self.page_directory.get(tail_rid)?;
                let schema = self.page_ranges.read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?;
                Ok(schema.is_none())
            }
        }
    }

    // TODO do for m2 -- what the helly do these do
    // pub fn create_index() {

    // }

    // pub fn drop_index() {

    // }

    pub fn merge(&mut self) {
        let mut merged_base = self.page_ranges.base.clone_range();

        let mut seen: HashSet<i64> = HashSet::new();
        let mut max_tail_per_collection: std::collections::HashMap<usize, i64> = std::collections::HashMap::new();
        let max_rid = self.rid.start - 1;

        for tail_rid in (0..=max_rid).rev() {
            let tail_addr = match self.page_directory.get(tail_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            let base_rid = match self.page_ranges.read_meta_col(&tail_addr, MetaPage::BaseRidCol, WhichRange::Tail) {
                Ok(Some(base)) => base,
                _ => continue,
            };

            if base_rid >= tail_rid {
                continue;
            }

            if seen.contains(&base_rid) {
                continue;
            }
            seen.insert(base_rid);

            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            let schema = match self.page_ranges.read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail) {
                Ok(val) => val,
                Err(_) => continue,
            };

            if schema.is_none() {
                let meta_col = self.num_columns + MetaPage::SchemaEncodingCol as usize;
                merged_base[base_addr.collection_num].pages[meta_col]
                    .update(base_addr.offset, Some(-1))
                    .unwrap();
            }
            /*
            else {
                let schema_bits = schema.unwrap();
                for col in 0..self.num_columns {
                    if (schema_bits >> col) & 1 == 1 {
                        // read from live tail
                        let val = self.page_ranges
                            .read_single(col, &tail_addr, WhichRange::Tail)
                            .unwrap();
                        // write into clone, not live base
                        merged_base[base_addr.collection_num].pages[col]
                            .update(base_addr.offset, val)
                            .unwrap();
                    }
                }
            }
             */

            // update indirection in clone to point to this tail
            // --> read_latest can use TPS shortcut correctly
            let indir_col = self.num_columns + MetaPage::IndirectionCol as usize;
            merged_base[base_addr.collection_num].pages[indir_col]
                .update(base_addr.offset, Some(tail_rid))
                .unwrap();

            max_tail_per_collection
                .entry(base_addr.collection_num)
                .and_modify(|v| *v = (*v).max(tail_rid))
                .or_insert(tail_rid);
        }

        self.page_ranges.base.swap_range(merged_base);

        // grow tps to match new range length if needed
        while self.page_ranges.base.tps.len() < self.page_ranges.base.range_len() {
            self.page_ranges.base.tps.push(i64::MAX);
        }

        for (collection_num, max_rid) in max_tail_per_collection {
            self.page_ranges.base.tps[collection_num] = max_rid;
        }
    }
}
