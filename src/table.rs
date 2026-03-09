use crate::bufferpool::BufferPool;
use crate::errors::DbError;
use crate::index::Index;
use crate::page_collection::MetaPage;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use dashmap::DashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use crate::iterators::AtomicIterator;

pub struct TableMeta {
    pub table_id: usize,
    pub name: String,
    pub num_data_columns: usize,
    pub key_index: usize,
    pub next_rid: i64,
}

pub struct Table {
    pub name: String,
    pub page_ranges: PageRanges,
    pub page_directory: PageDirectory,
    pub rid: AtomicIterator<AtomicI64>,
    pub num_data_columns: usize,
    pub key_index: usize,
    pub indices: Vec<Index>,
    pub table_id: usize,
    pub num_total_cols: usize,
    pub dirty_base_rids: DashSet<i64>,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 4;

    pub fn new(
        table_name: String,
        num_columns: usize,
        key_index: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
    ) -> Table {
        let num_total_cols = num_columns + Table::NUM_META_PAGES;
        Self {
            name: table_name,
            page_ranges: PageRanges::new(num_total_cols, table_id, bufferpool),
            page_directory: PageDirectory::default(),
            rid: AtomicIterator::default(),
            key_index,
            num_data_columns: num_columns,
            indices: (0..1).map(|_| Index::new()).collect(),
            table_id,
            num_total_cols,
            dirty_base_rids: DashSet::new(),
        }
    }

    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges
            .read(&addr)
            .map_err(|e| DbError::Storage(e))
    }
    
    
    pub fn read_single(
        &self,
        rid: i64,
        column: usize,
        range: WhichRange,
    ) -> Result<Option<i64>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges
            .read_single(column, &addr, range)
            .map_err(|e| DbError::Storage(e))
    }

    pub fn rid_for_key(&self, key: i64) -> Result<i64, DbError> {
        self.indices[self.key_index]
            .locate(key)
            .ok_or(DbError::KeyNotFound(key))
    }

    pub fn read_projected(&self, projected: &[i64], rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges
            .read_projected(projected, &addr)
            .map_err(|e| DbError::Storage(e))
    }

    pub fn read_latest(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let mut result = self.read(rid)?;

        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        let tail_rid = match indirection {
            Some(ind) if ind != rid => ind,
            _ => return Ok(result), //No updates
        };

        let tps = self.page_ranges.get_tps(&base_addr);

        //base page already merged/up to date
        if tail_rid <= tps {
            return Ok(result);
        }

        let mut current_tail_rid = tail_rid;
        let mut accumulated_schema: i64 = 0;

        loop {
            let tail_addr = self.page_directory.get(current_tail_rid)?;
            let tail_schema = self
                .page_ranges
                .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                .unwrap_or(0);

            let new_cols = tail_schema & !accumulated_schema;
            for col in 0..self.num_data_columns {
                if (new_cols >> col) & 1 == 1 {
                    result[col] = self
                        .page_ranges
                        .read_single(col, &tail_addr, WhichRange::Tail)?;
                }
            }
            accumulated_schema |= tail_schema;

            let next_rid = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::IndirectionCol,
                WhichRange::Tail,
            )?;
            match next_rid {
                Some(next) if next == rid => break,
                Some(next) if next <= tps => break,
                Some(next) => current_tail_rid = next,
                None => break,
            }
        }

        Ok(result)
    }

    pub fn read_latest_single(&self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        let tail_rid = match indirection {
            Some(ind) if ind != rid => ind,
            _ => return self.page_ranges
                .read_single(col, &base_addr, WhichRange::Base)
                .map_err(DbError::Storage),
        };

        let tps = self.page_ranges.get_tps(&base_addr);
        
        if tail_rid <= tps {
            return self.page_ranges
                .read_single(col, &base_addr, WhichRange::Base)
                .map_err(DbError::Storage);
        }

        let mut current_tail_rid = tail_rid;
        loop {
            let tail_addr = self.page_directory.get(current_tail_rid)?;
            let tail_schema = self
                .page_ranges
                .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                .unwrap_or(0);

            if (tail_schema >> col) & 1 == 1 {
                return self
                    .page_ranges
                    .read_single(col, &tail_addr, WhichRange::Tail)
                    .map_err(DbError::Storage);
            }

            let next_rid = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::IndirectionCol,
                WhichRange::Tail,
            )?;
            match next_rid {
                Some(next) if next == rid => break,
                Some(next) if next <= tps => break,
                Some(next) => current_tail_rid = next,
                None => break,
            }
        }

        self.page_ranges
            .read_single(col, &base_addr, WhichRange::Base)
            .map_err(DbError::Storage)
    }


    pub fn read_version_single(
        &self,
        rid: i64,
        col: usize,
        mut relative_version: i64,
    ) -> Result<Option<i64>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        if let Some(tail_rid) = indirection && tail_rid != rid {
            let tps = self.page_ranges.get_tps(&base_addr);
            let mut current_tail_rid = tail_rid;

            loop {
                // Stop walking once we reach merged region
                if current_tail_rid <= tps {
                    break;
                }

                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0);

                if (tail_schema >> col) & 1 == 1 {
                    relative_version += 1;
                }
                if relative_version > 0 {
                    return self
                        .page_ranges
                        .read_single(col, &tail_addr, WhichRange::Tail)
                        .map_err(DbError::Storage);
                }

                let next_rid = self.page_ranges.read_meta_col(
                    &tail_addr,
                    MetaPage::IndirectionCol,
                    WhichRange::Tail,
                )?;
                match next_rid {
                    Some(next) if next == rid => break,
                    Some(next) => current_tail_rid = next,
                    None => break,
                }
            }
        }

        // Fall back to base page (covers no-tail and merged-region cases)
        self.page_ranges
            .read_single(col, &base_addr, WhichRange::Base)
            .map_err(DbError::Storage)
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
        relative_version: i64,
    ) -> Result<Vec<Option<i64>>, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let mut result = self.read(rid)?;

        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        if let Some(tail_rid) = indirection
            && tail_rid != rid
        {
            let tps = self.page_ranges.get_tps(&base_addr);

            // Collect only the unmerged portion of the tail chain (> TPS)
            let mut tail_chain: Vec<i64> = Vec::new();
            let mut current = tail_rid;
            loop {
                if current <= tps {
                    break; // the rest is already in the base page
                }
                tail_chain.push(current);
                let tail_addr = self.page_directory.get(current)?;
                let next = self.page_ranges.read_meta_col(
                    &tail_addr,
                    MetaPage::IndirectionCol,
                    WhichRange::Tail,
                )?;
                match next {
                    Some(n) if n == rid => break,
                    Some(n) => current = n,
                    None => break,
                }
            }

            let skip = (-relative_version) as usize;
            let applicable = if skip >= tail_chain.len() {
                &[] as &[i64]
            } else {
                &tail_chain[skip..]
            };

            let mut accumulated_schema: i64 = 0;
            for &t_rid in applicable {
                let tail_addr = self.page_directory.get(t_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncodingCol, WhichRange::Tail)?
                    .unwrap_or(0);
                let new_cols = tail_schema & !accumulated_schema;
                for col in 0..self.num_data_columns {
                    if (new_cols >> col) & 1 == 1 {
                        result[col] =
                            self.page_ranges.read_single(col, &tail_addr, WhichRange::Tail)?;
                    }
                }
                accumulated_schema |= tail_schema;
            }
        }

        Ok(projected
            .iter()
            .enumerate()
            .map(|(col, &flag)| if flag == 1 { result[col] } else { None })
            .collect())
    }

    pub fn is_deleted(&self, rid: i64) -> Result<bool, DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::IndirectionCol,
            WhichRange::Base,
        )?;

        let tail_rid = match indirection {
            Some(ind) if ind != rid => ind,
            _ => return Ok(false), 
        };

        let tps = self.page_ranges.get_tps(&base_addr);
        
        if tail_rid <= tps {
            let base_schema = self.page_ranges.read_meta_col(
                &base_addr,
                MetaPage::SchemaEncodingCol,
                WhichRange::Base,
            )?;
            return Ok(base_schema.is_none());
        }

        let tail_addr = self.page_directory.get(tail_rid)?;
        let schema = self.page_ranges.read_meta_col(
            &tail_addr,
            MetaPage::SchemaEncodingCol,
            WhichRange::Tail,
        )?;
        Ok(schema.is_none())
    }

    pub fn merge(&mut self) -> Result<(), DbError> {
        let dirty: Vec<i64> = self.dirty_base_rids.iter().map(|r| *r).collect();

        for &base_rid in &dirty {
            self.dirty_base_rids.remove(&base_rid);

            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            // Only process records that have an unmerged tail chain
            let indirection = match self.page_ranges.read_meta_col(
                &base_addr,
                MetaPage::IndirectionCol,
                WhichRange::Base,
            )? {
                Some(ind) if ind != base_rid => ind,
                _ => continue,
            };


            let tail_addr = match self.page_directory.get(indirection) {
                Ok(addr) => addr,
                Err(_) => continue,
            };
            let latest_schema = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::SchemaEncodingCol,
                WhichRange::Tail,
            )?;

            let (consolidated_data, new_schema) = if latest_schema.is_none() {
                (vec![None; self.num_data_columns], None)
            } else {
                let latest = self.read_latest(base_rid)?;
                (latest[..self.num_data_columns].to_vec(), Some(0i64))
            };

            let new_addr = self.page_ranges.append_base_merged(
                consolidated_data,
                base_rid,
                indirection,
                new_schema,
            )?;

            self.page_directory.add(base_rid, new_addr);

            self.page_ranges.update_tps(&new_addr, indirection);
        }
        Ok(())
    }
}