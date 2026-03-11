use crate::bufferpool::BufferPool;
use crate::disk_manager::TableCounters;
use crate::errors::DbError;
use crate::index::Index;
use crate::iterators::{AtomicIterator, PhysicalAddress};
use crate::page_collection::MetaPage;
use crate::page_directory::PageDirectory;
use crate::page_range::{PageRanges, WhichRange};
use crate::lock_manager::LockManager;
use dashmap::DashSet;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use crate::merge_worker::MergeWorker;

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
    pub lock_manager: Arc<LockManager>,
    pub tail_append_count: AtomicUsize,
    pub merge_worker: OnceLock<MergeWorker>,
}

impl Table {
    pub const PROJECTED_NUM_RECORDS: usize = 10001;
    pub const NUM_META_PAGES: usize = 4;
    pub const MERGE_TAIL_PAGE_INTERVAL: usize = 10;
    
    pub fn new_no_transaction(
        table_name: String,
        num_columns: usize,
        key_index: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
    ) -> Table {
        Table::new(
            table_name,
            num_columns,
            key_index,
            table_id,
            bufferpool,
            Arc::new(LockManager::new()),
        )
    }
    

    pub fn new(
        table_name: String,
        num_columns: usize,
        key_index: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        lock_manager: Arc<LockManager>,
    ) -> Table {
        let num_total_cols = num_columns + Table::NUM_META_PAGES;
        Self {
            name: table_name,
            page_ranges: PageRanges::new(num_total_cols, table_id, bufferpool),
            page_directory: PageDirectory::default(),
            rid: AtomicIterator::default(),
            key_index,
            num_data_columns: num_columns,
            indices: (0..num_columns)
                .map(|i| {
                    if i == key_index {
                        Index::new_unique()
                    } else {
                        Index::new_non_unique()
                    }
                })
                .collect(),
            table_id,
            num_total_cols,
            dirty_base_rids: DashSet::new(),
            lock_manager,
            tail_append_count: AtomicUsize::new(0),
            merge_worker: OnceLock::new(),
        }
    }

    pub fn restore (
        name: String,
        num_columns: usize,
        key_index: usize,
        table_id: usize,
        bufferpool: Arc<BufferPool>,
        page_dir_pairs: Vec<(i64, PhysicalAddress)>,
        counters: TableCounters,
        primary_pairs: Vec<(i64, i64)>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        let num_total_cols = num_columns + Table::NUM_META_PAGES;

        let indices: Vec<Index> = (0..num_columns).map(|i| {
            if i == key_index {
                Index::new_unique()
            } else {
                Index::new_non_unique()
            }
        }).collect();
        for (key, rid) in primary_pairs {
            indices[key_index].insert(key, rid);
        }

        let page_ranges = PageRanges::restore(
            num_total_cols,
            table_id,
            bufferpool,
            counters.base_collections,
            counters.tail_collections,
            counters.base_next_addr,
            counters.tail_next_addr,
            counters.pid_next_start,

        );

        let page_directory = PageDirectory::restore(page_dir_pairs);

        let rid: AtomicIterator<AtomicI64> = AtomicIterator::default();
        rid.set(counters.next_rid);

        Table {
            name,
            page_ranges,
            page_directory,
            rid,
            key_index,
            num_data_columns: num_columns,
            indices,
            table_id,
            num_total_cols,
            dirty_base_rids: DashSet::new(),
            lock_manager,
            tail_append_count: AtomicUsize::new(0),
            merge_worker: OnceLock::new(),
        }
    }


    pub fn read(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.page_ranges
            .read(&addr)
            .map_err( DbError::Storage)
    }

    pub fn read_single(
        &self,
        rid: i64,
        column: usize,
        range: WhichRange,
    ) -> Result<Option<i64>, DbError> {
        let addr = self.page_directory.get(rid)?;
        self.read_col(column, &addr, range)
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
            .map_err(DbError::Storage)
    }

    pub fn read_latest(&self, rid: i64) -> Result<Vec<Option<i64>>, DbError> {
        self.read_record_internal(rid, 0)
    }

    pub fn read_latest_single(&self, rid: i64, col: usize) -> Result<Option<i64>, DbError> {
        let (base_addr, tps, tail_opt) = self.get_unmerged_tail(rid)?;
        let mut current_tail_rid = match tail_opt {
            Some(r) => r,
            None => {
                return self
                    .page_ranges
                    .read_single(col, &base_addr, WhichRange::Base)
                    .map_err(DbError::Storage);
            }
        };

        loop {
            let tail_addr = self.page_directory.get(current_tail_rid)?;
            let tail_schema = self
                .page_ranges
                .read_meta_col(&tail_addr, MetaPage::SchemaEncoding, WhichRange::Tail)?
                .unwrap_or(0);

            if (tail_schema >> col) & 1 == 1 {
                return self
                    .page_ranges
                    .read_single(col, &tail_addr, WhichRange::Tail)
                    .map_err(DbError::Storage);
            }

            let next_rid = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::Indirection,
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
        let (base_addr, tps, tail_opt) = self.get_unmerged_tail(rid)?;

        if let Some(mut current_tail_rid) = tail_opt {
            loop {
                // Stop walking once we reach merged region
                if current_tail_rid <= tps {
                    break;
                }

                let tail_addr = self.page_directory.get(current_tail_rid)?;
                let tail_schema = self
                    .page_ranges
                    .read_meta_col(&tail_addr, MetaPage::SchemaEncoding, WhichRange::Tail)?
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
                    MetaPage::Indirection,
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
        self.read_col(col, &base_addr, WhichRange::Base)
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
        let skip_count = (-relative_version) as usize;
        let full = self.read_record_internal(rid, skip_count)?;

        Ok(projected
            .iter()
            .enumerate()
            .map(|(col, &flag)| if flag == 1 { full[col] } else { None })
            .collect())
    }

    pub fn is_deleted(&self, rid: i64) -> Result<bool, DbError> {
        let (base_addr, _tps, tail_opt) = self.get_unmerged_tail(rid)?;

        if let Some(tail_rid) = tail_opt {
            let tail_addr = self.page_directory.get(tail_rid)?;
            let schema = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::SchemaEncoding,
                WhichRange::Tail,
            )?;
            Ok(schema.is_none())
        } else {
            let base_schema = self.page_ranges.read_meta_col(
                &base_addr,
                MetaPage::SchemaEncoding,
                WhichRange::Base,
            )?;
            Ok(base_schema.is_none())
        }
    }

    #[inline]
    fn read_record_internal (&self, rid: i64, skip_count: usize) -> Result<Vec<Option<i64>>, DbError> {
        let (base_addr, tps, tail_opt) = self.get_unmerged_tail(rid)?;
        let mut result = self.page_ranges.read_data(&base_addr, self.num_data_columns).map_err(DbError::Storage)?;

        let mut current_tail_rid = match tail_opt {
            Some(tail_rid) => tail_rid,
            None => return Ok(result),
        };

        let mut accumulated_schema: i64 = 0;
        let mut updates_seen = 0;

        loop {
            let tail_addr = self.page_directory.get(current_tail_rid)?;

            if updates_seen >= skip_count {
                self.apply_tail_update(&tail_addr, &mut result, &mut accumulated_schema)?;
            }
            updates_seen += 1;

            let next_rid = self.page_ranges.read_meta_col(
                &tail_addr,
                MetaPage::Indirection,
                WhichRange::Tail,
            )?;

            match next_rid {
                Some(next) if next <= tps => break,
                Some(next) if next == rid => break,
                Some(next) => current_tail_rid = next,
                None => break,
            }
        }
        Ok(result)
    }

    #[inline]
    fn apply_tail_update(
        &self,
        tail_addr: &PhysicalAddress,
        result: &mut Vec<Option<i64>>,
        accumulated_schema: &mut i64,
    ) -> Result<(), DbError> {
        let tail_schema = self
            .page_ranges
            .read_meta_col(&tail_addr, MetaPage::SchemaEncoding, WhichRange::Tail)?
            .unwrap_or(0);

        let new_cols = tail_schema & !*accumulated_schema;
        for (col, schema) in result.iter_mut() .enumerate().take(self.num_data_columns) {
            if (new_cols >> col) & 1 == 1 {
                *schema = self
                    .page_ranges
                    .read_single(col, tail_addr, WhichRange::Tail)?;
            }
        }
        *accumulated_schema |= tail_schema;
        Ok(())
    }

    #[inline]
    fn read_col(&self, col: usize, addr: &PhysicalAddress, range: WhichRange) -> Result<Option<i64>, DbError> {
        self.page_ranges.read_single(col, addr, range).map_err(DbError::Storage)
    }

    #[inline]
    fn get_unmerged_tail(&self, rid: i64) -> Result<(PhysicalAddress, i64, Option<i64>), DbError> {
        let base_addr = self.page_directory.get(rid)?;
        let indirection = self.page_ranges.read_meta_col(
            &base_addr,
            MetaPage::Indirection,
            WhichRange::Base,
        )?;

        let tail_rid = match indirection {
            Some(t) if t != rid => t,
            _ => return Ok((base_addr, 0, None)),
        };

        let tps = self.page_ranges.get_tps(&base_addr);
        if tail_rid <= tps {
            return Ok((base_addr, tps, None));
        }

        Ok((base_addr, tps, Some(tail_rid)))
    }

    pub fn merge_rids(&self, rids: &[i64]) -> Result<(), DbError> {
        for &base_rid in rids {
            self.dirty_base_rids.remove(&base_rid);

            let base_addr = match self.page_directory.get(base_rid) {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            let indirection = match self.page_ranges.read_meta_col(
                &base_addr,
                MetaPage::Indirection,
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
                MetaPage::SchemaEncoding,
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

    pub fn merge(&mut self) -> Result<(), DbError> {
        let dirty: Vec<i64> = self.dirty_base_rids.iter().map(|r| *r).collect();
        self.merge_rids(&dirty)
    }

    /*
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
                MetaPage::Indirection,
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
                MetaPage::SchemaEncoding,
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
     */
}