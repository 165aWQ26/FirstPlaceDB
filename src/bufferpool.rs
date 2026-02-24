use crate::bufferpool_context::{PageLocation, TableContext};
use crate::page::{Page, PageError};
use crate::page_range::{PhysicalAddress, WhichRange};
use crate::table::Table;
use lru::LruCache;
use rustc_hash::FxHashMap;
use std::convert::Into;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroUsize;
use std::path::Path;

pub enum MetaPage {
    RidCol = 0,
    IndirectionCol = 1,
    SchemaEncodingCol = 2,
    StartTimeCol = 3,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BufferPoolError {
    ZeroPid,
    Full,
    PidNotInFrame,
    DiskWriteFail,
    DiskReadFail,
    BufferPoolWriteFail,
    InvalidLength,
}

//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now, we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct BufferPool {
    cache: LruCache<Pid, Page>,
    size: usize,
    table_names: Vec<String>,
}

impl BufferPool {
    pub const NUMBER_OF_FRAMES: usize = 32;

    #[inline]
    pub fn append_name(&mut self, name: String) {
        self.table_names.push(name);
    }

    //done
    #[inline]
    pub fn write_col(&mut self, pid: Pid, val: Option<i64>) -> Result<(), PageError> {
        if let Some(page) = self.cache.get_mut(&pid) {
            page.write(val)?;
        } else {
            return Err(PageError::PageNotFound(pid));
        }
        Ok(())
    }

    //done
    #[inline]
    pub fn read_col(
        &mut self,
        col: usize,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<Option<i64>, PageError> {
        let pid = make_pid(col, page_location, table_ctx);
        // If page is not in cache, unwrap can crash the entire program.
        // lazy_guarantee_page() ensures the page is in the bufferpool before access.
        self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)
            .map_err(|_| PageError::IndexOutOfBounds(page_location.addr.offset))?;
        self.cache
            .get(&pid)
            .unwrap()
            .read(page_location.addr.offset)
    }

    //done
    #[inline]
    pub fn update_meta_col(
        &mut self,
        val: Option<i64>,
        col_type: MetaPage,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<(), PageError> {
        match col_type {
            MetaPage::IndirectionCol => {
                let col: usize = table_ctx.total_cols - Table::NUM_META_PAGES + col_type as usize;
                let pid = make_pid(col, page_location, table_ctx);
                // See read_col
                self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)
                    .map_err(|_| PageError::IndexOutOfBounds(page_location.addr.offset))?;

                // let page = self.cache.write().get(&pid).unwrap();
                //
                // page.set_dirty(true);
                // page.update(addr.offset, val)?;
                let page = self.cache.get_mut(&pid).unwrap();
                page.set_dirty(true);
                page.update(page_location.addr.offset, val)?;

                Ok(())
            }
            _ => Err(PageError::UpdateNotAllowed),
        }
    }

    // Returns a reference to the metadata page at the given column index
    // done
    #[inline]
    fn meta_record(&mut self, pid: Pid) -> &Page {
        self.cache.get(&pid).unwrap()
    }

    //done
    #[inline]
    pub fn read_meta_col(
        &mut self,
        col_type: MetaPage,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<Option<i64>, PageError> {
        let col: usize = table_ctx.total_cols - Table::NUM_META_PAGES + col_type as usize;
        let pid = make_pid(col, page_location, table_ctx);

        self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)
            .map_err(|_| PageError::IndexOutOfBounds(page_location.addr.offset))?;
        self.cache
            .get(&pid)
            .unwrap()
            .read(page_location.addr.offset)
    }

    //done
    pub fn is_full(&self) -> bool {
        self.size == BufferPool::NUMBER_OF_FRAMES
    }

    //done
    pub fn make_path(
        &self,
        addr: &PhysicalAddress,
        pid: Pid,
        table_cxt: &TableContext,
    ) -> Result<String, BufferPoolError> {
        let pid = pid.get_pid();
        if pid == 0 {
            return Err(BufferPoolError::ZeroPid);
        }

        let mut path = table_cxt.path.clone(); // ./ECS165/

        if pid < 0 {
            path.push_str("tp_");
        } else {
            path.push_str("bp_");
        }
        path.push_str(addr.collection_num.to_string().as_str());
        path.push_str("_");
        path.push_str(
            ((pid.abs() - 1) % (table_cxt.total_cols as i64))
                .to_string()
                .as_str(),
        );

        Ok(path)
    }

    pub fn write_i64(
        &self,
        val: impl Into<Option<i64>>,
        writer: &mut BufWriter<File>,
    ) -> Result<(), BufferPoolError> {
        const SENTINEL: i64 = i64::MAX;
        // let bytes = val.to_be_bytes();
        // writer
        //     .write_all(&bytes)
        //     .map_err(|_| BufferPoolError::DiskWriteFail)?;
        let bytes = match val.into() {
            Some(v) => v.to_be_bytes(),
            None => SENTINEL.to_be_bytes(),
        };
        writer
            .write_all(&bytes)
            .map_err(|_| BufferPoolError::DiskWriteFail)
    }

    pub fn write_to_disk(
        &mut self,
        page: &Page,
        addr: &PhysicalAddress,
        pid: Pid,
        table_ctx: &TableContext,
    ) -> Result<(), BufferPoolError> {
        // write
        let file_path = self.make_path(addr, pid, table_ctx)?;
        let file = File::create(&file_path).map_err(|_| BufferPoolError::DiskWriteFail)?;

        let mut writer = BufWriter::new(file);

        for value in page.iter() {
            self.write_i64(*value, &mut writer)?;
        }

        writer.flush().map_err(|_| BufferPoolError::DiskWriteFail)?;

        Ok(())
    }

    pub fn read_from_disk(
        &mut self,
        address: &PhysicalAddress,
        pid: Pid,
        table_ctx: &TableContext,
    ) -> Result<(), BufferPoolError> {
        if self.is_full() {
            self.evict(table_ctx)?;
        }

        let file_path = self.make_path(address, pid, table_ctx)?;
        let file = File::open(&file_path).map_err(|_| BufferPoolError::DiskReadFail)?;

        let mut reader = BufReader::new(file);
        let mut page = Page::default();

        let mut buffer = [0u8; 8];

        while let Ok(()) = self.read_bytes(&mut buffer, &mut reader) {
            let value = i64::from_be_bytes(buffer);
            page.write(Some(value))
                .map_err(|_e| BufferPoolError::DiskReadFail)?;
        }

        self.cache.push(pid, page);
        self.size += 1;

        Ok(())
    }
    pub fn read_usize(
        &mut self,
        buffer: &mut [u8],
        reader: &mut BufReader<File>,
    ) -> Result<usize, BufferPoolError> {
        self.read_bytes(buffer, reader)?;

        // 1. Ensure the slice is converted to a fixed-size array of 8 bytes
        // 2. Map the error to your custom BufferPoolError if it's not the right length
        let bytes: [u8; 8] = buffer[..8]
            .try_into()
            .map_err(|_| BufferPoolError::InvalidLength)?;

        let value = i64::from_be_bytes(bytes);
        Ok(value as usize)
    }

    pub fn read_bytes(
        &mut self,
        buffer: &mut [u8],
        reader: &mut BufReader<File>,
    ) -> Result<(), BufferPoolError> {
        reader
            .read_exact(buffer)
            .map_err(|_| BufferPoolError::DiskReadFail)
    }
    pub fn is_dirty(&self, page: Page) -> bool {
        page.is_dirty()
    }
    /// Kicks last page from the cache out of the bufferpool and onto disk
    /// If the file path is not set, the db is assumed to be in memory only.
    /// evict() will not write to disk but instead do only the removal step.
    pub fn evict(&mut self, table_ctx: &TableContext) -> Result<(), BufferPoolError> {
        // We only want to write to disk if the page is dirty.
        let (pid, page) = self.cache.pop_lru().unwrap();
        if page.is_dirty() && !table_ctx.path.is_empty() {
            let collection_num =
                { pid.get_pid().unsigned_abs() as usize - 1 } / table_ctx.total_cols;
            let addr = PhysicalAddress {
                offset: 0,
                collection_num,
            };
            self.write_to_disk(&page, &addr, pid, table_ctx)?;
        }
        self.size -= 1;
        Ok(())
    }

    pub fn evict_all(&mut self, tables: &FxHashMap<String, Table>) -> Result<(), BufferPoolError> {
        while self.size != 0 {
            //offset no
            //addr.offset = self.cache.get(&pid).unwrap().len() - 1;
            let (pid, page) = self.cache.pop_lru().unwrap();

            let table_ctx = &tables
                .get(&self.table_names[pid.get_table_id()])
                .unwrap()
                .table_ctx;
            let addr = PhysicalAddress {
                offset: 0,
                collection_num: (pid.0.abs() - 1) as usize / table_ctx.total_cols,
            };
            if page.is_dirty() {
                self.write_to_disk(&page, &addr, pid, table_ctx)?;
            }
            self.size -= 1;
        }
        Ok(())
    }

    pub fn on_disk(&self, addr: &PhysicalAddress, pid: Pid, table_ctx: &TableContext) -> bool {
        let path = self.make_path(addr, pid, table_ctx);

        Path::new(&path.unwrap()).exists()
    }

    /// Creates a new blank page at the NEXT pid (incremented by total_cols).
    ///
    /// This is used when a page exists in the cache but is full — we need
    /// to advance to the next page in the column's page chain without
    /// clobbering the existing full page.
    ///
    /// Previously, this function pushed at the current pid THEN incremented,
    /// which overwrote the existing cached page with a blank one, losing
    /// any dirty writes. The increment was also local (mut pid by value),
    /// so the caller never saw the new pid.
    ///
    /// Fix: increment FIRST, push at the new pid, and return it so the
    /// caller knows where the new page lives.
    pub fn create_blank_page(
        &mut self,
        pid: Pid,
        table_ctx: &TableContext,
    ) -> Result<Pid, BufferPoolError> {
        if self.is_full() {
            self.evict(table_ctx)?
        }
        let mut new_pid = pid;
        increment_pid(&mut new_pid, table_ctx.total_cols as i64)?;
        self.cache.push(new_pid, Page::default());
        self.size += 1;
        Ok(new_pid)
    }

    /// Ensures a usable page exists in the buffer pool for the given pid.
    ///
    /// Four cases:
    ///   1. Page in cache + has capacity → return it, nothing to do
    ///   2. Page in cache + full → create a NEW page at the next pid
    ///      (don't clobber the full page)
    ///   3. Page not in cache + exists on disk → load it
    ///   4. Page not in cache + not on disk → create blank at current pid
    ///
    /// Previous bugs this addresses:
    ///   - Old code returned early ONLY when the page was full (inverted
    ///     condition), so pages with capacity fell through to read_from_disk
    ///     or create_blank_page, which overwrote the cached page via
    ///     LruCache::push with the same key.
    ///   - create_blank_page pushed at the current pid before incrementing,
    ///     so the "full page" branch also clobbered the existing page.
    ///   - Pages not in the buffer pool were never loaded, causing panics
    ///     on reads.
    #[inline]
    pub fn lazy_guarantee_page(
        &mut self,
        addr: &PhysicalAddress,
        pid: Pid,
        table_ctx: &TableContext,
    ) -> Result<Pid, BufferPoolError> {
        if self.cache.contains(&pid) {
                return Ok(pid);
        }

        if self.on_disk(addr, pid, table_ctx) {
            self.read_from_disk(addr, pid, table_ctx)?;
            return Ok(pid);
        } else {
            // Not on disk
            if self.is_full() {
                self.evict(table_ctx)?;
            }
            self.cache.push(pid, Page::default());
            self.size += 1;
        }
        Ok(pid)
    }

    #[inline]
    pub fn append(
        &mut self,
        all_data: Vec<Option<i64>>,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        for (i, val) in all_data.into_iter().enumerate() {
            let pid = make_pid(i, page_location, table_ctx);
            let pid = self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)?;

            self.write_col(pid, val)
                .map_err(|_| BufferPoolError::BufferPoolWriteFail)?;
        }
        Ok(page_location.addr)
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(BufferPool::NUMBER_OF_FRAMES).unwrap()),
            size: 0,
            table_names: vec![],
        }
    }
}

/// Pid, address
pub type Pid = (i64, usize);

trait PidExt {
    fn get_pid(&self) -> i64;
    fn get_table_id(&self) -> usize;
}

impl PidExt for Pid {
    #[inline]
    fn get_pid(&self) -> i64 {
        self.0
    }
    #[inline]
    fn get_table_id(&self) -> usize {
        self.1
    }
}

pub fn make_pid(col: usize, page_location: &PageLocation, table_context: &TableContext) -> Pid {
    let pid = ((col + table_context.total_cols * page_location.addr.collection_num) + 1) as i64;
    if page_location.range == WhichRange::Tail {
        (-pid, table_context.table_id)
    } else {
        (pid, table_context.table_id)
    }
}

pub fn increment_pid(pid: &mut Pid, step: i64) -> Result<Pid, BufferPoolError> {
    match *pid {
        p if p.0 > 0 => pid.0 += step,
        p if p.0 < 0 => pid.0 -= step,
        _ => return Err(BufferPoolError::ZeroPid),
    }
    Ok(*pid)
}

impl From<PageError> for BufferPoolError {
    fn from(e: PageError) -> Self {
        match e {
            PageError::Full => BufferPoolError::Full,
            PageError::PageNotFound(_) => BufferPoolError::PidNotInFrame,
            PageError::IOError(_) => BufferPoolError::DiskWriteFail,
            PageError::IndexOutOfBounds(_) | PageError::UpdateNotAllowed => {
                BufferPoolError::BufferPoolWriteFail
            }
        }
    }
}
