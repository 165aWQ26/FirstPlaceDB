use crate::bufferpool_context::{PageLocation, TableContext};
use crate::page::{Page, PageError};
use crate::page_range::{PhysicalAddress, WhichRange};
use crate::table::Table;
use lru::LruCache;
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
    BaseRidCol = 4,
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

pub struct BufferPool {
    cache: LruCache<Pid, Page>,
    table_names: Vec<String>,
    table_contexts: Vec<TableContext>,
}

impl BufferPool {
    pub const NUMBER_OF_FRAMES: usize = 32;

    #[inline]
    pub fn is_full(&self) -> bool {
        self.cache.len() >= self.cache.cap().get()
    }

    #[inline]
    pub fn append_name(&mut self, name: String) {
        self.table_names.push(name);
    }

    #[inline]
    pub fn append_context(&mut self, ctx: TableContext) {
        self.table_contexts.push(ctx);
    }

    #[inline]
    pub fn write_col(&mut self, pid: Pid, val: Option<i64>) -> Result<(), PageError> {
        if let Some(page) = self.cache.get_mut(&pid) {
            page.write(val)?;
        } else {
            return Err(PageError::PageNotFound(pid));
        }
        Ok(())
    }

    #[inline]
    pub fn read_col(
        &mut self,
        col: usize,
        page_location: &PageLocation,
        table_ctx: &TableContext,
    ) -> Result<Option<i64>, PageError> {
        let pid = make_pid(col, page_location, table_ctx);
        self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)
            .map_err(|_| PageError::IndexOutOfBounds(page_location.addr.offset))?;
        self.cache
            .get(&pid)
            .unwrap()
            .read(page_location.addr.offset)
    }

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
                self.lazy_guarantee_page(&page_location.addr, pid, table_ctx)
                    .map_err(|_| PageError::IndexOutOfBounds(page_location.addr.offset))?;
                let page = self.cache.get_mut(&pid).unwrap();
                page.set_dirty(true);
                page.update(page_location.addr.offset, val)?;
                Ok(())
            }
            _ => Err(PageError::UpdateNotAllowed),
        }
    }

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

    pub fn make_path(
        &self,
        addr: &PhysicalAddress,
        pid: Pid,
        table_cxt: &TableContext,
    ) -> Result<String, BufferPoolError> {
        let pid_val = pid.get_pid();
        if pid_val == 0 {
            return Err(BufferPoolError::ZeroPid);
        }
        let mut path = table_cxt.path.clone();
        if pid_val < 0 {
            path.push_str("tp_");
        } else {
            path.push_str("bp_");
        }
        path.push_str(addr.collection_num.to_string().as_str());
        path.push_str("_");
        path.push_str(
            ((pid_val.abs() - 1) % (table_cxt.total_cols as i64))
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
            self.evict()?;
        }
        let file_path = self.make_path(address, pid, table_ctx)?;
        let file = File::open(&file_path).map_err(|_| BufferPoolError::DiskReadFail)?;
        let mut reader = BufReader::new(file);
        let mut page = Page::default();
        let mut buffer = [0u8; 8];
        while let Ok(()) = self.read_bytes(&mut buffer, &mut reader) {
            let value = i64::from_be_bytes(buffer);
            if value == i64::MAX {
                page.write(None).map_err(|_| BufferPoolError::DiskReadFail)?;
            } else {
                page.write(Some(value)).map_err(|_| BufferPoolError::DiskReadFail)?;
            }
        }
        self.cache.push(pid, page);
        Ok(())
    }

    pub fn read_usize(
        &mut self,
        buffer: &mut [u8],
        reader: &mut BufReader<File>,
    ) -> Result<usize, BufferPoolError> {
        self.read_bytes(buffer, reader)?;
        let bytes: [u8; 8] = buffer[..8]
            .try_into()
            .map_err(|_| BufferPoolError::InvalidLength)?;
        Ok(i64::from_be_bytes(bytes) as usize)
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

    pub fn evict(&mut self) -> Result<(), BufferPoolError> {
        let (pid, page) = self.cache.pop_lru().unwrap();
        let table_ctx = self.table_contexts[pid.get_table_id()].clone();
        if page.is_dirty() && !table_ctx.path.is_empty() {
            let collection_num =
                (pid.get_pid().unsigned_abs() as usize - 1) / table_ctx.total_cols;
            let addr = PhysicalAddress { offset: 0, collection_num };
            self.write_to_disk(&page, &addr, pid, &table_ctx)?;
        }
        Ok(())
    }

    pub fn evict_all(&mut self) -> Result<(), BufferPoolError> {
        while self.cache.len() != 0 {
            let (pid, page) = self.cache.pop_lru().unwrap();
            let table_ctx = self.table_contexts[pid.get_table_id()].clone();
            let addr = PhysicalAddress {
                offset: 0,
                collection_num: (pid.0.abs() - 1) as usize / table_ctx.total_cols,
            };
            if page.is_dirty() {
                self.write_to_disk(&page, &addr, pid, &table_ctx)?;
            }
        }
        Ok(())
    }

    pub fn on_disk(&self, addr: &PhysicalAddress, pid: Pid, table_ctx: &TableContext) -> bool {
        self.make_path(addr, pid, table_ctx)
            .map(|p| Path::new(&p).exists())
            .unwrap_or(false)
    }

    #[inline]
    pub fn guarantee_writable_page(
        &mut self,
        addr: &PhysicalAddress,
        pid: Pid,
        table_ctx: &TableContext,
    ) -> Result<Pid, BufferPoolError> {
        if self.cache.contains(&pid) {
            return Ok(pid);
        }
        if self.is_full() {
            self.evict()?;
        }
        self.cache.push(pid, Page::default());
        Ok(pid)
    }

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
        }
        if self.is_full() {
            self.evict()?;
        }
        self.cache.push(pid, Page::default());
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
            let pid = self.guarantee_writable_page(&page_location.addr, pid, table_ctx)?;
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
            table_names: vec![],
            table_contexts: vec![],
        }
    }
}

/// Pid = (page_id, table_id)
pub type Pid = (i64, usize);

trait PidExt {
    fn get_pid(&self) -> i64;
    fn get_table_id(&self) -> usize;
}

impl PidExt for Pid {
    #[inline]
    fn get_pid(&self) -> i64 { self.0 }
    #[inline]
    fn get_table_id(&self) -> usize { self.1 }
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