use crate::page::{Page, PageError};
use crate::page_range::{PhysicalAddress, WhichRange};
use crate::table::Table;
use lru::LruCache;
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
}

//In general this structure will make a lot of assumptions about the data that is passed (not good for modularity but wtv).
//For now, we assume metadata is appended after data
//When writing getters and setters we will have to assume a position of each meta_col.
pub struct BufferPool {
    cache: LruCache<i64, Page>,
    size: usize,
    total_cols: usize,
    path: String,
}

impl BufferPool {
    pub const NUMBER_OF_FRAMES: usize = 32;

    //done
    pub fn set_total_cols(&mut self, cols: usize) {
        self.total_cols = Table::NUM_META_PAGES + cols;
    }


    //done
    pub fn set_path(&mut self, path: String) {
        self.path = path;
    }

    //done
    #[inline]
    pub fn write_col(&mut self, pid: i64, val: Option<i64>) -> Result<(), PageError> {
        //col: usize, total_cols: usize, collection_num: usize, range : WhichRange
        let page = self.cache.get_mut(&pid).unwrap();

        page.write(val)

}

    //done
    #[inline]
    pub fn read_col(
        &mut self,
        col: usize,
        addr: &PhysicalAddress,
        range: WhichRange,
    ) -> Result<Option<i64>, PageError> {
        let pid = make_pid(col, self.total_cols, addr.collection_num, &range);
        // If page is not in cache, unwrap can crash the entire program.
        // lazy_guarentee_page() ensures the page is in the bufferpool before access.
        self.lazy_guarantee_page(addr, pid)
            .map_err(|_| PageError::IndexOutOfBounds(addr.offset))?;
        self.cache.get(&pid).unwrap().read(addr.offset)
    }

    //done
    #[inline]
    pub fn update_meta_col(
        &mut self,
        addr: &PhysicalAddress,
        val: Option<i64>,
        col_type: MetaPage,
        range: WhichRange,
    ) -> Result<(), PageError> {
        match(col_type) {
            MetaPage::IndirectionCol => {
                let col: usize = self.total_cols - Table::NUM_META_PAGES + col_type as usize;
                let pid = make_pid(col, self.total_cols, addr.collection_num, &range);
                // See read_col
                self.lazy_guarantee_page(addr, pid)
                    .map_err(|_| PageError::IndexOutOfBounds(addr.offset))?;
                let page = self.cache.get_mut(&pid).unwrap();

                page.set_dirty(true);
                page.update(addr.offset, val)?;
                Ok(())
            }
            _ => {
                Err(PageError::UpdateNotAllowed)
            }
        }
    }

    // Returns a reference to the metadata page at the given column index
    // done
    #[inline]
    fn meta_record(&mut self, pid: i64) -> &Page {
        self.cache.get(&pid).unwrap()
    }

    //done
    #[inline]
    pub fn read_meta_col(
        &mut self,
        addr: &PhysicalAddress,
        col_type: MetaPage,
        range: WhichRange,
    ) -> Result<Option<i64>, PageError> {
        let col: usize = self.total_cols - Table::NUM_META_PAGES + col_type as usize;
        let pid = make_pid(col, self.total_cols, addr.collection_num, &range);

        self.lazy_guarantee_page(addr, pid)
            .map_err(|_| PageError::IndexOutOfBounds(addr.offset))?;
        self.meta_record(pid).read(addr.offset)
    }

    //done
    pub fn is_full(&self) -> bool {
        self.size == BufferPool::NUMBER_OF_FRAMES
    }

    //done
    pub fn make_path(&self, addr: &PhysicalAddress, pid: Pid) -> Result<String, BufferPoolError> {

        if pid == 0 {
            return Err(BufferPoolError::ZeroPid);
        }

        let mut path = self.path.clone(); // ./ECS165

        if pid < 0 {
            path.push_str("tp_");
        } else {
            path.push_str("bp_");
        }
        path.push_str(addr.collection_num.to_string().as_str());
        path.push_str("_");
        path.push_str(
            ((pid.abs() - 1) % (self.total_cols as i64))
                .to_string()
                .as_str(),
        );

        Ok(path)
    }


    ///
    pub fn write_to_disk(
        &mut self,
        page: &Page,
        addr: &PhysicalAddress,
        pid: Pid,
    ) -> Result<(), BufferPoolError> {
        // write
        let file_path = self.make_path(addr, pid)?;
        let file = File::create(&file_path).map_err(|_| BufferPoolError::DiskWriteFail)?;

        let mut writer = BufWriter::new(file);

        for value in page.iter() {
            if let Some(val) = value {
                let bytes = val.to_be_bytes();
                writer
                    .write_all(&bytes)
                    .map_err(|_| BufferPoolError::DiskWriteFail)?;
            } else {
                //largest positive number
                let crazy_big_num : i64 = -1 ^ (1 << 63);

                let bytes = crazy_big_num.to_be_bytes();
                writer
                    .write_all(&bytes)
                    .map_err(|_| BufferPoolError::DiskWriteFail)?;
            }
        }

        writer.flush().map_err(|_| BufferPoolError::DiskWriteFail)?;

        Ok(())
    }

    pub fn read_from_disk(
        &mut self,
        address: &PhysicalAddress,
        pid: Pid,
    ) -> Result<(), BufferPoolError> {
        if self.is_full() {
            self.evict()?;
        }
        let file_path = self.make_path(address, pid)?;
        let file = File::open(&file_path).map_err(|_| BufferPoolError::DiskReadFail)?;

        let mut reader = BufReader::new(file);
        let mut page = Page::default();

        let mut buffer = [0u8; 8];

        while let Ok(()) = reader.read_exact(&mut buffer) {
            let value = i64::from_be_bytes(buffer);
            page.write(Some(value))
                .map_err(|_e| BufferPoolError::DiskReadFail)?;
        }

        self.cache.push(pid, page);
        self.size += 1;

        Ok(())
    }

    pub fn is_dirty(&self, page: Page) -> bool {
        page.is_dirty()
    }
    // when does a page become dirty?
    // it is dirty when the content in the bufferpool does not match the content in the disk
    // if we just append stuff to the bufferpool, that's still dirty bc it's never added to the disk.
    /// Kicks last page from the cache out of the bufferpool and onto disk
    /// If the file path is not set, the db is assumed to be in memory only.
    /// evict() will not write to disk but instead do only the removal step.
    pub fn evict(&mut self) -> Result<(), BufferPoolError> {
        // We only want to write to disk if the page is dirty.
        let (pid, page) = self.cache.pop_lru().unwrap();
        if page.is_dirty() && !self.path.is_empty() {
            let collection_num = { pid.unsigned_abs() as usize - 1 } / self.total_cols;
            let addr = PhysicalAddress {
                offset: 0,
                collection_num,
            };
            self.write_to_disk(&page, &addr, pid)?;
        }
        self.size -= 1;
        Ok(())
    }


    pub fn evict_all(&mut self) -> Result<(), BufferPoolError> {
        while self.size != 0 {
            //offset no
            //addr.offset = self.cache.get(&pid).unwrap().len() - 1;
            let (pid, page) = self.cache.pop_lru().unwrap();
            let addr = PhysicalAddress {
                offset: 0,
                collection_num: (pid.abs() - 1) as usize / self.total_cols,
            };
            if page.is_dirty() {
                self.write_to_disk(&page, &addr, pid)?;
            }
            self.size -= 1;
        }
        Ok(())
    }


    pub fn on_disk(&self, addr: &PhysicalAddress, pid: Pid) -> bool {
        let path = self.make_path(addr, pid);

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
    pub fn create_blank_page (
        &mut self,
        pid: Pid,
    ) -> Result<Pid, BufferPoolError> {
        if self.is_full() {
            self.evict()?
        }
        let mut new_pid = pid;
        increment_pid(&mut new_pid, self.total_cols as i64)?;
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
    ) -> Result<Pid, BufferPoolError> {

        if self.cache.contains(&pid) {
            if self.cache.get(&pid).unwrap().has_capacity() {
                return Ok(pid);
            }
            // Page exist but full, evict and make new page and return new pid
            return self.create_blank_page(pid);
        }

        if self.on_disk(addr, pid) {
            self.read_from_disk(addr, pid)?;
            return Ok(pid);
        } else { // Not on disk
            if self.is_full() {
                self.evict()?;
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
        addr: &PhysicalAddress,
        range: &WhichRange,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        for (i, val) in all_data.into_iter().enumerate() {
            let pid = make_pid(i, self.total_cols, addr.collection_num, range);
            let pid = self.lazy_guarantee_page(addr, pid)?;

            self.write_col(pid, val)
                .map_err(|_| BufferPoolError::BufferPoolWriteFail)?;
        }
        Ok(*addr)
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(BufferPool::NUMBER_OF_FRAMES).unwrap()),
            size: 0,
            total_cols: 0,
            path: String::from(""),
        }
    }
}

pub type Pid = i64;

pub fn make_pid(col: usize, total_cols: usize, collection_num: usize, range: &WhichRange) -> Pid {
    let pid = ((col + total_cols * collection_num) + 1) as i64;
    if *range == WhichRange::Tail { -pid } else { pid }
}

pub fn increment_pid(pid: &mut Pid, step: i64) -> Result<i64, BufferPoolError> {
    match *pid {
        p if p > 0 => *pid += step,
        p if p < 0 => *pid -= step,
        _ => return Err(BufferPoolError::ZeroPid),
    }
    Ok(*pid)
}
