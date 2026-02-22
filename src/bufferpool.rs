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
    frames: LruCache<i64, Page>,
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
        let page = self.frames.get_mut(&pid).unwrap();
        
        page.set_dirty(true);
        page.write(val)
    }

    //done
    #[inline]
    pub fn read_col(
        &mut self,
        col: usize,
        addr: PhysicalAddress,
        range: WhichRange,
    ) -> Result<Option<i64>, PageError> {
        let pid = Pid::new(col, self.total_cols, addr.collection_num, range);
        // self.frames.get(pid).unwrap().read(offset);
        self.frames.get(&pid.get()).unwrap().read(addr.offset)
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
        // match col {
        //     MetaPage::IndirectionCol => {
        //         // let pid: i64 = ((self.total_cols * Table::NUM_META_PAGES) + col as i64) as i64;
        //         let pid: i64 = (self.total_cols * Table::NUM_META_PAGES + col as usize) as i64;
        //         // let actual_col = self.pages.len() - Table::NUM_META_PAGES + col as usize;
        //         self.frames.get_mut(&(pid)).unwrap().update(address.offset, val)?;
        //         Ok(())
        //     }
        //     MetaPage::SchemaEncodingCol => panic!("Cannot update schema encoding"),
        //     MetaPage::StartTimeCol => panic!("Cannot update start time"),
        //     MetaPage::RidCol => panic!("Cannot update RID"),
        // }
        if let MetaPage::IndirectionCol = col_type {
            let col: usize = self.total_cols - Table::NUM_META_PAGES + col_type as usize;
            let pid = Pid::new(col, self.total_cols, addr.collection_num, range);
            let page = self.frames.get_mut(&pid.get()).unwrap();
            
            page.set_dirty(true);
            page.update(addr.offset, val)?;
            Ok(())
        } else {
            Err(PageError::UpdateNotAllowed)
        }
    }

    // Returns a reference to the metadata page at the given column index
    // done
    #[inline]
    fn meta_record(&mut self, pid: i64) -> &Page {
        self.frames.get(&pid).unwrap()
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
        let pid = Pid::new(col, self.total_cols, addr.collection_num, range);
        self.meta_record(pid.get()).read(addr.offset)
    }

    //done
    pub fn is_full(&self) -> bool {
        self.size == BufferPool::NUMBER_OF_FRAMES
    }

    //done
    pub fn make_path(&self, addr: &PhysicalAddress, pid: Pid) -> Result<String, BufferPoolError> {
        let pid_val = pid.get();
        if pid_val == 0 {
            return Err(BufferPoolError::ZeroPid);
        }
        let mut path = self.path.clone(); // ./ECS165

        if pid_val < 0 {
            path.push_str("tp_");
        } else {
            path.push_str("bp_");
        }
        path.push_str(addr.collection_num.to_string().as_str());
        path.push_str(
            ((pid_val.abs() - 1) % (self.total_cols as i64))
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
            self.evict(address, pid)?;
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
        
        self.frames.push(pid.get(), page);
        self.size += 1;

        Ok(())
    }

    pub fn is_dirty(&self, page: Page) -> bool {
        page.is_dirty()
    }
    // when does a page become dirty?
    // it is dirty when the content in the bufferpool does not match the content in the disk
    // if we just append stuff to the bufferpool, that's still dirty bc it's never added to the disk.
    pub fn evict(&mut self, addr: &PhysicalAddress, pid: Pid) -> Result<(), BufferPoolError> {
        // We only want to write to disk if the page is dirty.
        let page = self.frames.pop_lru().unwrap().1;
        if page.is_dirty() {
            self.write_to_disk(&page, addr, pid)?;
        }
        self.size -= 1;
        Ok(())
    }
    #[inline]
    pub fn evict_all(&mut self) -> Result<(), BufferPoolError> {
        while self.size != 0 {
            let (pid, page) = self.frames.pop_lru().unwrap();
            let mut addr: PhysicalAddress = PhysicalAddress::default();
            //offset no
            //addr.offset = self.frames.get(&pid).unwrap().len() - 1;
            addr.collection_num = ((pid.abs() - 1) as usize ) % self.total_cols;
    
            let pid = Pid { pid };
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

    pub fn create_blank_page(
        &mut self,
        addr: &PhysicalAddress,
        mut pid: Pid,
    ) -> Result<(), BufferPoolError> {
        if self.is_full() {
            self.evict(addr, pid)?
        }
        self.frames.push(pid.get(), Page::default());
        pid.increment(self.total_cols as i64)?;
        Ok(self.size += 1)
    }

    /// If page exists in bufferpool,
    ///     If page does not have capacity and LRU is full
    ///         evict, make new page
    ///     else
    ///         write to page lol -- ok we dont need this since its lazy
    /// Else // Page not in bufferpool:
    ///     If on disk
    ///         Pull from disk
    ///     else
    ///         if LRU is full
    ///              evict
    ///         Push blank page
    #[inline]
    pub fn lazy_guarantee_page(
        &mut self,
        addr: &PhysicalAddress,
        pid: Pid,
    ) -> Result<Pid, BufferPoolError> {
        if self.frames.contains(&pid.get()) && !self.frames.get(&pid.get()).unwrap().has_capacity()
        {
            self.create_blank_page(addr, pid)?;
        } else {
            if self.on_disk(addr, pid) {
                self.read_from_disk(addr, pid)?;
            } else {
                self.create_blank_page(addr, pid)?;
            }
        }

        Ok(pid)
    }

    #[inline]
    pub fn append(
        &mut self,
        all_data: Vec<Option<i64>>,
        addr: &PhysicalAddress,
        range: WhichRange,
    ) -> Result<PhysicalAddress, BufferPoolError> {
        let mut first_pid = Pid::new(0, self.total_cols, addr.collection_num, range);
        for (i, val) in all_data.into_iter().enumerate() {
            first_pid.increment(i as i64)?;
            let pid = self.lazy_guarantee_page(addr, first_pid)?;
            self.write_col(pid.get(), val).map_err(|_| BufferPoolError::BufferPoolWriteFail)?;
        }
        Ok(*addr)
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self {
            frames: LruCache::new(NonZeroUsize::new(BufferPool::NUMBER_OF_FRAMES).unwrap()),
            size: 0,
            total_cols: 0,
            path: String::from(""),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Pid {
    /// ID fot the Nth page. Tail if negative, base if positive
    pid: i64,
}

impl Pid {
    pub fn new(col: usize, total_cols: usize, collection_num: usize, range: WhichRange) -> Self {
        let pid_unsigned = (col + total_cols * collection_num) + 1;
        let mut pid = pid_unsigned as i64;
        if matches![range, WhichRange::Tail] {
            pid = -pid;
        }
        Self { pid }
    }

    pub fn get(&self) -> i64 {
        self.pid
    }

    pub fn increment(&mut self, i: i64) -> Result<i64, BufferPoolError> {
        if self.pid == 0 {
            return Err(BufferPoolError::ZeroPid);
        }
        if self.pid > 0 {
            self.pid += i;
        } else {
            self.pid -= i;
        }
        Ok(self.pid)
    }
}

// pub struct PidIterator {
//     current: i64,
// }
//
// impl Iterator for PidIterator {
//     type Item = i64;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.current == 0 {
//             return None;
//         }
//
//         let result = self.current;
//
//         if self.current < 0 {
//             self.current -= 1;
//         } else {
//             self.current += 1;
//         }
//         Some(result)
//     }
// }
//
// impl Pid {
//     pub fn iter(&self) -> PidIterator {
//         PidIterator { current: self.pid }
//     }
// }
