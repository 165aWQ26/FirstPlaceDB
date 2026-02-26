use crate::table::write_i64;
use crate::table::read_usize;
use crate::bufferpool::BufferPool;
use crate::db_error::DbError;
use crate::page_range::PhysicalAddress;
use crate::table::{Table, TableError};
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

#[derive(Clone)]
pub struct PageDirectory {
    /// RID -> Address
    directory: Vec<Option<PhysicalAddress>>,
}

/* We do not need a hashmap here. There is No benefit...
Instead, just use a vec and the rid as the index directly.
Everything is None by default.
Don't know if we every need to delete, RID isn't reused
*/
impl PageDirectory {
    //TODO: Calculate this by looking at testers
    #[inline]
    pub fn add(&mut self, rid: i64, address: PhysicalAddress) {
        let index = rid as usize;
        if index >= self.directory.len() {
            self.directory.resize(index + 1, None);
        }
        self.directory[index] = Some(address);
    }

    pub fn delete(&mut self, rid: i64) -> Result<(), DbError> {
        let index = rid as usize;
        self.directory
            .get(index)
            .ok_or(DbError::RecordNotFound(rid))?;
        self.directory[index] = None;
        Ok(())
    }

    //noinspection SpellCheckingInspection
    #[inline]
    pub fn get(&self, rid: i64) -> Result<PhysicalAddress, DbError> {
        self.directory
            .get(rid as usize)
            .copied()
            .flatten()
            .ok_or(DbError::RecordNotFound(rid))
    }

    pub fn write_to_disk(
        &self,
        writer: &mut BufWriter<File>,
    ) -> Result<(), TableError> {
        let entries: Vec<_> = self
            .directory
            .iter()
            .enumerate()
            .filter_map(|(i, addr)| addr.map(|a| (i, a)))
            .collect();

        write_i64(entries.len() as i64, writer)?;

        for (rid, addr) in entries {
            write_i64(rid as i64, writer)?;
            write_i64(addr.collection_num as i64, writer)?;
            write_i64(addr.offset as i64, writer)?;
        }
        Ok(())
    }

    pub fn read_from_disk(
        &mut self,
        buffer: &mut [u8; 8],
        reader: &mut BufReader<File>,
    ) -> Result<(), TableError> {
        let count = read_usize(buffer, reader)?;

        for _ in 0..count {
            let rid = read_usize(buffer, reader)?;
            let collection_num = read_usize(buffer, reader)?;
            let offset = read_usize(buffer, reader)?;

            let addr = PhysicalAddress {
                offset,
                collection_num,
            };
            if rid >= self.directory.len() {
                self.directory.resize(rid + 1, None);
            }
            self.directory[rid] = Some(addr);
        }

        Ok(())
    }
}

impl Default for PageDirectory {
    fn default() -> Self {
        PageDirectory {
            directory: vec![None; Table::PROJECTED_NUM_RECORDS],
        }
    }
}
