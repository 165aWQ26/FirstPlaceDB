use crate::bufferpool::{BufferPool};
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
    //When this throws a panic it means you are either accessing a record that
    //DNE or has been deleted... Too lazy to write real exception handling DAANNNYYY Fix me
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
        bufferpool: Arc<Mutex<BufferPool>>,
    ) -> Result<(), TableError> {
        for addr in self.directory.iter() {
            bufferpool
                .lock()
                .write_i64(addr.unwrap().collection_num as i64, writer)
                .map_err(|_| TableError::WriteFail)?;
            bufferpool
                .lock()
                .write_i64(addr.unwrap().offset as i64, writer)
                .map_err(|_| TableError::WriteFail)?;
        }
        Ok(())
    }

    pub fn read_from_disk(
        &mut self,
        buffer: &mut [u8],
        reader: &mut BufReader<File>,
        bufferpool: Arc<Mutex<BufferPool>>,
    ) -> Result<(), TableError> {
        // We need to iterate X times?
        // X = # of total vals times
        // Until the end of the file lol.

        while let Ok(mut current) = bufferpool.lock().read_usize(buffer, reader) {
            let mut addr: PhysicalAddress = PhysicalAddress::default();

            addr.collection_num = current;
            current = bufferpool
                .lock()
                .read_usize(buffer, reader)
                .map_err(|_| TableError::ReadFail)?;
            addr.offset = current;

            self.directory.push(Some(addr));
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
