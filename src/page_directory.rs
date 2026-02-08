use crate::page_range::PhysicalAddress;
use crate::table::Table;
use crate::error::DbError;

pub struct PageDirectory {
    /// RID -> Address
    directory: Vec<Option<PhysicalAddress>>,
}
/* We do not need a hashmap here. There is No benefit...
Instead just use a vec and the rid as the index directly.
Everything is None by default.
Don't know if we every need to delete, RID isn't reused
*/
impl PageDirectory {
    //TODO: Calculate this by looking at testers
    pub fn add(&mut self, rid: i64, address: PhysicalAddress) {
        self.directory[rid as usize] = Some(address);
    }

    pub fn delete(&mut self, rid: i64) -> Result<(), DbError> {
        let index = rid as usize;
        self.directory
        .get(index)
        .ok_or(DbError::RecordNotFound(rid))?;
    self.directory[index] = None;
    Ok(())
    }

    //When this throws a panic it means you are either accessing a record that
    //DNE or has been deleted... Too lazy to write real exception handling DAANNNYYY Fix me
    pub fn get(&self, rid: i64) -> Result<PhysicalAddress, DbError> {
        self.directory.get(rid as usize)
        .copied()
        .flatten()
        .ok_or(DbError::RecordNotFound(rid))
    }
}

impl Default for PageDirectory {
    fn default() -> Self {
        PageDirectory {
            directory: vec![None; Table::PROJECTED_NUM_RECORDS],
        }
    }
}
