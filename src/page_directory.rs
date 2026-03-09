use dashmap::DashMap;
use crate::errors::DbError;
use crate::iterators::PhysicalAddress;

pub struct PageDirectory {
    /// RID -> Address
    directory: DashMap<i64, PhysicalAddress>,
}
/* We do not need a hashmap here. There is No benefit...
Instead just use a vec and the rid as the index directly.
Everything is None by default.
Don't know if we every need to delete, RID isn't reused
*/
impl PageDirectory {
    #[inline]
    pub fn add(&self, rid: i64, address: PhysicalAddress) {
        self.directory.insert(rid, address);
    }

    pub fn delete(&self, rid: i64) -> Result<(), DbError> {
        if self.directory.remove(&rid).is_none() {
            return Err(DbError::RecordNotFound(rid));
        }
        Ok(())
    }

    //When this throws a panic it means you are either accessing a record that
    //DNE or has been deleted... Too lazy to write real exception handling DAANNNYYY Fix me
    #[inline]
    pub fn get(&self, rid: i64) -> Result<PhysicalAddress, DbError> {
        self.directory.get(&rid).map(|addr| *addr.value()).ok_or(DbError::RecordNotFound(rid))
    }
}

impl Default for PageDirectory {
    fn default() -> Self {
        PageDirectory {
            directory: DashMap::new(),
        }
    }
}
