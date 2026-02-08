use crate::page::Page;
use crate::page_range::{PhysicalAddress, PhysicalAddressIterator};
use crate::table::Table;

pub struct PageDirectory {
    /// RID -> Address
    directory: Vec<Option<PhysicalAddress>>
}
/* We do not need a hashmap here. There is No benefit...
 Instead just use a vec and the rid as the index directly.
 Everything is None by default.
 Don't know if we every need to delete, RID isn't reused
 */
impl PageDirectory {

    //TODO: Calculate this by looking at testers
    pub fn add(&mut self, rid : usize, address: PhysicalAddress) {
        self.directory[rid] = Some(address);
    }

    pub fn delete(&mut self, rid : usize) {
        self.directory[rid] = None;
    }

    //When this throws a panic it means you are either accessing a record that
    //DNE or has been deleted... Too lazy to write real exception handling DAANNNYYY Fix me
    pub fn get(&self, rid : usize) -> PhysicalAddress {
        self.directory[rid].unwrap()
    }
}

impl Default for PageDirectory {
    fn default() -> Self {
        PageDirectory {
            directory: vec![None; Table::PROJECTED_NUM_RECORDS]
        }
    }
}
