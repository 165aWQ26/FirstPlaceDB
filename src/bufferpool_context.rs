use crate::page_range::{PhysicalAddress, WhichRange};

#[derive(Clone)]
pub struct TableContext {
    pub table_id: usize,
    pub total_cols: usize,
    pub path: String,
}

impl TableContext {
    pub fn new(table_id: usize, total_cols: usize, path: String) -> Self {
        Self {
            table_id,
            total_cols,
            path,
        }
    }
}

pub struct PageLocation {
    pub addr: PhysicalAddress,
    pub range: WhichRange,
}

impl PageLocation {
    pub fn new(addr: PhysicalAddress, range: WhichRange) -> Self {
        Self { addr, range }
    }

    pub fn base(addr: PhysicalAddress) -> Self {
        Self {
            addr,
            range: WhichRange::Base,
        }
    }

    pub fn tail(addr: PhysicalAddress) -> Self {
        Self {
            addr,
            range: WhichRange::Tail,
        }
    }
}
