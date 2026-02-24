use crate::page_range::{PhysicalAddress, WhichRange};

#[derive(Clone)]
pub struct TableContext {
    pub table_id: usize,
    pub total_cols: usize,
}

impl TableContext {
    pub fn new(table_id: usize, total_cols: usize) -> Self {
        Self { table_id, total_cols }
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
        Self { addr, range: WhichRange::Base }
    }

    pub fn tail(addr: PhysicalAddress) -> Self {
        Self { addr, range: WhichRange::Tail }
    }
}