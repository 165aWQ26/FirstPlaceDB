use crate::table::Table;
use std::sync::Arc;

pub struct CoreIndex {
    pub(crate) table: Arc<Table>,
}

impl CoreIndex {
    pub fn create_index(&self, col: usize) {
        if col < self.table.num_data_columns {
            self.table.indices[col].enable()
        }
    }

    pub fn drop_index(&self, col: usize) {
        // Bruh no way to delete from skiplist; Handle automatically based on epoch
        let _ = col;
    }
}