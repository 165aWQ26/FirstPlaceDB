use rustc_hash::FxHashMap;

use crate::table::Table;

struct Database {
    tables: FxHashMap<String, Table>,
}

impl Database {
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        let table = Table::new(name.clone(), num_columns + Table::NUM_META_PAGES, num_columns, key_index);
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}
