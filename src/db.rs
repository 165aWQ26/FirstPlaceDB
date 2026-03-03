use rustc_hash::FxHashMap;

use crate::table::Table;

struct Database {
    tables: FxHashMap<usize, Table>,
    table_names: FxHashMap<String, usize>, //use this for the guard check
    table_id: std::ops::RangeFrom<usize>,
}

#[allow(dead_code)]
impl Database {
    pub fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
            table_names: FxHashMap::default(),
            table_id: 0..,
        }
    }
    
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        let table_id = self.table_id.next().unwrap();
        let table = Table::new(
            name.clone(),
            num_columns,
            key_index,
            table_id,
        );
        self.tables.insert(table_id, table);
        self.table_names.insert(name, table_id);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(self.table_names.get(name).unwrap()) //Todo danny fix this error handling
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(self.table_names.get(name).unwrap()); //Todo danny fix this error handling
    }
}