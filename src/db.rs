use rustc_hash::FxHashMap;

use crate::bufferpool::BufferPool;
use crate::table::Table;

struct Database <'a>{
    tables: FxHashMap<String, Table<'a>>,
    //Will want to add functionality to work with other tables
    pub bufferpool: BufferPool,
}


#[allow(dead_code)]
impl <'a> Database <'a>{
    pub const NUMBER_OF_FRAMES: usize = 20;
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) {
        //once again this assumes single table functionality
        self.bufferpool = BufferPool::new(Database::NUMBER_OF_FRAMES);

        let table = Table::new(
            name.clone(),
            num_columns + Table::NUM_META_PAGES,
            key_index,
            &self.bufferpool
        );
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
    pub fn drop_table(&mut self, name: &str) {
        self.tables.remove(name);
    }
}