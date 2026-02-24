use std::sync::{Arc, Mutex};
use std::thread;
use crate::table::Table;

pub fn create_merge_worker(table: Arc<Mutex<Table>>){
    //TODO: WRAP TABLE IN Arc<Mutex<Table>> AT QUERY/DB LEVEL
    // gives thread value ownership via move
    thread::spawn(move || {
        let mut last_merged_at = 0;
        loop {
            thread::sleep(std::time::Duration::from_millis(10));
            if let Ok(mut t) = table.lock() {
                if t.tail_count % 10 == 0 && t.tail_count > 0 && t.tail_count != last_merged_at {
                    last_merged_at = t.tail_count;
                    t.merge();
                }
            }
        }
    });
}