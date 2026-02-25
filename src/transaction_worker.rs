use std::sync::{Arc, Mutex};
use std::thread;
use crate::table::Table;

#[allow(dead_code)]
pub fn create_merge_worker(table: Arc<Mutex<Table>>){
    //TODO: WRAP TABLE IN Arc<Mutex<Table>> AT QUERY/DB LEVEL
    // gives thread value ownership via move
    thread::spawn(move || {
        loop {
            thread::sleep(std::time::Duration::from_millis(10));
            if let Ok(mut t) = table.lock() {
                if t.page_ranges.tail.full_pages_since_merge >= 10 {
                    let _ = t.merge();
                }
            }
        }
    });
}