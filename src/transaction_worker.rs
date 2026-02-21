use std::sync::{Arc, Mutex};
use std::thread;
use crate::table::Table

pub fn create_merge_worker(table: Arc<Mutex<Table>>){
    //TODO: WRAP TABLE IN Arc<Mutex<Table>> AT QUERY/DB LEVEL
    // gives thread value ownership via move
    thread::spawn(move || {
        loop{
            thread::sleep(std::time::Duration::from_secs(5)); //change to whatever we want to use ig, i just defaulted on 5
            if let Ok(mut t) = table.lock(){
                t.merge();
            }
        }
    });
}