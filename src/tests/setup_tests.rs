use crate::db::Database;
use crate::query::Query;
use std::process::Command;

pub fn setup_db(num_columns: usize) -> Database {
    //Deletes all files and directories in ./ECS165
    let status = Command::new("sh")
        .arg("-c")
        .arg("rm -rf ./ECS165/*")
        .status()
        .expect("Failed to execute command");
    let mut db = Database::new();
    db.open("./ECS165");
    db.create_table(String::from("test"), num_columns, 0);
    db
}

pub fn setup_query(db: &'_ mut Database) -> Option<Query<'_>> {
    if let Ok(Some(table)) = db.get_table(&String::from("test")) {
        return Some(Query::new(table));
    }
    None
}
