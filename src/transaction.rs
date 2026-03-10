use crate::iterators::AtomicIterator;
use crate::lock_manager::lock_manager;
use crate::query::Query;
use crate::table::Table;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

static TXN_COUNTER: AtomicIterator<AtomicUsize> = AtomicIterator { next: AtomicUsize::new(0) };

#[derive(Clone)]
pub enum QueryOp {
    Insert        { table: Arc<Table>, args: Vec<Option<i64>> },
    Update        { table: Arc<Table>, key: i64, cols: Vec<Option<i64>> },
    Delete        { table: Arc<Table>, key: i64 },
    Select        { table: Arc<Table>, key: i64, search_col: usize, proj: Vec<i64> },
    SelectVersion { table: Arc<Table>, key: i64, search_col: usize, proj: Vec<i64>, version: i64 },
    Sum           { table: Arc<Table>, start: i64, end: i64, col: usize },
    SumVersion    { table: Arc<Table>, start: i64, end: i64, col: usize, version: i64 },
    Increment     { table: Arc<Table>, key: i64, col: usize },
}

enum UndoEntry {
    InsertUndo { table: Arc<Table>, key: i64 },
    UpdateUndo { table: Arc<Table>, key: i64, before: Vec<Option<i64>> },
    DeleteUndo { table: Arc<Table>, before: Vec<Option<i64>> },
}

pub struct Transaction {
    pub ops: Vec<QueryOp>,
}

impl Transaction {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn from_ops(ops: Vec<QueryOp>) -> Self {
        Self { ops }
    }

    pub fn add_op(&mut self, op: QueryOp) {
        self.ops.push(op);
    }

    pub fn run(&self) -> bool {
        let txn_id = TXN_COUNTER.next();
        let lm = lock_manager();
        let mut undo: Vec<UndoEntry> = Vec::new();

        for op in &self.ops {
            if !Self::acquire_locks(lm, op, txn_id) {
                Self::rollback(undo, txn_id);
                return false;
            }
            if !Self::execute_op(op, &mut undo) {
                Self::rollback(undo, txn_id);
                return false;
            }
        }

        lm.release_all(txn_id);
        true
    }

    fn acquire_locks(lm: &crate::lock_manager::LockManager, op: &QueryOp, txn_id: usize) -> bool {
        match op {
            QueryOp::Insert { table, args } => {
                if let Some(Some(key)) = args.get(table.key_index) {
                    lm.acquire_exclusive(table.table_id, *key, txn_id)
                } else {
                    true
                }
            }
            QueryOp::Update   { table, key, .. } => lm.acquire_exclusive(table.table_id, *key, txn_id),
            QueryOp::Delete   { table, key }     => lm.acquire_exclusive(table.table_id, *key, txn_id),
            QueryOp::Increment{ table, key, .. } => lm.acquire_exclusive(table.table_id, *key, txn_id),
            QueryOp::Select        { table, key, .. } => lm.acquire_shared(table.table_id, *key, txn_id),
            QueryOp::SelectVersion { table, key, .. } => lm.acquire_shared(table.table_id, *key, txn_id),
            QueryOp::Sum { table, start, end, .. } => {
                table.indices[table.key_index]
                    .locate_range(*start, *end)
                    .iter()
                    .all(|&rid| {
                        table.read_latest_single(rid, table.key_index)
                            .ok()
                            .flatten()
                            .map_or(true, |key| lm.acquire_shared(table.table_id, key, txn_id))
                    })
            }
            QueryOp::SumVersion { table, start, end, .. } => {
                table.indices[table.key_index]
                    .locate_range(*start, *end)
                    .iter()
                    .all(|&rid| {
                        table.read_latest_single(rid, table.key_index)
                            .ok()
                            .flatten()
                            .map_or(true, |key| lm.acquire_shared(table.table_id, key, txn_id))
                    })
            }
        }
    }

    fn execute_op(op: &QueryOp, undo: &mut Vec<UndoEntry>) -> bool {
        match op {
            QueryOp::Insert { table, args } => {
                match Query::new(table.clone()).insert(args.clone()) {
                    Ok(true) => {
                        if let Some(Some(key)) = args.get(table.key_index) {
                            undo.push(UndoEntry::InsertUndo { table: table.clone(), key: *key });
                        }
                        true
                    }
                    _ => false,
                }
            }
            QueryOp::Update { table, key, cols } => {
                let before = Self::read_before_image(table, *key);
                match Query::new(table.clone()).update(*key, cols.clone()) {
                    Ok(true) => {
                        if let Some(b) = before {
                            undo.push(UndoEntry::UpdateUndo { table: table.clone(), key: *key, before: b });
                        }
                        true
                    }
                    _ => false,
                }
            }
            QueryOp::Delete { table, key } => {
                let before = Self::read_before_image(table, *key);
                match Query::new(table.clone()).delete(*key) {
                    Ok(true) => {
                        if let Some(b) = before {
                            undo.push(UndoEntry::DeleteUndo { table: table.clone(), before: b });
                        }
                        true
                    }
                    _ => false,
                }
            }
            QueryOp::Select { table, key, search_col, proj } =>
                Query::new(table.clone()).select(*key, *search_col, proj).is_ok(),
            QueryOp::SelectVersion { table, key, search_col, proj, version } =>
                Query::new(table.clone()).select_version(*key, *search_col, proj, *version).is_ok(),
            QueryOp::Sum { table, start, end, col } =>
                Query::new(table.clone()).sum(*start, *end, *col).is_ok(),
            QueryOp::SumVersion { table, start, end, col, version } =>
                Query::new(table.clone()).sum_version(*start, *end, *col, *version).is_ok(),
            QueryOp::Increment { table, key, col } =>
                Query::new(table.clone()).increment(*key, *col).unwrap_or(false),
        }
    }

    fn read_before_image(table: &Arc<Table>, key: i64) -> Option<Vec<Option<i64>>> {
        let rid = table.indices[table.key_index].locate(key)?;
        let full = table.read_latest(rid).ok()?;
        Some(full[..table.num_data_columns].to_vec())
    }

    fn rollback(undo: Vec<UndoEntry>, txn_id: usize) {
        for entry in undo.into_iter().rev() {
            match entry {
                UndoEntry::InsertUndo { table, key } => {
                    let _ = Query::new(table).delete(key);
                }
                UndoEntry::UpdateUndo { table, key, before } => {
                    let mut restore = before;
                    restore[table.key_index] = None;
                    let _ = Query::new(table).update(key, restore);
                }
                UndoEntry::DeleteUndo { table, before } => {
                    let _ = Query::new(table).insert(before);
                }
            }
        }
        lock_manager().release_all(txn_id);
    }
}