use crate::iterators::AtomicIterator;
use crate::lock_manager::LockManager;
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

    /// Extract the table reference from any op variant.
    fn table_of(op: &QueryOp) -> &Arc<Table> {
        match op {
            QueryOp::Insert        { table, .. } => table,
            QueryOp::Update        { table, .. } => table,
            QueryOp::Delete        { table, .. } => table,
            QueryOp::Select        { table, .. } => table,
            QueryOp::SelectVersion { table, .. } => table,
            QueryOp::Sum           { table, .. } => table,
            QueryOp::SumVersion    { table, .. } => table,
            QueryOp::Increment     { table, .. } => table,
        }
    }

    pub fn run(&self) -> bool {
        if self.ops.is_empty() {
            return true;
        }

        let txn_id = TXN_COUNTER.next();
        let lm = &Self::table_of(&self.ops[0]).lock_manager;
        let mut undo: Vec<UndoEntry> = Vec::new();
        let mut held_locks: Vec<(usize, i64)> = Vec::new();

        for op in &self.ops {
            if !Self::acquire_locks(lm, op, txn_id, &mut held_locks) {
                Self::rollback(undo, txn_id, &held_locks, lm);
                return false;
            }
            if !Self::execute_op(op, &mut undo) {
                Self::rollback(undo, txn_id, &held_locks, lm);
                return false;
            }
        }

        lm.release_locks(txn_id, &held_locks);
        true
    }

    fn acquire_locks(
        lm: &LockManager,
        op: &QueryOp,
        txn_id: usize,
        held: &mut Vec<(usize, i64)>,
    ) -> bool {
        match op {
            QueryOp::Insert { table, args } => {
                if let Some(Some(key)) = args.get(table.key_index) {
                    if lm.acquire_exclusive(table.table_id, *key, txn_id) {
                        held.push((table.table_id, *key));
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            }
            QueryOp::Update { table, key, .. } => {
                if lm.acquire_exclusive(table.table_id, *key, txn_id) {
                    held.push((table.table_id, *key));
                    true
                } else {
                    false
                }
            }
            QueryOp::Delete { table, key } => {
                if lm.acquire_exclusive(table.table_id, *key, txn_id) {
                    held.push((table.table_id, *key));
                    true
                } else {
                    false
                }
            }
            QueryOp::Increment { table, key, .. } => {
                if lm.acquire_exclusive(table.table_id, *key, txn_id) {
                    held.push((table.table_id, *key));
                    true
                } else {
                    false
                }
            }
            QueryOp::Select { table, key, .. } => {
                if lm.acquire_shared(table.table_id, *key, txn_id) {
                    held.push((table.table_id, *key));
                    true
                } else {
                    false
                }
            }
            QueryOp::SelectVersion { table, key, .. } => {
                if lm.acquire_shared(table.table_id, *key, txn_id) {
                    held.push((table.table_id, *key));
                    true
                } else {
                    false
                }
            }
            QueryOp::Sum { table, start, end, .. } => {
                let rids = table.indices[table.key_index].locate_range(*start, *end);
                for &rid in &rids {
                    if let Ok(Some(key)) = table.read_latest_single(rid, table.key_index) {
                        if !lm.acquire_shared(table.table_id, key, txn_id) {
                            return false;
                        }
                        held.push((table.table_id, key));
                    }
                }
                true
            }
            QueryOp::SumVersion { table, start, end, .. } => {
                let rids = table.indices[table.key_index].locate_range(*start, *end);
                for &rid in &rids {
                    if let Ok(Some(key)) = table.read_latest_single(rid, table.key_index) {
                        if !lm.acquire_shared(table.table_id, key, txn_id) {
                            return false;
                        }
                        held.push((table.table_id, key));
                    }
                }
                true
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
                // Normalise: force the key column to None so
                // Query::update doesn't reject the op.
                let mut update_cols = cols.clone();
                update_cols[table.key_index] = None;

                let before = Self::read_before_image(table, *key);
                match Query::new(table.clone()).update(*key, update_cols) {
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

    fn rollback(undo: Vec<UndoEntry>, txn_id: usize, held_locks: &[(usize, i64)], lm: &LockManager) {
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
        lm.release_locks(txn_id, held_locks);
    }
}