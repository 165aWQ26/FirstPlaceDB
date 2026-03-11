use dashmap::DashMap;
use std::sync::OnceLock;

#[derive(Default)]
struct LockEntry {
    exclusive: Option<usize>,
    shared: Vec<usize>,
}

pub struct LockManager {
    table: DashMap<(usize, i64), LockEntry>,
}

impl LockManager {
    fn new() -> Self {
        Self { table: DashMap::new() }
    }

    pub fn acquire_shared(&self, table_id: usize, key: i64, txn_id: usize) -> bool {
        let mut entry = self.table
            .entry((table_id, key))
            .or_insert_with(LockEntry::default);

        let ok = entry.exclusive.is_none() || entry.exclusive == Some(txn_id);
        if ok && !entry.shared.contains(&txn_id) {
            entry.shared.push(txn_id);
        }
        ok
    }

    pub fn acquire_exclusive(&self, table_id: usize, key: i64, txn_id: usize) -> bool {
        let mut entry = self.table
            .entry((table_id, key))
            .or_insert_with(LockEntry::default);

        let other_shared = entry.shared.iter().any(|&id| id != txn_id);
        let other_exclusive = entry.exclusive.map_or(false, |id| id != txn_id);

        if other_shared || other_exclusive {
            return false;
        }

        entry.exclusive = Some(txn_id);
        if !entry.shared.contains(&txn_id) {
            entry.shared.push(txn_id);
        }
        true
    }
    
    pub fn release_locks(&self, txn_id: usize, held: &[(usize, i64)]) {
        for &(table_id, key) in held {
            let remove = {
                if let Some(mut entry) = self.table.get_mut(&(table_id, key)) {
                    entry.shared.retain(|&id| id != txn_id);
                    if entry.exclusive == Some(txn_id) {
                        entry.exclusive = None;
                    }
                    entry.shared.is_empty() && entry.exclusive.is_none()
                } else {
                    false
                }
            };
            if remove {
                // Entry is empty; clean it up so the map doesn't grow forever.
                // remove_if avoids a TOCTOU race: only remove if still empty.
                self.table.remove_if(&(table_id, key), |_, entry| {
                    entry.shared.is_empty() && entry.exclusive.is_none()
                });
            }
        }
    }
}

static LOCK_MANAGER: OnceLock<LockManager> = OnceLock::new();

pub fn lock_manager() -> &'static LockManager {
    LOCK_MANAGER.get_or_init(LockManager::new)
}