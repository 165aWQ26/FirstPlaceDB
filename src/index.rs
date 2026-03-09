use crossbeam_skiplist::{SkipSet,SkipMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Index {
    inner: IndexInner,
    enabled: AtomicBool
}

enum IndexInner {
    Unique(SkipMap<i64, i64>),
    NonUnique(SkipSet<(i64, i64)>),
}

impl Index {
    pub fn new_unique() -> Self {
        Self {
            inner: IndexInner::Unique(SkipMap::new()),
            enabled: AtomicBool::new(true)
        }
    }
    pub fn new_non_unique() -> Self {
        Self {
            inner: IndexInner::NonUnique(SkipSet::new()),
            enabled: AtomicBool::new(true)
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn insert(&self, key: i64, rid: i64) {
        if !self.is_enabled() {
            return;
        }
        match &self.inner {
            IndexInner::Unique(map) => {map.insert(key,rid);}
            IndexInner::NonUnique(set) => {set.insert((key,rid));}
        }
    }

    pub fn insert_unique(&self, key: i64, rid: i64) -> bool {
        match &self.inner {
            IndexInner::Unique(map) => {
                let entry = map.get_or_insert(key, rid);
                *entry.value() == rid
            }
            IndexInner::NonUnique(_) => panic!("insert_unique called on non-unique index"),
        }
    }

    pub fn remove(&self, key: i64, rid: i64) {
        if !self.is_enabled() {
            return;
        }
        match &self.inner {
            IndexInner::Unique(map) => {map.remove(&key);}
            IndexInner::NonUnique(set) => {set.remove(&(key,rid));}
        }
    }
    pub fn locate(&self, key: i64) -> Option<i64> {
        match &self.inner {
            IndexInner::Unique(map) => map.get(&key).map(|e| *e.value()),
            IndexInner::NonUnique(_) => panic!("locate called on non-unique index"),
        }
    }
    pub fn locate_all(&self, key: i64) -> Vec<i64> {
        if !self.is_enabled() {
            return Vec::new();
        }
        match &self.inner {
            IndexInner::Unique(_) => panic!("locate_all called on unique index"),
            IndexInner::NonUnique(set) => {
                set.range((Bound::Included(&(key, i64::MIN)), Bound::Included(&(key, i64::MAX))))
                    .map(|e| e.value().1)
                    .collect()
            }
        }
    }
    pub fn locate_range(&self, begin: i64, end: i64) -> Vec<i64> {
        match &self.inner {
            IndexInner::Unique(map) => {
                map.range(begin..=end)
                    .map(|e| *e.value())
                    .collect()
            }
            IndexInner::NonUnique(set) => {
                set.range((Bound::Included(&(begin, i64::MIN)), Bound::Included(&(end, i64::MAX))))
                    .map(|e| e.value().1)
                    .collect()
            }
        }
    }

    pub fn all_pairs(&self) -> Vec<(i64, i64)> {
        match &self.inner {
            IndexInner::Unique(map) => map.iter().map(|e| (*e.key(), *e.value())).collect(),
            IndexInner::NonUnique(set) => set.iter().map(|e| (e.value().0, e.value().1)).collect(),
        }
    }
}