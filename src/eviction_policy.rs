use crate::bufferpool::FrameId;
use lru::LruCache;

pub struct EvictionPolicy {
    t1: LruCache<FrameId, ()>,
    t2: LruCache<FrameId, ()>,
    b1: LruCache<FrameId, ()>,
    b2: LruCache<FrameId, ()>,
    p: usize,
    capacity: usize,
    ghost_cap: usize,
}

impl EvictionPolicy {
    pub fn new(capacity: usize) -> Self {
        Self::with_ghost_cap(capacity, capacity)
    }

    pub fn with_ghost_cap(capacity: usize, ghost_cap: usize) -> Self {
        Self {
            t1: LruCache::unbounded(),
            t2: LruCache::unbounded(),
            b1: LruCache::unbounded(),
            b2: LruCache::unbounded(),
            p: 0,
            capacity,
            ghost_cap,
        }
    }

    pub fn len(&self) -> usize {
        self.t1.len() + self.t2.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn replace(&mut self, prefer_t2: bool) -> Option<FrameId> {
        let evict_t1 = !self.t1.is_empty()
            && (self.t1.len() > self.p || (self.t1.len() == self.p && !prefer_t2));

        if evict_t1 {
            let (victim, _) = self.t1.pop_lru().unwrap();
            Some(victim)
        } else if !self.t2.is_empty() {
            let (victim, _) = self.t2.pop_lru().unwrap();
            self.push_b2(victim);
            Some(victim)
        } else if !self.t1.is_empty() {
            // Fallback: T2 empty, evict T1 regardless.
            let (victim, _) = self.t1.pop_lru().unwrap();
            self.push_b1(victim);
            Some(victim)
        } else {
            None
        }
    }

    fn push_b1(&mut self, fid: FrameId) {
        if self.b1.len() >= self.ghost_cap {
            self.b1.pop_lru();
        }
        self.b1.push(fid, ());
    }

    fn push_b2(&mut self, fid: FrameId) {
        if self.b2.len() >= self.ghost_cap {
            self.b2.pop_lru();
        }
        self.b2.push(fid, ());
    }

    fn remove_b1(&mut self, fid: FrameId) {
        self.b1.pop(&fid);
    }

    fn remove_b2(&mut self, fid: FrameId) {
        self.b2.pop(&fid);
    }

    fn trim_ghosts(&mut self) {
        if self.b1.len() >= self.b2.len() {
            self.b1.pop_lru();
        } else {
            self.b2.pop_lru();
        }
    }

    pub(super) fn on_access(&mut self, fid: FrameId) {
        if self.t1.pop(&fid).is_some() {
            self.t2.push(fid, ());
        } else if self.t2.contains(&fid) {
            self.t2.promote(&fid);
        }
    }

    pub(super) fn on_insert(&mut self, fid: FrameId) {
        if self.b1.contains(&fid) {
            // B1 ghost hit → recency demand strong -> grow p.
            let delta = if !self.b1.is_empty() {
                (self.b2.len() / self.b1.len()).max(1)
            } else {
                1
            };
            self.p = (self.p + delta).min(self.capacity);
            self.remove_b1(fid);
            self.t2.push(fid, ());
            return;
        }

        if self.b2.contains(&fid) {
            // B2 ghost hit → frequency demand strong → shrink p.
            let delta = if !self.b2.is_empty() {
                (self.b1.len() / self.b2.len()).max(1)
            } else {
                1
            };
            self.p = self.p.saturating_sub(delta);

            self.remove_b2(fid);
            self.t2.push(fid, ());
            return;
        }
        // Full miss
        self.t1.push(fid, ());
    }

    pub(super) fn evict_victim(&mut self) -> Option<FrameId> {
        let cache_size = self.t1.len() + self.t2.len();
        if cache_size < self.capacity {
            return None;
        }
        if self.t1.len() < self.capacity {
            let victim = self.replace(false);
            if self.b1.len() + self.b2.len() >= self.ghost_cap {
                self.trim_ghosts();
            }
            victim
        } else {
            self.b1.pop_lru().map(|(fid, _)| fid)
        }
    }
}
