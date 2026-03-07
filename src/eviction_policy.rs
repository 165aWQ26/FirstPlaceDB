use lru::LruCache;
use crate::bufferpool::FrameId;

pub struct EvictionPolicy {
    t1: LruCache<FrameId, ()>,
    t2: LruCache<FrameId, ()>,
    b1: LruCache<FrameId, ()>,
    b2: LruCache<FrameId, ()>,

    p: usize,

    capacity: usize,

    ghost_cap: usize,

    frames: Vec<FrameId>,
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

    fn push_b1(&mut self, frameId: FrameId) {
        if self.b1.len() >= self.ghost_cap {
            self.b1.pop_lru();
        }
        self.b1.push(frameId, ());
    }

    fn push_b2(&mut self, frameId: FrameId) {
        if self.b2.len() >= self.ghost_cap {
            self.b2.pop_lru();
        }
        self.b2.push(frameId, ());
    }

    fn remove_b1(&mut self, frameId: FrameId) {
        self.b1.pop(&frameId);
    }

    fn remove_b2(&mut self, frameId: FrameId) {
        self.b2.pop(&frameId);
    }

    fn trim_ghosts(&mut self) {
        if self.b1.len() >= self.b2.len() {
            self.b1.pop_lru();
        } else {
            self.b2.pop_lru();
        }
    }

    fn on_access(&mut self, frameId: FrameId) {
        if self.t1.pop(&frameId).is_some() {
            self.t2.push(frameId, ());
        } else if self.t2.contains(&frameId) {
            self.t2.promote(&frameId);
        }
    }

    fn on_insert(&mut self, frameId: FrameId) -> Option<FrameId> {
        let cache_size = self.t1.len() + self.t2.len();

        if self.b1.contains(&frameId) {
            // B1 ghost hit → recency demand strong → grow p.
            let delta = if !self.b1.is_empty() {
                (self.b2.len() / self.b1.len()).max(1)
            } else {
                1
            };
            self.p = (self.p + delta).min(self.capacity);

            let victim = if cache_size >= self.capacity {
                self.replace(false)
            } else {
                None
            };
            self.remove_b1(frameId);
            self.t2.push(frameId, ());
            return victim;
        }

        if self.b2.contains(&frameId) {
            // B2 ghost hit → frequency demand strong → shrink p.
            let delta = if !self.b2.is_empty() {
                (self.b1.len() / self.b2.len()).max(1)
            } else {
                1
            };
            self.p = self.p.saturating_sub(delta);

            let victim = if cache_size >= self.capacity {
                self.replace(true)
            } else {
                None
            };
            self.remove_b2(frameId);
            self.t2.push(frameId, ());
            return victim;
        }

        // Full miss - brand new page.
        let victim = if cache_size >= self.capacity {
            if self.t1.len() < self.capacity {
                let v = self.replace(false);
                if self.b1.len() + self.b2.len() >= self.ghost_cap {
                    self.trim_ghosts();
                }
                v
            } else {
                // T1 alone fills the cache; evict directly without recording in
                // B1 to avoid inflating ghosts when the frequency set is empty.
                self.t1.pop_lru().map(|(frameId, _)| frameId)
            }
        } else {
            None
        };

        self.t1.push(frameId, ());
        victim
    }
}