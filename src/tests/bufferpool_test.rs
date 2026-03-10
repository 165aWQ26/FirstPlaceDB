#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::bufferpool::{BufferPool, DiskManager, BP_CAP};
    use crate::page_collection::PageId;

    static BP_TEST_CTR: AtomicUsize = AtomicUsize::new(0);

    fn make_bp() -> (Arc<BufferPool>, String) {
        let id = BP_TEST_CTR.fetch_add(1, Ordering::Relaxed);
        let dir = format!("./test_tmp/bp_{}_{}", std::process::id(), id);
        let _ = std::fs::remove_dir_all(&dir);
        let bp = Arc::new(BufferPool::new(
            Arc::new(parking_lot::RwLock::new(DiskManager::new(&dir).unwrap()))
        ));
        (bp, dir)
    }

    #[test]
    fn write_and_read_single_page() {
        let (bp, _dir) = make_bp();
        let pid = PageId::new(0, 0);
        bp.write(pid, Some(42), 0).unwrap();
        assert_eq!(bp.read(pid, 0).unwrap(), Some(42));
    }

    #[test]
    fn write_none_reads_none() {
        let (bp, _dir) = make_bp();
        let pid = PageId::new(0, 0);
        bp.write(pid, None, 0).unwrap();
        assert_eq!(bp.read(pid, 0).unwrap(), None);
    }

    #[test]
    fn write_multiple_offsets() {
        let (bp, _dir) = make_bp();
        let pid = PageId::new(0, 0);
        for i in 0..10usize {
            bp.write(pid, Some(i as i64), i).unwrap();
        }
        for i in 0..10usize {
            assert_eq!(bp.read(pid, i).unwrap(), Some(i as i64));
        }
    }

    #[test]
    fn update_overwrites_value() {
        let (bp, _dir) = make_bp();
        let pid = PageId::new(0, 0);
        bp.write(pid, Some(10), 0).unwrap();
        bp.update(pid, 0, Some(99)).unwrap();
        assert_eq!(bp.read(pid, 0).unwrap(), Some(99));
    }

    #[test]
    fn update_to_none() {
        let (bp, _dir) = make_bp();
        let pid = PageId::new(0, 0);
        bp.write(pid, Some(10), 0).unwrap();
        bp.update(pid, 0, None).unwrap();
        assert_eq!(bp.read(pid, 0).unwrap(), None);
    }

    #[test]
    fn distinct_pages_independent() {
        let (bp, _dir) = make_bp();
        let p0 = PageId::new(0, 0);
        let p1 = PageId::new(1, 0);
        bp.write(p0, Some(1), 0).unwrap();
        bp.write(p1, Some(2), 0).unwrap();
        assert_eq!(bp.read(p0, 0).unwrap(), Some(1));
        assert_eq!(bp.read(p1, 0).unwrap(), Some(2));
    }

    #[test]
    fn eviction_and_reload() {
        let (bp, _dir) = make_bp();
        // Write to more pages than BP_CAP to force eviction
        for i in 0..(BP_CAP + 10) {
            let pid = PageId::new(i, 0);
            bp.write(pid, Some(i as i64), 0).unwrap();
        }
        // First pages should have been evicted; reading them reloads from disk
        for i in 0..10usize {
            let pid = PageId::new(i, 0);
            assert_eq!(bp.read(pid, 0).unwrap(), Some(i as i64));
        }
    }

    #[test]
    fn evict_all_flushes_dirty_pages() {
        let (bp, dir) = make_bp();
        let pid = PageId::new(0, 0);
        bp.write(pid, Some(123), 0).unwrap();
        bp.evict_all().unwrap();

        // Reload via a fresh pool pointed at the same dir
        let dm = Arc::new(parking_lot::RwLock::new(DiskManager::new(&dir).unwrap()));
        let bp2 = BufferPool::new(dm);
        assert_eq!(bp2.read(pid, 0).unwrap(), Some(123));
    }
}