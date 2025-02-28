use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, RwLock};

#[derive(Debug)]
struct Item<V> {
    value: V,
    last_access: AtomicU64,
}

#[derive(Debug)]
pub struct SyncCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone, // + Display,
{
    internal_hash_map: Arc<RwLock<HashMap<K, Item<V>>>>,
    max_size: AtomicUsize,
    access_counter: AtomicU64,
    access_pivot: AtomicU64,
}

impl<K, V> SyncCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone, // + Display,
{
    pub fn new(max_size: NonZeroUsize) -> Self {
        let max_size = usize::from(max_size);
        SyncCache {
            internal_hash_map: Arc::new(RwLock::new(HashMap::with_capacity(max_size))),
            max_size: AtomicUsize::new(max_size),
            access_counter: AtomicU64::new(0),
            access_pivot: AtomicU64::new(0),
        }
    }

    fn get_value_and_refresh_access(&self, item: &Item<V>) -> V {
        loop {
            let t_before = item.last_access.load(Relaxed);
            let t_after = self.access_counter.fetch_add(1, Relaxed) + (t_before >> 1);
            if item
                .last_access
                .compare_exchange_weak(t_before, t_after, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }
        item.value.clone()
    }

    pub fn get_or_insert<F, E>(&self, key: K, value_constructor: F) -> Result<V, E>
    where
        F: Fn() -> Result<V, E>,
    {
        {
            let cache_read_lock = self.internal_hash_map.read().unwrap();
            if let Some(item) = cache_read_lock.get(&key) {
                println!("Found cached entry for {:?} with read lock", key);
                return Ok(self.get_value_and_refresh_access(item));
            }
        }

        let mut cache_write_lock = self.internal_hash_map.write().unwrap();
        if let Some(item) = cache_write_lock.get(&key) {
            println!("Found cached entry for {:?} with write lock", key);
            return Ok(self.get_value_and_refresh_access(item));
        }
        while cache_write_lock.len() >= self.max_size.load(Relaxed) {
            println!(
                "Shrinken HashMap len={} capacity={} self.access_pivot={}",
                cache_write_lock.len(),
                cache_write_lock.capacity(),
                self.access_pivot.load(Relaxed)
            );
            let mut i = 0;
            let pivot_i = ((cache_write_lock.len() as f64) * 0.38).floor() as i64;
            let mut min_access_to_pivot = u64::MAX;
            let access_pivot = self.access_pivot.load(Relaxed);
            println!(
                "i={}, pivot_i={}, min_access_to_pivot={}, access_pivot={}",
                i, pivot_i, min_access_to_pivot, access_pivot
            );
            cache_write_lock.retain(|k, item| {
                if *k == key {
                    return true
                }
                let last_access = item.last_access.load(Relaxed);
                if i <= pivot_i {
                    min_access_to_pivot = min_access_to_pivot.min(last_access);
                } else if i == pivot_i + 1 {
                    self.access_pivot.store(min_access_to_pivot, Relaxed);
                }
                let last_access_quite_a_time_ago = last_access <= access_pivot
                    || (last_access <= min_access_to_pivot && i >= pivot_i);
                println!("i={}, k={:?}, last_access={}, min_access_so_far={}, last_access_quite_a_time_ago={}", i, k, last_access, min_access_to_pivot, last_access_quite_a_time_ago);
                i += 1;
                !last_access_quite_a_time_ago
            });
            // self.access_pivot.store(min_access_so_far, Relaxed);
        }

        // naive implementation, rust is picky about read() -> then write()
        // let key_with_smallest_access = {
        //     cache_write_lock
        //         .iter()
        //         .min_by(|a, b| a.1.last_access.load(Relaxed).cmp(&b.1.last_access.load(Relaxed)))
        //         .unwrap()
        //         .0
        // };
        // cache_write_lock.remove(key_with_smallest_access);
        // if *key_with_smallest_access != key {
        //     cache_write_lock.remove(key_with_smallest_access);
        // }
        // }
        println!("Create new value for {:?}", key);
        let value = value_constructor()?;
        cache_write_lock.insert(
            key,
            Item {
                value: value.clone(),
                last_access: AtomicU64::new(self.access_counter.fetch_add(1, Relaxed)),
            },
        );
        Ok(value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.internal_hash_map.read().unwrap().contains_key(key)
    }

    pub fn get_cached(&self, key: &K) -> Option<V> {
        self.internal_hash_map
            .read()
            .unwrap()
            .get(key)
            .map(|item| self.get_value_and_refresh_access(item))
    }

    pub fn len(&self) -> usize {
        self.internal_hash_map.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.internal_hash_map.read().unwrap().is_empty()
    }

    pub fn max_size(&self) -> usize {
        self.max_size.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::sync_cache::SyncCache;
    use std::num::NonZeroUsize;

    fn upper(s: &str) -> Result<String, ()> {
        Ok(s.to_uppercase())
    }

    #[test]
    fn test_cache() {
        let cache: SyncCache<String, String> = SyncCache::new(NonZeroUsize::new(3).unwrap());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.max_size(), 3);

        assert_eq!(
            cache
                .get_or_insert(String::from("foo"), || upper("foo"))
                .unwrap(),
            String::from("FOO")
        );
        assert_eq!(
            cache
                .get_or_insert(String::from("bar"), || upper("bar"))
                .unwrap(),
            String::from("BAR")
        );
        assert_eq!(
            cache
                .get_or_insert(String::from("foobar"), || upper("FOOBAR"))
                .unwrap(),
            String::from("FOOBAR")
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.max_size(), 3);

        assert_eq!(
            cache
                .get_or_insert(String::from("new1"), || upper("NEW1"))
                .unwrap(),
            String::from("NEW1")
        );
        assert!(cache.len() <= 3);
        assert!(cache.max_size() <= 3);

        assert_eq!(cache.contains_key(&String::from("new1")), true);
        assert_eq!(cache.contains_key(&String::from("foo")), false);
    }
}
