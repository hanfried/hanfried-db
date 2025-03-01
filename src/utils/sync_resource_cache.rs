use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};

struct Item<V> {
    resource: Option<V>,
    // freshness: u64,
}

pub struct SyncResourceCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone,
{
    internal_hash_map: Arc<RwLock<HashMap<K, Item<V>>>>,
    resource_capacity: usize,
    open_resources: AtomicUsize,
}

impl<K, V> SyncResourceCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone,
{
    pub fn new(resource_capacity: usize) -> Self {
        SyncResourceCache {
            internal_hash_map: Arc::new(RwLock::new(HashMap::new())),
            resource_capacity,
            open_resources: AtomicUsize::new(0),
        }
    }

    pub fn len_known(&self) -> usize {
        self.internal_hash_map.read().unwrap().len()
    }

    pub fn len_open(&self) -> usize {
        self.open_resources.load(Relaxed)
        // self.internal_hash_map
        //     .read()
        //     .unwrap()
        //     .values()
        //     .filter(|v| v.resource.is_some())
        //     .count()
    }

    pub fn capacity(&self) -> usize {
        self.resource_capacity
    }

    pub fn knows_key(&self, key: &K) {
        self.internal_hash_map.read().unwrap().contains_key(key);
    }

    pub fn resource_is_open(&self, key: &K) -> bool {
        match self.internal_hash_map.read().unwrap().get(key) {
            Some(item) => item.resource.is_some(),
            None => false,
        }
    }

    pub fn get_or_insert<F, E>(&self, key: K, resource_constructor: F) -> Result<V, E>
    where
        F: Fn() -> Result<V, E>,
    {
        {
            let cache_read_lock = self.internal_hash_map.read().unwrap();
            if let Some(item) = cache_read_lock.get(&key) {
                if let Some(r) = item.resource.clone() {
                    return Ok(r);
                }
            }
        }
        let mut cache_write_lock = self.internal_hash_map.write().unwrap();
        if let Some(item) = cache_write_lock.get(&key) {
            if let Some(r) = item.resource.clone() {
                return Ok(r);
            }
        }
        println!(
            "cache_write_lock len={} vs self.resource_capacity={}",
            cache_write_lock.len(),
            self.resource_capacity
        );

        let resource = resource_constructor()?;
        if self.open_resources.load(Relaxed) < self.resource_capacity {
            self.open_resources.fetch_add(1, Relaxed);
        } else {
            let first = cache_write_lock.iter_mut().take(1);
            first.for_each(|(_, v)| v.resource = None)
        }

        cache_write_lock.insert(
            key,
            Item {
                resource: Some(resource.clone()),
            },
        );
        Ok(resource.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::sync_resource_cache::SyncResourceCache;
    use std::collections::HashMap;

    fn upper(s: &str) -> Result<String, ()> {
        Ok(s.to_uppercase())
    }

    #[test]
    fn test_resource_cache() {
        let cache: SyncResourceCache<String, String> = SyncResourceCache::new(3);
        assert_eq!(cache.capacity(), 3);
        assert_eq!(cache.len_known(), 0);
        assert_eq!(cache.len_open(), 0);

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
        assert_eq!(cache.len_known(), 3);
        assert_eq!(cache.len_open(), 3);

        assert_eq!(
            cache
                .get_or_insert(String::from("new1"), || upper("NEW1"))
                .unwrap(),
            String::from("NEW1")
        );
        assert_eq!(cache.len_known(), 4);
        assert_eq!(cache.len_open(), 3);
        assert_eq!(cache.resource_is_open(&String::from("foo")), false);
        assert_eq!(cache.resource_is_open(&String::from("bar")), true);
        assert_eq!(cache.resource_is_open(&String::from("foobar")), true);
        assert_eq!(cache.resource_is_open(&String::from("new1")), false);
    }
}
