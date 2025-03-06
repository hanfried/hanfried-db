use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, RwLock};

#[derive(Debug)]
struct Item<V> {
    resource: Option<V>,
    freshness: AtomicU64,
}

#[derive(Debug)]
pub struct SyncResourceCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone + Debug,
{
    internal_hash_map: Arc<RwLock<HashMap<K, Item<V>>>>,
    resource_capacity: usize,
    open_resources: AtomicUsize,
    access_counter: AtomicU64,
}

impl<K, V> SyncResourceCache<K, V>
where
    K: Eq + Hash + Debug,
    V: Clone + Debug,
{
    pub fn new(resource_capacity: usize) -> Self {
        SyncResourceCache {
            internal_hash_map: Arc::new(RwLock::new(HashMap::new())),
            resource_capacity,
            open_resources: AtomicUsize::new(0),
            access_counter: AtomicU64::new(0),
        }
    }

    pub fn len_known(&self) -> usize {
        self.internal_hash_map.read().unwrap().len()
    }

    pub fn len_open(&self) -> usize {
        self.open_resources.load(Relaxed)
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

    fn refresh_access(&self, item: &Item<V>) {
        loop {
            let freshness_before = item.freshness.load(Relaxed);
            let freshness_after =
                self.access_counter.fetch_add(1, Relaxed) + (freshness_before >> 1);
            if item
                .freshness
                .compare_exchange_weak(freshness_before, freshness_after, Relaxed, Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn get_or_create<F, E>(&self, key: K, resource_constructor: F) -> Result<V, E>
    where
        F: Fn() -> Result<V, E>,
    {
        {
            let cache_read_lock = self.internal_hash_map.read().unwrap();
            if let Some(item) = cache_read_lock.get(&key) {
                if let Some(r) = item.resource.clone() {
                    // println!("Found resource from read cache {:?} {:?}", &key, r);
                    return Ok(r);
                }
            }
        }
        let mut cache_write_lock = self.internal_hash_map.write().unwrap();
        if let Some(item) = cache_write_lock.get(&key) {
            if let Some(r) = item.resource.clone() {
                // println!("Found resource in write cache {:?} {:?}", &key, r);
                self.refresh_access(item);
                return Ok(r);
            }
        }
        // println!("Create resource for key={:?}", key);
        let resource = resource_constructor()?;
        if self.open_resources.load(Relaxed) < self.resource_capacity {
            self.open_resources.fetch_add(1, Relaxed);
        } else {
            let item = cache_write_lock
                .values_mut()
                .filter(|item| item.resource.is_some())
                .min_by_key(|item| item.freshness.load(Relaxed))
                .unwrap();
            // println!("Item's resource had been key={:?} {:?}", key, item.resource);
            // resource_destructor(item.resource.as_ref().unwrap())?;
            item.resource = None;
            // println!("Set resource to None for key={:?}", key)
        }

        cache_write_lock.insert(
            key,
            Item {
                resource: Some(resource.clone()),
                freshness: AtomicU64::new(self.access_counter.fetch_add(1, Relaxed)),
            },
        );
        Ok(resource.clone())
    }

    pub fn for_each(&self, mut f: impl FnMut(&V)) {
        self.internal_hash_map
            .read()
            .unwrap()
            .values()
            .for_each(|item| {
                if let Some(r) = item.resource.as_ref() {
                    f(r)
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::sync_resource_cache::SyncResourceCache;

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
                .get_or_create(String::from("foo"), || upper("foo"))
                .unwrap(),
            String::from("FOO")
        );

        assert_eq!(
            cache
                .get_or_create(String::from("bar"), || upper("bar"))
                .unwrap(),
            String::from("BAR")
        );
        assert_eq!(
            cache
                .get_or_create(String::from("foobar"), || upper("FOOBAR"))
                .unwrap(),
            String::from("FOOBAR")
        );
        assert_eq!(cache.len_known(), 3);
        assert_eq!(cache.len_open(), 3);

        assert_eq!(
            cache
                .get_or_create(String::from("new1"), || upper("NEW1"))
                .unwrap(),
            String::from("NEW1")
        );
        assert_eq!(cache.len_known(), 4);
        assert_eq!(cache.len_open(), 3);
        assert_eq!(cache.resource_is_open(&String::from("foo")), false);
        assert_eq!(cache.resource_is_open(&String::from("bar")), true);
        assert_eq!(cache.resource_is_open(&String::from("foobar")), true);
        assert_eq!(cache.resource_is_open(&String::from("new1")), true);
        println!("cache {:?}", cache)
    }
}
