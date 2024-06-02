use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::RwLock;
use crate::cache::bloom::BloomFilter;
use crate::cache::{bloom, counter};
use crate::cache::counter::CMSketch;
use crate::cache::lru::{Map, new_lru, new_slru, SegmentedLRU, StoreItem, WindowLRU};

pub struct Cache<K:?Sized, V> {
    m: RwLock<u8>,
    lru: WindowLRU<V>,
    slru: SegmentedLRU<V>,
    watch_dog: BloomFilter,
    c: CMSketch,
    t: i32,
    threshold: i32,
    data: Map<V>,
    _pd: PhantomData<K>,
}


// size is the number of data to be cached


impl<K: ?Sized, V> Cache<K, V>
    where K: Hash + Eq,
          V: Clone,
{
    pub fn new(size: usize) -> Self {
        // LRU window size，1% of Total
        let lru_pct = 0.01;
        let lru_sz = ((lru_pct * size as f64) as usize).max(1);
        // SLRU size,99% of Total
        let slru_sz = ((size as f64 * (1.0 - lru_pct)) as usize).max(1);

        // SLRU stage one size,20% of SLRU
        let slru_one = ((0.2 * slru_sz as f64) as usize).max(1);
        // SLRU stage one size,80% of SLRU
        let slru_two = slru_sz - slru_one;
        let data = Rc::new(RefCell::new(HashMap::with_capacity(size)));
        Cache {
            m: Default::default(),
            lru: new_lru(lru_sz, Rc::clone(&data)),
            slru: new_slru(slru_one, slru_two, Rc::clone(&data)),
            watch_dog: bloom::new(size as isize, 0.01),
            c: counter::new(size as u64),
            t: 0,
            threshold: 0,
            data,
            _pd: PhantomData,
        }
    }
    fn set(&mut self, key: &K, value: V) -> bool {
        let _unused = self.m.write().expect("set k-v pairs fail");

        // keyHash is used for quick lookup, conflictHash is used to check for conflicts
        let (key_hash, conflict_hash) = self.key_to_hash(&key);

        // The newly added cache items are first placed in the window LRU, so stage = 0
        let item = StoreItem {
            stage: 0,
            key: key_hash,
            conflict: conflict_hash,
            value,
        };

        // If the window is full, the evicted data is returned
        if let Some(lru_victim) = self.lru.add(item) {
            // If there is evicted data from the window, we need to find a victim from the stageOne part of the SLRU
            // and perform a comparison between the two
            if let Some(slru_victim) = self.slru.victim() {
                // The window LRU's evicted data can enter stageOne since the SLRU is not full
                if !self.watch_dog.allow(lru_victim.borrow().key as u32) {
                    return true;
                }

                let lru_count = self.c.estimate(lru_victim.borrow().key);
                let slru_count = self.c.estimate(slru_victim.borrow().key);

                if lru_count < slru_count {
                    return true;
                }
            } else {
                // The window LRU's evicted data can enter stageOne since the SLRU is not full
                self.slru.add(lru_victim);
                return true;
            }
        } else {
            return true;
        }
        false
    }

    fn key_to_hash(&self, k: &K) -> (u64, u64)
        where
            K: Hash
    {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        let h1 = hasher.finish();
        // TODO: if it is a number does it need to be done?
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        k.hash(&mut hasher);
        let h2 = hasher.finish();
        (h1, h2)
    }

    fn get(&mut self, key: &K) -> (Option<V>) {
        let _unused = self.m.write().expect("get k-v pairs fail");

        self.t += 1;
        if self.t == self.threshold {
            self.c.reset();
            self.watch_dog.reset();
            self.t = 0;
        }

        let (key_hash, conflict_hash) = self.key_to_hash(&key);

        if let Some(item) = self.data.borrow().get(&key_hash) {
            let item_ref = item.borrow();
            if item_ref.conflict != conflict_hash {
                return None;
            }
            self.watch_dog.allow(key_hash as u32);
            self.c.increment(key_hash);

            if item_ref.stage == 0 {
                self.lru.get(item_ref.key);
            } else {
                self.slru.get(Rc::clone(&item));
            }
            return Some(item_ref.value.clone());
        }
        None
    }
    pub fn del(&self, key: &K) -> Option<u64> {
        let _unused = self.m.write().expect("get k-v pairs fail");
        let (key_hash, conflict_hash) = self.key_to_hash(&key);
        if let Some(val) = self.data.borrow().get(&key_hash) {
            let item = val.borrow();
            if conflict_hash != item.conflict {
                return None;
            }
            self.data.borrow_mut().remove(&key_hash);
            return Some(item.conflict);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cache::{Cache};

    #[test]
    fn test_key_to_hash() {
        let a = 12314u64;
        let c = Cache::<u64, u64>::new(100);
        let (h1, h2) = c.key_to_hash(&a);
        // 3962117728473627647,9330451337157661844
        assert_eq!(h1, 3962117728473627647);
        assert_eq!(h2, 9330451337157661844);

        let a = "hello ferris".to_string();
        let c = Cache::<String, u64>::new(100);
        let (h1, h2) = c.key_to_hash(&a);
        // 12643562960511582310,17903442243031495094
        assert_eq!(h1, 12643562960511582310);
        assert_eq!(h2, 17903442243031495094);
    }
}