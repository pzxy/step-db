use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::rc::Rc;

pub type Item<T> = Rc<RefCell<StoreItem<T>>>;
pub type Map<T> = Rc<RefCell<HashMap<u64, Item<T>>>>;

#[derive(Debug)]
pub struct WindowLRU<T> {
    data: Map<T>,
    cap: usize,
    list: LinkedList<Item<T>>,
}

#[derive(Copy, Clone, Debug)]
pub struct StoreItem<T> {
    pub stage: u8,
    pub key: u64,
    pub conflict: u64,
    pub value: T,
}

pub fn new_lru<T>(size: usize, data: Map<T>) -> WindowLRU<T> {
    WindowLRU {
        data,
        cap: size,
        list: LinkedList::new(),
    }
}

impl<T> WindowLRU<T> {
    pub fn add(&mut self, new_item: StoreItem<T>) -> Option<Rc<RefCell<StoreItem<T>>>> {
        let item = Rc::new(RefCell::new(new_item));

        // If the window's capacity is not full, directly insert the new item
        if self.list.len() < self.cap {
            self.list.push_front(Rc::clone(&item));
            self.data
                .borrow_mut()
                .insert(item.borrow().key, Rc::clone(&item));
            return None;
        }

        // If the window's capacity is full, evict the item from the tail according to the LRU rule
        let evict_item = self.list.pop_back().unwrap();
        self.data.borrow_mut().remove(&evict_item.borrow().key);

        self.list.push_front(Rc::clone(&item));
        self.data
            .borrow_mut()
            .insert(item.borrow().key, Rc::clone(&item));
        Some(evict_item)
    }

    pub fn get(&mut self, key: u64) {
        if let Some(item) = self.remove_item_in_list(key) {
            self.list.push_front(item);
        }
    }
    fn remove_item_in_list(&mut self, key: u64) -> Option<Item<T>> {
        if let Some(pos) = self.list.iter().position(|i| i.borrow().key == key) {
            let mut after = self.list.split_off(pos);
            let ret = after.pop_front();
            self.list.append(&mut after);
            return ret;
        }
        None
    }
}

#[derive(Debug)]
pub struct SegmentedLRU<T> {
    data: Map<T>,
    stage_one_cap: usize,
    stage_two_cap: usize,
    stage_one: LinkedList<Item<T>>,
    stage_two: LinkedList<Item<T>>,
}

const STAGE_ONE: u8 = 1;
const STAGE_TWO: u8 = 2;

pub fn new_slru<T>(stage_one_cap: usize, stage_two_cap: usize, data: Map<T>) -> SegmentedLRU<T> {
    SegmentedLRU {
        data,
        stage_one_cap,
        stage_two_cap,
        stage_one: LinkedList::new(),
        stage_two: LinkedList::new(),
    }
}

impl<T> SegmentedLRU<T> {
    pub fn add(&mut self, item: Item<T>) {
        // New items always start in stage one
        item.borrow_mut().stage = 1;
        let item = Rc::new(item);
        // If stage one is not full and the overall capacity is not reached, we're done
        if self.stage_one.len() < self.stage_one_cap
            || self.len() < self.stage_one_cap + self.stage_two_cap
        {
            self.stage_one.push_front(Rc::clone(&item));
            self.data
                .borrow_mut()
                .insert(item.borrow().key, Rc::clone(&item));
            return;
        }

        // Otherwise, we need to evict from stage one
        let evicted = self.stage_one.pop_back().unwrap();
        self.data.borrow_mut().remove(&evicted.borrow().key);

        self.stage_one.push_front(Rc::clone(&item));
        self.data
            .borrow_mut()
            .insert(item.borrow().key, Rc::clone(&item));
    }

    pub fn get(&mut self, new_item: Item<T>) {
        if STAGE_TWO == new_item.borrow().stage {
            if let Some(v) = self.stage_two.pop_back() {
                self.stage_two.push_front(v);
            };
        }
        // item in stage one, and stage two is not full yet
        if self.stage_two.len() < self.stage_two_cap {
            {
                self.remove_item_in_stage_one(new_item.borrow().key);
                new_item.borrow_mut().stage = STAGE_TWO;
            }
            self.stage_two.push_front(Rc::clone(&new_item));
        } else {
            // move old data from stage two to stage one
            let old = self.stage_two.pop_back().unwrap();

            new_item.borrow_mut().stage = STAGE_TWO;
            self.stage_two.push_front(Rc::clone(&new_item));
            self.data
                .borrow_mut()
                .insert(new_item.borrow().key, Rc::clone(&new_item));

            old.borrow_mut().stage = STAGE_ONE;
            if self.stage_one.len() >= self.stage_one_cap {
                let evicted = self.stage_one.pop_back().unwrap();
                self.data.borrow_mut().remove(&evicted.borrow().key);
            }
            self.stage_one.push_front(Rc::clone(&old));
            self.data
                .borrow_mut()
                .insert(old.borrow().key, Rc::clone(&old));
        }
    }
    fn remove_item_in_stage_one(&mut self, key: u64) -> Option<Item<T>> {
        if let Some(pos) = self.stage_one.iter().position(|i| i.borrow().key == key) {
            let mut after = self.stage_one.split_off(pos);
            let ret = after.pop_front();
            self.stage_one.append(&mut after);
            return ret;
        }
        None
    }
    fn len(&self) -> usize {
        self.stage_one.len() + self.stage_two.len()
    }

    pub fn victim(&self) -> Option<&Item<T>> {
        if self.len() < self.stage_one_cap + self.stage_two_cap {
            return None;
        }
        self.stage_one.back()
    }

    fn key_of(&self, _item: &T) -> u64 {
        // Implement your own key generation logic here
        0
    }

    fn equal(&self, a: &T, b: &T) -> bool {
        // Implement your own equality comparison logic here
        std::ptr::eq(a, b)
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::lru::{new_lru, StoreItem};
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    struct User {
        name: String,
    }

    #[test]
    fn test_lru() {
        let data = Rc::new(RefCell::new(HashMap::new()));
        let mut a = new_lru::<User>(100, data);
        let name = "Ferris".to_string();
        let v = StoreItem {
            stage: 0,
            key: 0,
            conflict: 0,
            value: User { name: name.clone() },
        };
        if let Some(ret) = a.add(v) {
            assert_eq!(name, ret.borrow().value.name)
        }
    }

    #[test]
    fn test_lru2() {}
}
