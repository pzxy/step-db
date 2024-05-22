use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::Relaxed;
use crate::cache::area::{Area};
use crate::cache::entry::{Entry, Value};
use crate::cache::skiplist;
use crate::cache::utils::compare_keys;

pub const MAX_HEIGHT: usize = 20;

#[derive(Debug, Default)]
pub struct Node {
    value: AtomicU64,
    key_offset: u32,
    key_size: u16,
    height: u16,
    tower: [AtomicU32; MAX_HEIGHT],
}

impl Node {
    pub fn get_next_offset(&self, h: i32) -> u32 {
        self.tower[h as usize].load(Relaxed)
    }
    pub fn get_value_offset(&self) -> (u32, u32) {
        let i = self.value.load(Relaxed);
        return skiplist::decode_value(i);
    }
}

fn new_node(area: &mut Area, key: Vec<u8>, v: Value, height: usize) -> Rc<&mut Node> {
    let node_offset = area.put_node(height);
    let key_offset = area.put_key(key.clone());
    let val = crate::cache::skiplist::encode_value(area.put_value(&v), v.encoded_size() as u32);
    let mut node = area.get_node_mut(node_offset).unwrap();
    {
        let n = Rc::get_mut(&mut node).unwrap();
        n.key_offset = key_offset;
        n.key_size = key.len() as u16;
        n.height = height as u16;
        n.value = AtomicU64::from(val as u64);
    }
    node
}


pub struct SkipList {
    height: AtomicI32,
    head_offset: u32,
    area: Area,
}

fn new_skip_list(area_size: u32) -> Box<SkipList> {
    let mut area = Area::new(area_size);
    let mut head = Rc::new(&mut Node::default());
    {
        head = new_node(&mut area, vec![], Value::default(), MAX_HEIGHT);
    }
    let area_ref: &Area = &area;
    let head_offset = area_ref.get_node_offset(Rc::clone(&head).as_ref());
    let s = Box::new(SkipList {
        height: AtomicI32::new(1),
        head_offset,
        area,
    });
    return s;
}

impl SkipList {
    fn add(&mut self, e: Entry) {
        let key = e.key;
        let v = Value {
            meta: e.meta,
            v: e.value,
            expires_at: e.expires_at,
            version: e.version,
        };
        let list_height = self.height.load(Relaxed);
        let mut prev = [0u32; MAX_HEIGHT + 1];
        let mut next = [0u32; MAX_HEIGHT + 1];
        prev[list_height as usize] = self.head_offset;
        for i in (0..list_height).rev() {
            // Use higher level to speed up for current level.
            (prev[i as usize], next[i as usize]) = self.find_splice_for_level(&key, prev[(i + 1) as usize], i);
            if prev[i as usize] == next[i as usize] {
                let vo = self.area.put_value(&v);
                let enc_value = encode_value(vo, v.encoded_size() as u32);
                let prev_node = self.area.get_node_mut(prev[i as usize]).unwrap();
                prev_node.value.store(enc_value, Relaxed);
                return;
            }
        }
    }
    // findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
    // The input "before" tells us where to start looking.
    // If we found a node with the same key, then we return outBefore = outAfter.
    // Otherwise, outBefore.key < key < outAfter.key.
    fn find_splice_for_level(&mut self, key: &[u8], before: u32, level: i32) -> (u32, u32) {
        loop {
            let mut before = before;
            // Assume before.key < key.
            let before_node = self.area.get_node(before).unwrap();
            let next = before_node.get_next_offset(level);

            let (mut key_offset, mut key_size) = (0, 0);
            {
                let next_node = self.area.get_node(next);
                if next_node.is_none() {
                    return (before, next);
                }
                let next_node = next_node.unwrap();
                key_offset = next_node.key_offset;
                key_size = next_node.key_size;
            }
            let next_key = self.area.get_key(key_offset, key_size);
            let cmp = compare_keys(key, &next_key);
            if cmp == 0 {
                // Equality case.
                return (next, next);
            }
            if cmp < 0 {
                // before.key < key < next.key. We are done for this level.
                return (before, next);
            }
            before = next; // Keep moving right on this level.
        }
    }
}


impl SkipList {
    pub fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (Option<Rc<&Node>>, bool) {
        let mut x = self.get_head().unwrap();
        let mut level = (self.get_height() - 1) as i32;

        loop {
            // Assume x.key < key.
            let next = self.get_next(Rc::clone(&x).as_ref(), level);
            if next.is_none() {
                // x.key < key < END OF LIST
                if level > 0 {
                    // Can descend further to iterate closer to the end.
                    level -= 1;
                    continue;
                }
                // Level=0. Cannot descend further. Let's return something that makes sense.
                if !less {
                    return (None, false);
                }
                // Try to return x. Make sure it is not a head node.
                if x.key_offset.eq(&self.get_head().unwrap().key_offset) {
                    return (None, false);
                }
                return (Some(x), false);
            }
            let next = next.unwrap();
            let next_key = self.area.get_key(next.key_offset, next.key_size);
            let cmp = compare_keys(key, &next_key);
            if cmp > 0 {
                // x.key < next.key < key. We can continue to move right.
                x = next;
                continue;
            }
            if cmp == 0 {
                // x.key < key == next.key.
                if allow_equal {
                    return (Some(next), true);
                }
                if !less {
                    // We want >, so go to base level to grab the next bigger note.
                    return (self.get_next(Rc::clone(&next).as_ref(), 0), false);
                }
                // We want <. If not base level, we should go closer in the next level.
                if level > 0 {
                    level -= 1;
                    continue;
                }
                // On base level. Return x.
                if x.key_offset.eq(&self.get_head().unwrap().key_offset) {
                    return (None, false);
                }
                return (Some(x), false);
            }
            // cmp < 0. In other words, x.key < key < next.
            if level > 0 {
                level -= 1;
                continue;
            }
            // At base level. Need to return something.
            if !less {
                return (Some(next), false);
            }
            // Try to return x. Make sure it is not a head node.
            if x.key_offset.eq(&self.get_head().unwrap().key_offset) {
                return (None, false);
            }
            return (Some(x), false);
        }
    }

    pub fn search(&self, key: &[u8]) -> Value {
        let (n, _) = self.find_near(key, false, true); // findGreaterOrEqual.
        if n.is_none() {
            return Value::default();
        }
        let n = n.unwrap();
        let next_key = self.area.get_key(n.key_offset, n.key_size);
        if !same_key(key, &next_key) {
            return Value::default();
        }

        let (val_offset, val_size) = n.get_value_offset();
        let vs = self.area.get_value(val_offset, val_size);
        vs
    }
}

impl SkipList {
    pub fn get_next(&self, nd: &Node, height: i32) -> Option<Rc<&Node>> {
        self.area.get_node(nd.get_next_offset(height))
    }

    pub fn get_head(&self) -> Option<Rc<&Node>> {
        self.area.get_node(self.head_offset)
    }

    pub fn get_height(&self) -> i32 {
        self.height.load(Relaxed)
    }
}


fn encode_value(val_offset: u32, val_size: u32) -> u64 {
    (u64::from(val_size) << 32) | u64::from(val_offset)
}

fn decode_value(value: u64) -> (u32, u32) {
    let val_offset = value as u32;
    let val_size = (value >> 32) as u32;
    (val_offset, val_size)
}

// ParseKey parses the actual key from the key bytes.
fn parse_key(key: &[u8]) -> &[u8] {
    if key.len() < 8 {
        key
    } else {
        &key[..key.len() - 8]
    }
}

// ParseTs parses the timestamp from the key bytes.
fn parse_ts(key: &[u8]) -> u64 {
    if key.len() <= 8 {
        0
    } else {
        u64::MAX - u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
    }
}

// SameKey checks for key equality ignoring the version timestamp suffix.
fn same_key(src: &[u8], dst: &[u8]) -> bool {
    if src.len() != dst.len() {
        return false;
    }
    parse_key(src) == parse_key(dst)
}

// KeyWithTs generates a new key by appending ts to key.
fn key_with_ts(key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 8);
    out.extend_from_slice(key);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use crate::cache::area::Area;
    use crate::cache::entry::{new_entry, Value};
    use crate::cache::skiplist::new_skip_list;

    #[test]
    fn test_node() {
        let height = 20;
        let key1 = "key1".to_string();
        let value1 = "value1".to_string();
        let v = Value::default();
        let mut area = Area::new(1000);
        let node_offset = area.put_node(height);
        let key_offset = area.put_key(key1.into_bytes().clone());
        // area.put_value()
        let value_offset = area.put_value(&v);
        // let v =  v.encoded_size();

        let mut node = area.get_node(node_offset).unwrap();
        assert_eq!(node.tower.len(), height);
    }

    #[test]
    fn test_skip_list() {
        let mut list = new_skip_list(1000);
        let k1 = "key1";
        let v1 = "value1";
        let entry1 = new_entry(k1.as_bytes(), v1.as_bytes());
        list.add(entry1);
        let value = list.search(k1.as_bytes());
        assert_eq!(*k1.as_bytes(), value.v)
    }
}
