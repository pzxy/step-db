use crate::cache::area::Area;
use crate::cache::entry::{Entry, Value};
use crate::cache::iterator;
use crate::cache::iterator::SkipListIter;
use crate::cache::utils::compare_keys;
use rand::random;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64};

pub const MAX_HEIGHT: usize = 20;

#[repr(C)]
#[derive(Debug, Default)]
pub struct Node {
    pub value: AtomicU64,
    pub key_offset: u32,
    pub key_size: u16,
    pub height: u16,
    pub(crate) tower: [AtomicU32; MAX_HEIGHT],
}

impl Node {
    pub fn get_next_offset(&self, h: i32) -> u32 {
        self.tower[h as usize].load(Relaxed)
    }
    pub fn get_value_offset(&self) -> (u32, u32) {
        let i = self.value.load(Relaxed);
        return decode_value(i);
    }
    pub fn set_value(&self, vo: u64) {
        self.value.store(vo, Relaxed);
    }
}

fn new_node<'a>(area: &'a Area, key: Vec<u8>, v: &'a Value, height: usize) -> Rc<&'a mut Node> {
    let node_offset = area.put_node(height);
    let key_offset = area.put_key(key.clone());
    let val = encode_value(area.put_value(&v), v.encoded_size() as u32);
    let mut node = area.get_node_mut(node_offset).unwrap();
    {
        let n = Rc::get_mut(&mut node).unwrap();
        n.key_offset = key_offset;
        n.key_size = key.len() as u16;
        n.height = height as u16;
        n.value = AtomicU64::from(val);
        let x = &area.get_buf()[8..104];
        println!("new_node :{:?}", x.to_vec());
    }
    node
}

pub struct SkipList {
    pub height: AtomicI32,
    pub head_offset: u32,
    pub area: Rc<Area>,
}

fn new_skip_list(area_size: u32) -> Box<SkipList> {
    let mut ret = Box::new(SkipList {
        height: AtomicI32::new(1),
        area: Rc::new(Area::new(area_size)),
        head_offset: 0,
    });
    {
        // let area_tmp = Rc::clone(&ret.area);
        let v = Value::default();
        let head = new_node(ret.area.deref(), vec![], &v, MAX_HEIGHT);

        ret.head_offset = ret.area.deref().get_node_offset(Rc::clone(&head).as_ref());
    }
    return ret;
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
        let area_tmp = Rc::clone(&self.area);

        for i in (0..list_height).rev() {
            // Use higher level to speed up for current level.
            (prev[i as usize], next[i as usize]) =
                self.find_splice_for_level(&key, prev[(i + 1) as usize], i);
            if prev[i as usize] == next[i as usize] {
                let vo = area_tmp.put_value(&v);
                let enc_value = encode_value(vo, v.encoded_size() as u32);
                let prev_node = area_tmp.get_node_mut(prev[i as usize]).unwrap();
                prev_node.set_value(enc_value);
                return;
            }
        }
        let height = random_height();
        let mut x = new_node(area_tmp.as_ref(), key.clone(), &v, height);

        let mut list_height = self.get_height();
        while height > list_height as usize {
            if self
                .height
                .compare_exchange(list_height, height as i32, Acquire, Relaxed)
                .is_ok()
            {
                // Successfully increased skiplist.height.
                break;
            }
            list_height = self.get_height();
        }
        for i in 0..height {
            loop {
                if area_tmp.get_node(prev[i]).is_none() {
                    assert!(i > 1); // This cannot happen in base level.
                                    // We haven't computed prev, next for this level because height exceeds old listHeight.
                                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    (prev[i], next[i]) =
                        self.find_splice_for_level(&key, self.head_offset, i as i32);
                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    assert_ne!(prev[i], next[i]);
                }
                {
                    let x_m = Rc::get_mut(&mut x).unwrap();
                    x_m.tower[i] = AtomicU32::from(next[i]);
                }
                if let Some(pnode) = area_tmp.get_node(prev[i]) {
                    if pnode.tower[i]
                        .compare_exchange(next[i], area_tmp.get_node_offset(&x), Acquire, Relaxed)
                        .is_ok()
                    {
                        // Managed to insert x between prev[i] and next[i]. Go to the next level.
                        break;
                    }
                }
                // CAS failed. We need to recompute prev and next.
                // It is unlikely to be helpful to try to use a different level as we redo the search,
                // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                (prev[i], next[i]) = self.find_splice_for_level(&key, prev[i], i as i32);
                if prev[i] == next[i] {
                    assert_eq!(i, 0);
                    let vo = area_tmp.put_value(&v);
                    let enc_value = encode_value(vo, v.encoded_size() as u32);
                    if let Some(prev_node) = area_tmp.get_node(prev[i]) {
                        prev_node.set_value(enc_value);
                    }
                    return;
                }
            }
        }
    }
    // findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
    // The input "before" tells us where to start looking.
    // If we found a node with the same key, then we return outBefore = outAfter.
    // Otherwise, outBefore.key < key < outAfter.key.
    fn find_splice_for_level(&self, key: &[u8], before: u32, level: i32) -> (u32, u32) {
        let area_tmp = Rc::clone(&self.area);
        let mut before = before;
        loop {
            // Assume before.key < key.
            let next = area_tmp.get_node(before).unwrap().get_next_offset(level);

            let next_node = area_tmp.get_node(next);
            if next_node.is_none() {
                return (before, next);
            }
            let next_node = next_node.unwrap();
            let key_offset = next_node.key_offset;
            let key_size = next_node.key_size;
            let next_key = area_tmp.get_key(key_offset, key_size);
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
    pub fn find_near(
        &self,
        key: &[u8],
        less: bool,
        allow_equal: bool,
    ) -> (Option<Rc<&Node>>, bool) {
        let mut x = self.get_head().unwrap();
        let mut level = (self.get_height() - 1) as i32;
        let area_tmp = Rc::clone(&self.area);
        loop {
            // Assume x.key < key.
            let next = self.get_next(x.deref(), level);
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
            println!("next node:{:?}", next);
            let next_key = area_tmp.get_key(next.key_offset, next.key_size);
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
        let area_tmp = Rc::clone(&self.area);
        let (n, _) = self.find_near(key, false, true); // findGreaterOrEqual.
        if n.is_none() {
            return Value::default();
        }
        let n = n.unwrap();
        let next_key = area_tmp.get_key(n.key_offset, n.key_size);
        if !same_key(key, &next_key) {
            return Value::default();
        }

        let (val_offset, val_size) = n.get_value_offset();
        let vs = area_tmp.get_value(val_offset, val_size);
        vs
    }
}

impl SkipList {
    pub fn get_next(&self, nd: &Node, height: i32) -> Option<Rc<&Node>> {
        let offset = nd.get_next_offset(height);
        println!("next offset:{},height:{}", offset, height);
        self.area.get_node(offset)
    }

    pub fn get_head(&self) -> Option<Rc<&Node>> {
        self.area.get_node(self.head_offset)
    }

    pub fn get_height(&self) -> i32 {
        self.height.load(Relaxed)
    }

    pub fn get_value(&self, n: &Node) -> Value {
        let (val_offset, val_size) = n.get_value_offset();
        return self.area.get_value(val_offset, val_size);
    }
    pub fn iter(&self) -> SkipListIter {
        return iterator::new(self);
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

fn random_height() -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && random::<u32>() <= u32::MAX / 3 {
        h += 1;
    }
    h
}

#[cfg(test)]
mod tests {
    use crate::cache::entry::new_entry;
    use crate::cache::skiplist::new_skip_list;
    use rand::Rng;

    fn gen_key(len: usize) -> String {
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0; len];
        for i in 0..len {
            let b = rng.gen_range(97..123) as u8;
            bytes[i] = b;
        }
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn test_skip_list() {
        let mut list = new_skip_list(10000);
        let k1 = gen_key(10);
        let v1 = "111111";
        let entry1 = new_entry(k1.as_bytes(), v1.as_bytes());
        list.add(entry1);
        let value = list.search(k1.as_bytes());
        assert_eq!(*v1.as_bytes(), value.v);

        let k2 = gen_key(10);
        let v2 = "222222";
        let entry2 = new_entry(k2.as_bytes(), v2.as_bytes());
        list.add(entry2);
        let value = list.search(k1.as_bytes());

        assert_eq!(*v1.as_bytes(), value.v);

        list.search(gen_key(10).as_bytes());
        println!("{:?}", list.area.get_buf());
    }

    #[test]
    fn test_iterator() {
        let mut list = new_skip_list(10000);
        let k1 = gen_key(10);
        let v1 = "111111";
        let entry1 = new_entry(k1.as_bytes(), v1.as_bytes());
        list.add(entry1);

        let k2 = gen_key(10);
        let v2 = "222222";
        let entry1 = new_entry(k1.as_bytes(), v1.as_bytes());
        list.add(entry1);

        let k3 = gen_key(10);
        let v3 = "333333";
        let entry1 = new_entry(k1.as_bytes(), v1.as_bytes());
        list.add(entry1);

        for (i, e) in list.iter().enumerate() {
            match i {
                1 => assert_eq!(k1, String::from_utf8(e.key).unwrap()),
                2 => assert_eq!(k2, String::from_utf8(e.key).unwrap()),
                3 => assert_eq!(v3, String::from_utf8(e.value).unwrap()),
                x => assert_eq!(x, 0),
            }
        }
    }
}
