use std::cell::{RefCell};
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32};
use std::sync::atomic::Ordering::{Relaxed};
use crate::cache::entry::Value;
use crate::cache::skiplist::{MAX_HEIGHT, Node};

const OFFSET_SIZE: usize = std::mem::size_of::<u32>();
const NODE_ALIGN: usize = std::mem::size_of::<u64>() - 1;
const MAX_NODE_SIZE: usize = std::mem::size_of::<Node>();

pub struct Area {
    n: AtomicU32,
    is_grow: bool,
    buf: RefCell<Vec<u8>>,
}

impl Area {
    pub(crate) fn new(n: u32) -> Area {
        Area {
            n: AtomicU32::new(1),
            is_grow: false,
            buf: RefCell::new(vec![0; n as usize]),
        }
    }

    pub(crate) fn get_buf(&self) -> std::cell::Ref<'_, Vec<u8>> {
        self.buf.borrow()
    }
    pub(crate) fn get_buf_mut(&self) -> std::cell::RefMut<'_, Vec<u8>> {
        self.buf.borrow_mut()
    }

    fn allocate(&self, sz: u32) -> u32 {
        let offset = self.n.fetch_add(sz, Relaxed);
        if !self.is_grow {
            assert!((offset + sz) <= self.get_buf().len() as u32);
            return offset;
        }
        // TODO： increase the capacity of buf
        return offset;
    }
    fn size(&self) -> i64 {
        return self.n.load(Relaxed) as i64;
    }

    pub(crate) fn put_node(&self, height: usize) -> u32 {
        let unused = (MAX_HEIGHT - height) * OFFSET_SIZE;
        let sz = (MAX_NODE_SIZE - unused + NODE_ALIGN) as u32;
        let offset = self.allocate(sz);
        return (offset + NODE_ALIGN as u32) & !(NODE_ALIGN as u32);
    }

    pub(crate) fn put_key(&self, key: Vec<u8>) -> u32 {
        let key_sz = key.len() as u32;
        let offset = self.allocate(key_sz);
        let end = (offset + key_sz) as usize;
        self.get_buf_mut()[offset as usize..end].copy_from_slice(&key);
        return offset;
    }

    pub(crate) fn put_value(&self, value: &Value) -> u32 {
        let encode_sz = value.encoded_size();
        let offset = self.allocate(encode_sz as u32) as usize;
        value.encode_value(&mut self.get_buf_mut()[offset..]);
        return offset as u32;
    }

    pub(crate) fn get_node_mut(&self, offset: u32) -> Option<Rc<&mut Node>> {
        if offset == 0 {
            return None;
        }
        let x = unsafe {
            mem::transmute::<&mut u8, &mut Node>(&mut self.get_buf_mut()[offset as usize])
        };

        return Some(Rc::new(x));
    }

    pub(crate) fn get_node(&self, offset: u32) -> Option<Rc<&Node>> {
        if offset == 0 {
            return None;
        }
        let x = unsafe {
            mem::transmute::<&u8, &Node>(&self.get_buf()[offset as usize])
        };
        println!("get_node node:{:?}", x);
        return Some(Rc::new(x));
    }

    pub(crate) fn get_key(&self, offset: u32, sz: u16) -> Vec<u8> {
        let offset = offset as usize;
        let end = offset + sz as usize;
        println!("offset:{},end:{}", offset, end);
        return self.get_buf()[offset..end].to_vec();
    }
    pub fn get_value(&self, offset: u32, sz: u32) -> Value {
        let end = (offset + sz) as usize;
        let mut ret = Value::default();
        ret.decode_value(&self.get_buf()[offset as usize..end]);
        return ret;
    }

    pub fn get_node_offset(&self, nd: &Node) -> u32 {
        let node_ptr = nd as *const Node as *const u8;
        let arena_start = self.get_buf().as_ptr();
        unsafe {
            node_ptr.offset_from(arena_start) as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::area::Area;
    use crate::cache::entry::Value;

    #[test]
    fn test_area() {
        let height = 20;
        let k = Vec::from("key_1");
        let v = Value {
            meta: 1,
            v: Vec::from("no step,no miles"),
            expires_at: 1234567890,
            version: 1,
        };
        let area = Area::new(1000);

        let node_offset = area.put_node(height);

        let key_offset = area.put_key(k.clone());
        let key_size = k.len();

        let value_offset = area.put_value(&v);
        let value_size = v.encoded_size();

        let node_target = area.get_node(node_offset).unwrap();
        let key_target = area.get_key(key_offset, key_size as u16);
        let value_target = area.get_value(value_offset, value_size as u32);
        assert_eq!(height, node_target.tower.len());
        assert_eq!(k, key_target);
        assert_eq!(v.v, value_target.v);
    }
}

