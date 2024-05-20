use std::mem;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use crate::cache::entry::{new_value, Value};
use crate::cache::skiplist::{MAX_HEIGHT, Node};

const OFFSET_SIZE: usize = std::mem::size_of::<u32>();
const NODE_ALIGN: usize = std::mem::size_of::<u64>() - 1;
const MAX_NODE_SIZE: usize = std::mem::size_of::<Node>();

struct Area {
    n: AtomicU32,
    is_grow: bool,
    buf: Vec<u8>,
}

fn new_area(n: u32) -> Area {
    Area {
        n: AtomicU32::new(1),
        is_grow: false,
        buf: vec![0; n as usize],
    }
}

impl Area {
    fn allocate(&self, sz: u32) -> u32 {
        let offset = self.n.fetch_add(sz, Relaxed);
        if !self.is_grow {
            assert!((offset + sz) <= self.buf.len() as u32);
            return offset;
        }
        // TODO： increase the capacity of buf
        return offset;
    }
    fn size(&self) -> i64 {
        return self.n.load(Relaxed) as i64;
    }

    fn put_node(&self, height: usize) -> u32 {
        let unused = (MAX_HEIGHT - height) * OFFSET_SIZE;
        let sz = (MAX_NODE_SIZE - unused + NODE_ALIGN) as u32;
        let offset = self.allocate(sz);
        return (offset + NODE_ALIGN as u32) & !(NODE_ALIGN as u32);
    }

    fn put_key(&mut self, key: Vec<u8>) -> u32 {
        let key_sz = key.len() as u32;
        let offset = self.allocate(key_sz);
        let end = (offset + key_sz) as usize;
        self.buf[offset as usize..end].copy_from_slice(&key);
        return offset;
    }

    fn put_value(&mut self, value: Value) -> u32 {
        let encode_sz = value.encoded_size();
        let offset = self.allocate(encode_sz as u32) as usize;
        value.encode_value(&mut self.buf[offset..]);
        return offset as u32;
    }

    fn get_node(&self, offset: u32) -> Option<*const Node> {
        if offset == 0 {
            return None;
        }
        return Some(unsafe {
            mem::transmute(&self.buf[offset as usize])
        });
    }

    fn get_key(&self, offset: u32, sz: u16) -> Vec<u8> {
        let offset = offset as usize;
        let end = offset + sz as usize;
        return self.buf[offset..end].to_vec();
    }
    fn get_value(&self, offset: u32, sz: u16) -> Value {
        let end = (offset + sz as u32) as usize;
        let mut ret = new_value();
        ret.decode_value(&self.buf[offset as usize..end]);
        return ret;
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::entry::Value;


    #[test]
    fn test_node() {

    }
}



