const MAX_VAR_INT_LEN64: usize = 10;

#[derive(Debug)]
pub struct Value {
    meta: u8,
    v: Vec<u8>,
    expires_at: u64,
    version: u64,
}

pub fn new_value() -> Value {
    Value {
        meta: 0,
        v: vec![],
        expires_at: 0,
        version: 0,
    }
}

impl Value {
    pub fn encoded_size(&self) -> usize {
        let sz = self.v.len() + 1; // meta
        let enc = size_varint(self.expires_at);
        sz + enc
    }

    pub fn decode_value(&mut self, buf: &[u8]) {
        self.meta = buf[0];
        let (expires_at, sz) = decode_uvarint(&buf[1..]);
        self.expires_at = expires_at;
        self.v = buf[1 + sz as usize..].to_vec();
    }

    pub fn encode_value(&self, b: &mut [u8]) -> u32 {
        b[0] = self.meta;
        let sz = encode_uvarint(&mut b[1..], self.expires_at);
        b[1 + sz as usize..].copy_from_slice(&self.v);
        return b.len() as u32;
    }
}

fn size_varint(x: u64) -> usize {
    let mut n = 0;
    let mut value = x;
    while value > 0 {
        n += 1;
        value >>= 7;
    }
    n
}


fn decode_uvarint(buf: &[u8]) -> (u64, isize) {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == MAX_VAR_INT_LEN64 {
            return (0, -(i as isize + 1)); // overflow
        }
        if b < 0x80 {
            if i == MAX_VAR_INT_LEN64 - 1 && b > 1 {
                return (0, -(i as isize + 1)); // overflow
            }
            return (x | ((b as u64) << s), (i + 1) as isize);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

fn encode_uvarint(buf: &mut [u8], x: u64) -> isize {
    let mut i = 0;
    let mut value = x;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    (i + 1) as isize
}


#[cfg(test)]
mod tests {
    use crate::cache::entry::Value;

    #[test]
    fn test_uvarint() {}

    #[test]
    fn test_value() {
        let v = Value { meta: 2, v: "我为祖国献石油".to_string().into_bytes(), expires_at: 123456, version: 1 };
        let mut data = vec![0u8; v.encoded_size()];
        let sz = v.encode_value(&mut data);
        let mut vv = Value { meta: 2, v: vec![], expires_at: 123456, version: 1 };
        let x = vv.decode_value(&mut data);
        assert_eq!(v.v, vv.v);
    }
}
