use std::cmp::{max, min};
use std::f64::consts::LN_2;

#[derive(Debug)]
pub struct BloomFilter {
    bitmap: Vec<u8>,
    k: u8,
}

pub fn new(num_entries: isize, false_positive: f64) -> BloomFilter {
    init_filter(num_entries, false_positive)
}

// m = -n(lnP)/(ln2)^2
// m == Bits number of bitmap
// n == The total number of keys that can be remark when P is satisfied
// P == False positive,
fn bloom_bits(n: isize, fp: f64) -> f64 {
    -(n as f64 * fp.ln()) / LN_2.powi(2)
}

pub fn init_filter(num_entries: isize, false_positive: f64) -> BloomFilter {
    let mut bf = BloomFilter {
        bitmap: Vec::new(),
        k: 0,
    };
    let bits = bloom_bits(num_entries, false_positive);
    let bits_per_key = max(0, (bits / num_entries as f64).ceil() as isize);

    // k = (m/n)*ln2
    // k == Number of hash times/functions
    let k = (bits_per_key as f64 * LN_2) as u8;
    bf.k = max(1, min(k, 30));

    let bits = max(64, bits_per_key * num_entries) as usize;
    let bytes = (bits + 7) / 8;
    bf.bitmap = vec![0; bytes + 1];

    bf.bitmap[bytes] = k;
    bf
}

impl BloomFilter {
    fn insert(&mut self, h: u32) -> bool {
        if self.k > 30 {
            return true;
        }
        let bits = 8 * (self.bitmap.len() - 1) as u32;
        let delta = (h >> 17) | (h << 15);
        let mut h = h;
        for _ in 0..self.k {
            let bit_pos = h % bits;
            self.bitmap[(bit_pos / 8) as usize] |= 1u32.wrapping_shl(bit_pos % 8) as u8;
            h = h.wrapping_add(delta)
        }
        true
    }
    fn may_exist_key(&self, k: &[u8]) -> bool {
        self.may_exist(hash(k))
    }

    fn may_exist(&self, h: u32) -> bool {
        if self.bitmap.len() < 2 {
            return false;
        }
        let bits = 8 * (self.bitmap.len() - 1) as u32;
        let delta = (h >> 17) | (h << 15);
        let mut h = h;
        for _ in 0..self.k {
            let bit_pos = h % bits;
            // println!("bit_pos:{}", bit_pos);
            if self.bitmap[(bit_pos / 8) as usize] as u32 & (1u32.wrapping_shl(bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta)
        }
        true
    }
    fn allow_key(&mut self, k: &[u8]) -> bool {
        self.allow(hash(k))
    }
    pub(crate) fn allow(&mut self, h: u32) -> bool {
        let already = self.may_exist(h);
        if !already {
            return self.insert(h);
        }
        already
    }
    pub fn reset(&mut self) {
        for v in self.bitmap.iter_mut() {
            *v = 0;
        }
    }
}

fn hash(bytes: &[u8]) -> u32 {
    murmurhash32::murmurhash3(bytes)
}

#[cfg(test)]
mod tests {
    use crate::cache::bloom::new;

    #[test]
    fn test_bloom() {
        let mut bf = new(1000, 0.01);
        let k1 = "大西洋海底来的人".as_bytes();
        let k2 = "加里森敢死队".as_bytes();
        let k3 = "狗安偷生".as_bytes();
        bf.allow_key(k1);
        bf.allow_key(k2);

        let exist1 = bf.may_exist_key(k1);
        let exist2 = bf.may_exist_key(k2);
        let exist3 = bf.may_exist_key(k3);
        assert!(exist1);
        assert!(exist2);
        assert!(!exist3);
    }
}
