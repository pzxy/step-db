use std::cmp::{max, min};
use std::f64::consts::{LN_2, PI};

#[derive(Debug)]
pub struct BloomFilter {
    bitmap: Vec<u8>,
    k: u8,
}

pub fn new(num_entries: isize, false_positive: f64) -> BloomFilter {
    return init_filter(num_entries, false_positive);
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
    // fn insert(&self, h: u32) -> bool {}
}

fn hash(bytes: &[u8]) -> u32 {
    murmurhash32::murmurhash3(bytes)
}

#[cfg(test)]
mod tests {
    use crate::cache::bloom::{bloom_bits, init_filter, new};

    #[test]
    fn test_hash() {
        let bf = new(1000, 0.01);
        println!("a {:?},k:{}", bf.bitmap.len(), bf.k)
    }
}