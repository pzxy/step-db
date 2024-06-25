use rand::RngCore;
use std::array::from_fn;
use std::cmp::min;

const CM_DEPTH: usize = 4;

// Count-Min Sketch
#[derive(Debug)]
pub struct CMSketch {
    rows: [CmRow; CM_DEPTH],
    seed: [u64; CM_DEPTH],
    mask: u64,
}

pub fn new(num_counters: u64) -> CMSketch {
    if num_counters == 0 {
        panic!("invalid num_counters");
    }

    // num_counters must be a power of 2
    let num_counters = next_power_of_two(num_counters);
    // mask must be 0111...111
    let mask = num_counters - 1;

    let mut rng = rand::thread_rng();
    // 0000,0000|0000,0000|0000,0000
    // 0000,0000|0000,0000|0000,0000
    // 0000,0000|0000,0000|0000,0000
    // 0000,0000|0000,0000|0000,0000
    CMSketch {
        rows: from_fn(|_| new_row(num_counters)),
        seed: from_fn(|_| rng.next_u64()),
        mask,
    }
}

impl CMSketch {
    pub fn increment(&mut self, hashed: u64) {
        for (i, row) in self.rows.iter_mut().enumerate() {
            row.increment((hashed ^ self.seed[i]) & self.mask);
        }
    }

    pub fn estimate(&self, hashed: u64) -> i64 {
        let mut m = 255;
        for (i, row) in self.rows.iter().enumerate() {
            m = min(m, row.get((hashed ^ self.seed[i]) & self.mask))
        }
        m as i64
    }

    pub fn reset(&mut self) {
        let _ = self.rows.iter_mut().map(|x| x.reset());
    }

    pub fn clear(&mut self) {
        let _ = self.rows.iter_mut().map(|x| x.clear());
    }
}

#[derive(Debug)]
pub struct CmRow {
    data: Vec<u8>,
}

pub fn new_row(num_counters: u64) -> CmRow {
    CmRow {
        data: vec![0; (num_counters / 2) as usize],
    }
}

impl CmRow {
    pub fn get(&self, n: u64) -> u8 {
        (self.data[n as usize / 2].wrapping_shr(((n & 1) * 4) as u32)) & 0x0f
    }

    pub fn increment(&mut self, n: u64) {
        let i = n as usize / 2;
        let s = (n & 1) * 4;
        let v = (self.data[i].wrapping_shr(s as u32)) & 0x0f;
        if v < 15 {
            self.data[i] += 1u64.wrapping_shl(s as u32) as u8;
        }
    }

    pub fn reset(&mut self) {
        for byte in &mut self.data.iter_mut() {
            *byte = (*byte >> 1) & 0x77;
        }
    }

    pub fn clear(&mut self) {
        self.data.fill(0);
    }
}

// impl fmt::Display for CmRow {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         let mut s = String::new();
//         for i in 0..(self.data.len() * 2) {
//             s += &format!("{:02} ", (self.data[i / 2].wrapping_shr(((i & 1) * 4) as u32) & 0x0f));
//         }
//         write!(f, "{}", s.trim_end())
//     }
// }

fn next_power_of_two(x: u64) -> u64 {
    let mut x = x - 1;
    x |= x.wrapping_shr(1);
    x |= x.wrapping_shr(2);
    x |= x.wrapping_shr(8);
    x |= x.wrapping_shr(16);
    x |= x.wrapping_shr(32);
    x + 1
}

#[cfg(test)]
mod tests {
    use crate::cache::counter;

    #[test]
    fn test_counter() {
        let mut c = counter::new(100);
        let h = 1234567890;
        c.increment(h);
        c.increment(h);
        c.increment(h);
        let v = c.estimate(h);
        assert_eq!(v, 3)
    }
}
