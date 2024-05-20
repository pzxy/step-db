pub const MAX_HEIGHT: usize = 20;

pub struct Node {
    value: u64,
    key_offset: u32,
    key_size: u16,
    height: u16,
    tower: [u32; MAX_HEIGHT],
}