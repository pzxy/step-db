// CompareKeys checks the key without timestamp and checks the timestamp if keyNoTs
// is same.
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp.
pub fn compare_keys(key1: &[u8], key2: &[u8]) -> i32 {
    assert!(
        key1.len() > 8 && key2.len() > 8,
        "key1: {}, key2: {} < 8",
        String::from_utf8_lossy(key1),
        String::from_utf8_lossy(key2)
    );
    let cmp = key1[..key1.len() - 8].cmp(&key2[..key2.len() - 8]);
    if !cmp.is_eq() {
        return cmp as i32;
    }
    key1[key1.len() - 8..].cmp(&key2[key2.len() - 8..]) as i32
}
