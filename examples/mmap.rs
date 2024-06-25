use memmap2::MmapOptions;
use std::fs::File;

fn main() -> Result<(), std::io::Error> {
    let file = File::open("README.md")?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    assert_eq!(b"# step-db", &mmap[0..9]);
    Ok(())
}
