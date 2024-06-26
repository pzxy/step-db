use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::File;

fn mmap(fd: &File, size: usize) -> anyhow::Result<Mmap> {
    unsafe { Ok(MmapOptions::new().len(size).map(fd)?) }
}

fn mmap_mut(fd: &File, size: usize) -> anyhow::Result<MmapMut> {
    unsafe { Ok(MmapOptions::new().len(size).map_mut(fd)?) }
}
