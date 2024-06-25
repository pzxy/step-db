use crate::cache::entry::Entry;
use crate::cache::skiplist::{Node, SkipList};
use std::rc::Rc;

pub struct SkipListIter<'a> {
    l: &'a SkipList,
    n: Option<Rc<&'a Node>>,
    i: bool, // i == true, indicates not the first run
}

pub fn new(l: &SkipList) -> SkipListIter {
    SkipListIter {
        l,
        n: None,
        i: false,
    }
}

impl Iterator for SkipListIter<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.i {
            self.n = self.l.get_head();
            self.i = true;
            return self.item();
        }
        return match &self.n {
            None => None,
            Some(x) => {
                if let Some(next_n) = self.l.get_next(x, 0) {
                    self.n = Some(next_n);
                    self.item()
                } else {
                    self.n = None;
                    None
                }
            }
        };
    }
}

impl SkipListIter<'_> {
    fn valid(&self) -> bool {
        self.n.is_some()
    }

    fn item(&self) -> Option<Entry> {
        match &self.n {
            None => None,
            Some(n) => {
                let k = self.l.area.get_key(n.key_offset, n.key_size);
                let v = self.l.get_value(n);
                Option::from(Entry {
                    key: k,
                    value: v.v,
                    expires_at: v.expires_at,
                    meta: v.meta,
                    version: v.version,
                    ..Default::default()
                })
            }
        }
    }
}
