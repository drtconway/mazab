use std::collections::HashMap;

use indicatif::ProgressBar;
use noodles::sam::alignment::Record;

pub struct Remainder {
    pub tail: HashMap<String, Record>,
    pub flags: Vec<usize>
}

pub struct Pairer<Src>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    src: Src,
    cache: HashMap<String, Record>,
    flags: Vec<usize>,
    opt_prog: Option<ProgressBar>
}

impl<Src> Pairer<Src>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    pub fn new(src: Src, opt_prog: Option<ProgressBar>) -> Pairer<Src> {
        let mut flags = Vec::new();
        flags.resize(1 << 16, 0);

        Pairer {
            src,
            cache: HashMap::new(),
            flags,
            opt_prog
        }
    }

    pub fn remainder(&mut self) -> Remainder {
        assert!(self.src.next().is_none());
        let mut tail = HashMap::new();
        std::mem::swap(&mut self.cache, &mut tail);
        let mut flags = Vec::new();
        std::mem::swap(&mut self.flags, &mut flags);
        Remainder { tail, flags }
    }
}

impl<Src> Iterator for Pairer<Src>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    type Item = std::io::Result<(Record, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(rec_res) = self.src.next() {
            if let Some(prog) = &mut self.opt_prog {
                prog.inc(1);
            }
            match rec_res {
                Ok(rec) => {
                    self.flags[rec.flags().bits() as usize] += 1;
                    if rec.flags().is_supplementary()
                        || rec.flags().is_secondary()
                        || !rec.flags().is_segmented()
                    {
                        continue;
                    }
                    match rec.read_name() {
                        None => {
                            continue;
                        }
                        Some(nm) => {
                            let res = self.cache.remove(nm.as_ref() as &str);
                            match res {
                                None => {
                                    self.cache.insert(nm.to_string(), rec);
                                }
                                Some(other_rec) => {
                                    return Some(Ok((other_rec, rec)));
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let src_hint = self.src.size_hint();
        (src_hint.0, src_hint.1.map(|n| n + self.cache.len()))
    }
}
