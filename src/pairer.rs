use std::collections::HashMap;

use indicatif::ProgressBar;
use noodles::sam::alignment::Record;

pub struct Pairer<Src>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    src: Src,
    cache: HashMap<String, Record>,
    opt_prog: Option<ProgressBar>
}

impl<Src> Pairer<Src>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    pub fn new(src: Src, opt_prog: Option<ProgressBar>) -> Pairer<Src> {
        Pairer {
            src,
            cache: HashMap::new(),
            opt_prog
        }
    }

    pub fn tail(&mut self) -> HashMap<String, Record> {
        assert!(self.src.next().is_none());
        let mut res = HashMap::new();
        std::mem::swap(&mut self.cache, &mut res);
        res
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
