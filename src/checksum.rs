use std::collections::BinaryHeap;
use std::io::{stdout, BufReader, Write};

use noodles::fastq::{Reader, Writer};
use sha2::{digest::FixedOutput, Digest, Sha256};

use crate::files::open_reader;

fn hexy(xs: &[u8]) -> String {
    let s: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut res = String::new();
    for x in xs {
        res.push(s[(*x >> 4) as usize]);
        res.push(s[(*x & 0xf) as usize]);
    }
    res
}

#[derive(Debug, Eq, PartialEq)]
struct HashAndText {
    hash: String,
    text: Vec<u8>,
}

impl Ord for HashAndText {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialOrd for HashAndText {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn compute_checksum(filename_1: &str, filename_2: &str) -> std::io::Result<Vec<u8>> {
    let mut reader1 = open_reader(filename_1)
        .map(BufReader::new)
        .map(Reader::new)?;
    let mut reader2 = open_reader(filename_2)
        .map(BufReader::new)
        .map(Reader::new)?;

    let mut sketch: BinaryHeap<HashAndText> = BinaryHeap::new();

    let mut recs1 = reader1.records();
    let mut recs2 = reader2.records();
    let mut m = Vec::new();
    let mut rn = 0;
    loop {
        match (recs1.next(), recs2.next()) {
            (None, None) => {
                break;
            }
            (Some(lhs_res), None) => {
                println!(
                    "{} ran out of records before {} (hanging record is '{:?}'",
                    filename_2, filename_1, lhs_res
                );
                break;
            }
            (None, Some(rhs_res)) => {
                println!(
                    "{} ran out of records before {} (hanging record is '{:?}'",
                    filename_1, filename_2, rhs_res
                );
                break;
            }
            (Some(lhs_res), Some(rhs_res)) => {
                rn += 1;
                let lhs = lhs_res?;
                let rhs = rhs_res?;
                if lhs.name() != rhs.name() {
                    println!(
                        "read {}: mismatched record IDs: {} {}",
                        rn,
                        std::str::from_utf8(lhs.name()).unwrap(),
                        std::str::from_utf8(rhs.name()).unwrap()
                    );
                    break;
                }
                let mut w = Writer::new(Vec::new());
                w.write_record(&lhs)?;
                w.write_record(&rhs)?;
                let s = w.get_ref();
                let mut hasher: Sha256 = Sha256::new();
                hasher.update(s);
                let xs: Vec<u8> = Vec::from_iter(hasher.finalize_fixed());
                let h = hexy(&xs);
                let hat = HashAndText {
                    hash: h,
                    text: s.clone(),
                };
                sketch.push(hat);
                while sketch.len() > 1000 {
                    let _discard = sketch.pop();
                }
                //println!("{}", hexy(&xs));
                for i in 0..xs.len() {
                    while m.len() <= i {
                        m.push(0);
                    }
                    m[i] ^= xs[i];
                }
            }
        }
    }
    println!("number of read pairs: {}", rn);
    println!("{}", hexy(&m));
    println!("sketch:");
    while let Some(hat) = sketch.pop() {
        stdout().write_all(&hat.text)?;
    }
    Ok(m)
}
