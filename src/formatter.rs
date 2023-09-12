use std::io::Write;

use noodles::sam::{alignment::Record, record::sequence::Base};

use crate::block_writer::LocalBlockPairWriter;

pub struct ReadParFormatter {
    buffers: (Vec<u8>, Vec<u8>),
    writers: LocalBlockPairWriter,
}

impl ReadParFormatter {
    pub fn new(writers: LocalBlockPairWriter) -> ReadParFormatter {
        ReadParFormatter {
            buffers: (Vec::new(), Vec::new()),
            writers,
        }
    }

    pub fn write(&mut self, pair: (Record, Record)) -> std::io::Result<()> {
        assert!(pair.0.flags().is_first_segment() || pair.0.flags().is_last_segment());
        assert!(pair.1.flags().is_first_segment() || pair.1.flags().is_last_segment());
        if pair.0.flags().is_first_segment() != !pair.1.flags().is_first_segment() {
            println!("eek! {} {}", pair.0.flags().bits(), pair.1.flags().bits());
        }
        assert_eq!(
            pair.0.flags().is_first_segment(),
            !pair.1.flags().is_first_segment()
        );
        assert_eq!(
            pair.0.flags().is_last_segment(),
            !pair.1.flags().is_last_segment()
        );

        let (mut r1, mut r2) = if pair.0.flags().is_first_segment() {
            pair
        } else {
            (pair.1, pair.0)
        };

        if r1.flags().is_reverse_complemented() {
            reverse_complement(&mut r1);
        }

        if r2.flags().is_reverse_complemented() {
            reverse_complement(&mut r2);
        }

        let read_id: &str = r1.read_name().unwrap().as_ref();

        writeln!(&mut self.buffers.0, "@{}", read_id)?;
        writeln!(&mut self.buffers.0, "{}", r1.sequence())?;
        writeln!(&mut self.buffers.0, "+")?;
        writeln!(&mut self.buffers.0, "{}", r1.quality_scores())?;

        writeln!(&mut self.buffers.1, "@{}", read_id)?;
        writeln!(&mut self.buffers.1, "{}", r2.sequence())?;
        writeln!(&mut self.buffers.1, "+")?;
        writeln!(&mut self.buffers.1, "{}", r2.quality_scores())?;

        if self.buffers.0.len() + self.buffers.1.len() > 16 * 1024 * 1024 {
            self.writers.write((&self.buffers.0, &self.buffers.1))?;
            self.buffers.0.clear();
            self.buffers.1.clear();
        }

        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.buffers.0.len() + self.buffers.1.len() > 0 {
            self.writers.write((&self.buffers.0, &self.buffers.1))?;
            self.buffers.0.clear();
            self.buffers.1.clear();
        }

        Ok(())
    }
}

fn reverse_complement(rec: &mut Record) {
    fn complement(base: Base) -> Base {
        match base {
            Base::Eq => Base::Eq,
            Base::A => Base::T,
            Base::C => Base::G,
            Base::M => Base::K,
            Base::G => Base::C,
            Base::R => Base::Y,
            Base::S => Base::S,
            Base::V => Base::B,
            Base::T => Base::A,
            Base::W => Base::W,
            Base::Y => Base::R,
            Base::H => Base::D,
            Base::K => Base::M,
            Base::D => Base::H,
            Base::B => Base::V,
            _ => Base::N,
        }
    }

    let seq = rec.sequence_mut();
    seq.as_mut().reverse();
    for base in seq.as_mut() {
        *base = complement(*base);
    }

    let qual = rec.quality_scores_mut();
    qual.as_mut().reverse();
}
