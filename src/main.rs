use docopt::Docopt;
use flate2::Compression;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mazab::block_writer::{BlockPairWriter, LocalBlockPairWriter};
use mazab::pairer::Remainder;
use mazab::summarise::Summariser;
use mazab::{
    checksum::compute_checksum, files::open_writer, formatter::ReadParFormatter, pairer::Pairer,
    shuffler::Shuffler,
};
use noodles::core::Region;
use noodles::{bam, sam::alignment::Record};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

const USAGE: &'static str = "
Usage: mazab [options] <bam> <fastq1> <fastq2>
       mazab -X [options] <fastq1> <fastq2>

Options:
    -h                      Show this help message.
    -v                      Produce verbose output.
    -C COMPRESSION          Level of gzip compression (0-9, none, fast, default, best) [default: default]
    -t THREADS              Number of additional threads to used [default: 4]
    -U                      Write the read IDs of unpaired reads to stdout.
    -X                      Compute an order-independent digest on the reads.
";

pub fn make_compression(txt: &str) -> std::io::Result<Compression> {
    match txt {
        "fast" => Ok(Compression::fast()),
        "default" => Ok(Compression::default()),
        "best" => Ok(Compression::best()),
        _ => {
            let n = txt.parse::<u32>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "invalid compression specifier")
            })?;
            if n <= 9 {
                Ok(Compression::new(n))
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "compression level must be 0-9",
                ))
            }
        }
    }
}

pub fn gather_chromosome_info(bam: &str) -> std::io::Result<(Vec<String>, Vec<usize>, Vec<usize>)> {
    let mut reader = bam::indexed_reader::Builder::default().build_from_path(bam)?;
    let header = reader.read_header()?;
    let mut chrom_names: Vec<String> = Vec::new();
    let mut chrom_lengths: Vec<usize> = Vec::new();
    let mut chrom_record_count = Vec::new();

    for item in header.reference_sequences().iter() {
        let chrom_name = item.0.to_string();
        let chrom_length = item.1.length().get();
        //if chrom_length < 4 * 1024 * 1024 {
        //    // ignore anything < 4Mb
        //    continue;
        //}
        //if chrom_name.contains("_") {
        //    // skip all the alts.
        //    continue;
        //}
        chrom_names.push(chrom_name);
        chrom_lengths.push(chrom_length);
    }
    for ref_info in reader.index().reference_sequences().iter() {
        let m = ref_info.metadata();
        if let Some(d) = m {
            chrom_record_count.push((d.mapped_record_count() + d.unmapped_record_count()) as usize);
        } else {
            chrom_record_count.push(0);
        }
    }

    chrom_names.push("*".to_string());
    chrom_lengths.push(0);
    chrom_record_count.push(
        if let Some(n) = reader.index().unplaced_unmapped_record_count() {
            n as usize
        } else {
            0
        },
    );

    if chrom_names.len() != chrom_lengths.len() || chrom_names.len() != chrom_record_count.len() {
        println!("{:?}", chrom_names.len());
        println!("{:?}", chrom_lengths.len());
        println!("{:?}", chrom_record_count.len());
        panic!();
    }
    Ok((chrom_names, chrom_lengths, chrom_record_count))
}

pub fn chromosome_ranges(bam: &str) -> std::io::Result<Vec<String>> {
    let (chrom_names, chrom_lengths, _chrom_record_count) = gather_chromosome_info(bam)?;
    println!("{}", chrom_names.len());
    let mut res = Vec::new();
    res.push("*".to_string());
    for i in 0..chrom_names.len() {
        res.push(format!("{}:{}-{}", chrom_names[i], 1, chrom_lengths[i]));
    }
    //println!("chunks={:?}", res);
    Ok(res)
}

fn sum(xs: &[usize]) -> u64 {
    let mut total: u64 = 0;
    for x in xs {
        total += (*x) as u64;
    }
    total
}

pub fn open_writers(
    filename_1: &str,
    filename_2: &str,
) -> std::io::Result<Arc<Mutex<(Box<dyn Write>, Box<dyn Write>)>>> {
    let w1 = open_writer(filename_1)?;
    let w2 = open_writer(filename_2)?;
    Ok(Arc::new(Mutex::new((w1, w2))))
}

pub fn make_ok(rec: Record) -> std::io::Result<Record> {
    Ok(rec)
}

pub fn make_chan() -> (
    Option<Sender<(usize, Remainder)>>,
    Receiver<(usize, Remainder)>,
) {
    let (tx, rx) = channel();
    (Some(tx), rx)
}

fn doit2_inner_inner<Src>(
    query: Src,
    opt_prog: Option<ProgressBar>,
    writers: LocalBlockPairWriter,
) -> std::io::Result<Remainder>
where
    Src: Iterator<Item = std::io::Result<Record>>,
{
    let pairer = Pairer::new(query, opt_prog);
    let mut shuffler = Shuffler::new(65536, 19, pairer);
    let mut formatter = ReadParFormatter::new(writers);
    while let Some(res_pair) = shuffler.next() {
        let pair = res_pair?;
        formatter.write(pair)?;
    }
    formatter.flush()?;

    Ok(shuffler.src.remainder())
}

fn doit2_inner(
    bam: &str,
    chrom_name: &str,
    opt_prog: Option<ProgressBar>,
    writers: LocalBlockPairWriter,
) -> std::io::Result<Remainder> {
    let mut reader = bam::indexed_reader::Builder::default().build_from_path(bam)?;
    let hdr = reader.read_header()?;

    if chrom_name == "*" {
        let unmapped = reader.query_unmapped(&hdr)?;
        return doit2_inner_inner(unmapped, opt_prog, writers);
    }

    let query: bam::reader::Query<std::fs::File> =
        reader.query(&hdr, &Region::new(chrom_name, ..))?;
    doit2_inner_inner(query, opt_prog, writers)
}

pub fn doit2(
    bam: &str,
    filename_1: &str,
    filename_2: &str,
    verbose: bool,
    num_threads: usize,
    compression: Option<Compression>,
    write_unpaired_reads: bool,
) -> std::io::Result<()> {
    let target = ProgressDrawTarget::stderr_with_hz(1);
    let multi = MultiProgress::with_draw_target(target);
    let sty = ProgressStyle::with_template(
        "{prefix} [{elapsed_precise}] [{wide_bar}] {percent}% ({pos}/{len})",
    )
    .unwrap();

    let writers: BlockPairWriter = BlockPairWriter::new((filename_1, filename_2), compression)?;

    let pool = ThreadPool::new(num_threads);

    let chrom_info = gather_chromosome_info(bam)?;

    let total_record_count = sum(&chrom_info.2);
    let opt_glob_prog = if verbose {
        let prog = multi.add(ProgressBar::new(1));
        prog.set_style(sty.clone());
        prog.set_prefix("progress");
        prog.set_length(total_record_count);
        prog.set_position(0);
        Some(prog)
    } else {
        None
    };

    let (mut opt_tx, rx) = make_chan();

    let mut todo = 0;
    for chrom_num in 0..chrom_info.0.len() {
        let chrom_name = chrom_info.0[chrom_num].to_string();
        let _chrom_len = chrom_info.1[chrom_num];
        let chrom_count = chrom_info.2[chrom_num];
        if chrom_count == 0 {
            continue;
        }

        todo += 1;

        let tx = opt_tx.as_ref().unwrap().clone();
        let opt_prog = if verbose && chrom_count > 1000 {
            let prog = multi.add(ProgressBar::new(1));
            prog.set_style(sty.clone());
            prog.set_prefix(chrom_name.to_string());
            prog.set_length(chrom_count as u64);
            prog.set_position(0);
            Some(prog)
        } else {
            None
        };
        let bam_name = bam.to_string();
        let writers: LocalBlockPairWriter = writers.writers(&chrom_name)?;
        pool.execute(move || {
            let remainder =
                doit2_inner(&bam_name, &chrom_name, opt_prog, writers).expect("doit2_inner failed");
            tx.send((chrom_num, remainder)).expect("send failed");
        });
    }
    opt_tx.take();

    let mut flags = Vec::new();
    flags.resize(1 << 16, 0);

    let mut remainder_stats = Summariser::new();

    let mut unpaired = vec![];
    for (chrom_num, remainder) in rx {
        for i in 0..remainder.flags.len() {
            flags[i] += remainder.flags[i];
        }
        remainder_stats.add(remainder.tail.len() as f64);

        unpaired.push(remainder);
        if let Some(glob_prog) = &opt_glob_prog {
            glob_prog.inc(chrom_info.2[chrom_num] as u64);
        }
        todo -= 1;
    }
    assert_eq!(todo, 0);
    pool.join();

    let unpaired_iterator = unpaired
        .into_iter()
        .flat_map(|x| x.tail.into_values())
        .map(make_ok);
    let writers: LocalBlockPairWriter = writers.writers("<>")?;
    let final_remainder = doit2_inner_inner(unpaired_iterator, None, writers)?;

    println!("flags: bits\tcount\tPAIRED\tPROPER\tUNMAP\tMUNMAP\tREVERSE\tMREVERSE\tREAD1\tREAD2\tSECONDARY\tQCFAIL\tDUP\tSUPPLEMENTARY");
    for i in 0..flags.len() {
        if flags[i] == 0 {
            continue;
        }
        println!(
            "flags: {}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            i,
            flags[i],
            i & 1,
            (i >> 1) & 1,
            (i >> 2) & 1,
            (i >> 3) & 1,
            (i >> 4) & 1,
            (i >> 5) & 1,
            (i >> 6) & 1,
            (i >> 7) & 1,
            (i >> 8) & 1,
            (i >> 9) & 1,
            (i >> 10) & 1,
            (i >> 11) & 1,
        );
    }
    println!("unpaired: {}", final_remainder.tail.len());
    if write_unpaired_reads {
        for e in final_remainder.tail.iter() {
            println!("read_id: {}", e.0);
        }
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    let args = Docopt::new(USAGE)
        .and_then(|dopt| dopt.parse())
        .unwrap_or_else(|e| e.exit());
    //println!("{:?}", args);

    let verbose = args.get_bool("-v");

    if args.get_bool("-X") {
        let _sum1 = compute_checksum(args.get_str("<fastq1>"), args.get_str("<fastq2>"))?;
        return Ok(());
    }

    let num_threads = args
        .get_str("-t")
        .parse::<usize>()
        .expect("-t must be an integer");

    let compression = if args.get_str("-C") != "" && args.get_str("-C") != "none" {
        let res = make_compression(args.get_str("-C"))?;
        Some(res)
    } else {
        None
    };

    doit2(
        args.get_str("<bam>"),
        args.get_str("<fastq1>"),
        args.get_str("<fastq2>"),
        verbose,
        num_threads,
        compression,
        args.get_bool("-U"),
    )?;

    Ok(())
}
