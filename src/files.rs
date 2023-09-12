use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, BufWriter, Read, Result, Write},
    path::Path,
};

use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};

struct PlainFileReader {
    reader: BufReader<File>,
}

impl PlainFileReader {
    pub fn new(filename: &str) -> Result<PlainFileReader> {
        let file = File::open(filename)?;
        let reader = BufReader::new(file);
        Ok(PlainFileReader { reader })
    }
}

impl Read for PlainFileReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf)
    }
}

struct GzipFileReader {
    reader: BufReader<MultiGzDecoder<File>>,
}

impl GzipFileReader {
    pub fn new(filename: &str) -> Result<GzipFileReader> {
        let file = File::open(filename)?;
        let decoder = MultiGzDecoder::new(file);
        let reader = BufReader::new(decoder);
        Ok(GzipFileReader { reader })
    }
}

impl Read for GzipFileReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf)
    }
}

pub fn open_reader(filename: &str) -> Result<Box<dyn Read>> {
    let path = Path::new(filename);
    if path.extension() == Some(OsStr::new("gz")) {
        let gzippped = GzipFileReader::new(filename)?;
        let boxed = Box::new(gzippped);
        Ok(boxed)
    } else {
        let plain = PlainFileReader::new(filename)?;
        let boxed = Box::new(plain);
        Ok(boxed)
    }
}

struct PlainFileWriter {
    writer: BufWriter<File>,
}

impl PlainFileWriter {
    pub fn new(filename: &str) -> Result<PlainFileWriter> {
        let file = File::create(filename)?;
        let writer = BufWriter::new(file);
        Ok(PlainFileWriter { writer })
    }
}

impl Write for PlainFileWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

struct GzipFileWriter {
    writer: GzEncoder<File>,
}

impl GzipFileWriter {
    pub fn new(filename: &str) -> Result<GzipFileWriter> {
        let file = File::create(filename)?;
        let writer = GzEncoder::new(file, Compression::default());
        Ok(GzipFileWriter { writer })
    }
}

impl Write for GzipFileWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

pub fn open_writer(filename: &str) -> Result<Box<dyn Write>> {
    let path = Path::new(filename);
    if path.extension() == Some(OsStr::new("gz")) {
        let gzippped = GzipFileWriter::new(filename)?;
        let boxed = Box::new(gzippped);
        Ok(boxed)
    } else {
        let plain = PlainFileWriter::new(filename)?;
        let boxed = Box::new(plain);
        Ok(boxed)
    }
}
