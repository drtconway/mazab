use std::{
    io::Write,
    sync::{mpsc::{sync_channel, SyncSender}, Mutex},
    thread::JoinHandle,
};

use crate::files::open_writer;

pub struct BlockWriter {
    file: Option<SyncSender<Vec<u8>>>,
    joiner: Option<JoinHandle<std::io::Result<()>>>,
}

impl BlockWriter {
    pub fn new(filename: &str) -> BlockWriter {
        let filename = filename.to_string();
        let (tx, rx) = sync_channel::<Vec<u8>>(4);
        let handle = std::thread::spawn(move || {
            let mut file = open_writer(&filename)?;
            for data in rx {
                file.write_all(&data)?;
            }
            Ok(())
        });
        BlockWriter {
            file: Some(tx),
            joiner: Some(handle),
        }
    }

    pub fn writer(&self) -> std::io::Result<LocalBlockWriter> {
        match &self.file {
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "dead channel",
            )),
            Some(file) => Ok(LocalBlockWriter::new(file.clone())),
        }
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        self.file.take();
        match self.joiner.take() {
            None => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "already joined",
                ))
            }
            Some(joiner) => {
                joiner.join().map(|_| ()).map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "join failed"))
            }
        }
    }
}

pub struct LocalBlockWriter {
    file: SyncSender<Vec<u8>>,
}

impl LocalBlockWriter {
    pub fn new(file: SyncSender<Vec<u8>>) -> LocalBlockWriter {
        LocalBlockWriter { file }
    }
}

impl Write for LocalBlockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let data = Vec::from(buf);
        self.file
            .send(data)
            .map(|_| buf.len())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "send failed"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BlockPairWriter {
    block_writers: (BlockWriter, BlockWriter)
}

impl BlockPairWriter {
    pub fn new(filenames: (&str, &str)) -> std::io::Result<BlockPairWriter> {
        let w1 = BlockWriter::new(filenames.0);
        let w2 = BlockWriter::new(filenames.1);
        Ok(BlockPairWriter{ block_writers: (w1, w2) })
    }

    pub fn writers(&self) -> std::io::Result<LocalBlockPairWriter> {
        let w1 = self.block_writers.0.writer()?;
        let w2 = self.block_writers.1.writer()?;
        Ok(LocalBlockPairWriter { writers: Mutex::new((w1, w2)) })
    }
}

pub struct LocalBlockPairWriter {
    writers: Mutex<(LocalBlockWriter, LocalBlockWriter)>
}

impl LocalBlockPairWriter {
    pub fn write(&mut self, blocks: (&[u8], &[u8])) -> std::io::Result<()> {
        let mut writers = self.writers.lock().unwrap();
        writers.0.write_all(blocks.0)?;
        writers.1.write_all(blocks.1)?;
        Ok(())
    }
}