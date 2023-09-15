use std::{
    fs::File,
    io::{Read, Write},
    sync::mpsc::{sync_channel, SyncSender},
    thread::JoinHandle,
};

use flate2::{bufread::GzEncoder, Compression};

pub struct DataBlock {
    id: String,
    data: Vec<u8>,
}

pub struct BlockPairWriter {
    compression: Option<Compression>,
    file: Option<SyncSender<(DataBlock, DataBlock)>>,
    joiner: Option<JoinHandle<std::io::Result<()>>>,
}

impl BlockPairWriter {
    pub fn new(
        filenames: (&str, &str),
        compression: Option<Compression>,
    ) -> std::io::Result<BlockPairWriter> {
        let inner_filename_0 = filenames.0.to_string();
        let inner_filename_1 = filenames.1.to_string();
        let (tx, rx) = sync_channel::<(DataBlock, DataBlock)>(1);
        let handle = std::thread::spawn(move || {
            let mut file_0 = File::create(&inner_filename_0)?;
            let mut file_1 = File::create(&inner_filename_1)?;
            for blocks in rx {
                //println!("writing {}", blocks.0.id);
                file_0.write_all(&blocks.0.data)?;
                //println!("writing {}", blocks.1.id);
                file_1.write_all(&blocks.1.data)?;
            }
            Ok(())
        });
        Ok(BlockPairWriter {
            compression,
            file: Some(tx),
            joiner: Some(handle),
        })
    }

    pub fn writers(&self, id: &str) -> std::io::Result<LocalBlockPairWriter> {
        Ok(LocalBlockPairWriter {
            compression: self.compression,
            id: id.to_string(),
            block_num: 0,
            writers: self.file.clone().unwrap(),
        })
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        self.file.take();
        match self.joiner.take() {
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "already joined",
            )),
            Some(joiner) => joiner
                .join()
                .map(|_| ())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "join failed")),
        }
    }
}

pub struct LocalBlockPairWriter {
    compression: Option<Compression>,
    id: String,
    block_num: usize,
    writers: SyncSender<(DataBlock, DataBlock)>,
}

impl LocalBlockPairWriter {
    pub fn write(&mut self, blocks: (&[u8], &[u8])) -> std::io::Result<()> {
        self.block_num += 1;
        let block_id_1 = format!("1\t{}:{}", self.id, self.block_num);
        let block_id_2 = format!("2\t{}:{}", self.id, self.block_num);
        let data_0 = if let Some(compression) = &self.compression {
            let mut result = Vec::with_capacity(blocks.0.len());
            let mut gz = GzEncoder::new(blocks.0, compression.clone());
            gz.read_to_end(&mut result).unwrap();
            result
        } else {
            Vec::from(blocks.0)
        };
        let data_1 = if let Some(compression) = &self.compression {
            let mut result = Vec::with_capacity(blocks.1.len());
            let mut gz = GzEncoder::new(blocks.1, compression.clone());
            gz.read_to_end(&mut result).unwrap();
            result
        } else {
            Vec::from(blocks.0)
        };
        self.writers
            .send((
                DataBlock {
                    id: block_id_1,
                    data: data_0,
                },
                DataBlock {
                    id: block_id_2,
                    data: data_1,
                },
            ))
            .unwrap();
        Ok(())
    }
}
