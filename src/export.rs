use std::io::{IoResult, BufReader};
use std::io::fs::{readdir, File, PathExtensions};
use std::path::Path;
use std::rand::{Rng, OsRng};
use std::thread::Thread;
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use std::iter::repeat;

use bzip2::CompressionLevel;
use bzip2::reader::BzCompressor;

use super::database::Database;
use super::crypto;
use super::{BonzoResult, BonzoError};

pub enum FileInstruction {
    NewBlock(FileBlock),
    Complete(FileComplete),
    Error(BonzoError)
}

struct FileBlock {
    pub bytes: Vec<u8>,
    pub iv: Vec<u8>,
    pub hash: String
}

// known blocks are represented by Some(id), and new blocks are represented by
// None as we don't known the id in the thread that does the encryption
struct FileComplete {
    pub filename: String,
    pub hash: String,
    pub last_modified: u64,
    pub directory_id: u32,
    pub block_id_list: Vec<Option<u32>>
}

pub struct Blocks<'a> {
    file: File,
    buffer: Vec<u8>
}

impl<'a> Blocks<'a> {
    pub fn from_path(path: &Path, block_size: u32) -> IoResult<Blocks> {
        Ok(Blocks {
            file: try!(File::open(path)),
            buffer: repeat(0).take(block_size as uint).collect()
        })
    }
    
    pub fn next(&'a mut self) -> Option<&'a [u8]> {
        match self.file.read(self.buffer.as_mut_slice()) {
            Err(..)   => None,
            Ok(bytes) => Some(self.buffer.slice(0, bytes))
        }

        //self.file.read(self.buffer.as_mut_slice()).map(|bytes| {
        //    self.buffer.slice(0, bytes)
        //}).ok()
    }
}

pub struct ExportBlockSender {
    database: Database,
    encryption_key: Vec<u8>,
    block_size: u32,
    sender: SyncSender<FileInstruction>
}

impl ExportBlockSender {
    // FIXME: this method is senseless
    pub fn new(database: Database, encryption_key: Vec<u8>, block_size: u32, sender: SyncSender<FileInstruction>) -> ExportBlockSender {
        ExportBlockSender {
            database: database,
            encryption_key: encryption_key,
            block_size: block_size,
            sender: sender
        }
    }

    pub fn export_directory(&self, path: &Path, directory_id: u32) -> BonzoResult<()> {
        let mut content_list: Vec<(u64, Path)> = try!(readdir(path)
            .and_then(|list| list.into_iter()
                .map(|path| match path.stat() {
                    Ok(stats) => Ok((stats.modified, path)),
                    Err(e)    => Err(e)
                })
                .collect()
            ));

        content_list.sort_by(|&(a, _), &(b, _)| a.cmp(&b).reverse());

        let mut deleted_filenames = try!(self.database.get_directory_filenames(directory_id));
        
        for &(last_modified, ref content_path) in content_list.iter() {
            if content_path.is_dir() {
                let relative_path = try!(content_path.path_relative_from(path).ok_or(BonzoError::Other(format!("Could not get relative path"))));
                let name = try!(relative_path.as_str().ok_or(BonzoError::Other(format!("Cannot express directory name in UTF8"))));
                let child_directory_id = try!(self.database.get_directory(directory_id, name));
            
                try!(self.export_directory(content_path, child_directory_id));
            }
            else {
                try!(content_path
                    .filename_str()
                    .ok_or(BonzoError::Other(format!("Could not convert filename to string")))
                    .map(String::from_str)
                    .and_then(|filename| {
                        deleted_filenames.remove(&filename);
                        self.export_file(directory_id, content_path, filename, last_modified)
                    }));
            }
        }

        deleted_filenames.iter().map(|filename|
            self.database.persist_null_alias(directory_id, filename.as_slice())
        ).fold(Ok(()), |a, b| a.and(b))
    }

    fn export_file(&self, directory_id: u32, path: &Path, filename: String, last_modified: u64) -> BonzoResult<()> {
        if self.database.alias_known(directory_id, filename.as_slice(), last_modified) {           
            return Ok(());
        }
        
        let hash = try!(crypto::hash_file(path));

        if let Some(file_id) = self.database.file_from_hash(hash.as_slice()) {
            return Ok(try!(self.database.persist_alias(directory_id, Some(file_id), filename.as_slice(), last_modified)));
        }
        
        let mut blocks = try!(Blocks::from_path(path, self.block_size));
        let mut block_id_list = Vec::new();
        
        while let Some(slice) = blocks.next() {
            block_id_list.push(try!(self.export_block(slice)));
        }
        
        let _ = self.sender.try_send(FileInstruction::Complete(FileComplete {
            filename: filename,
            hash: hash,
            last_modified: last_modified,
            directory_id: directory_id,
            block_id_list: block_id_list
        }));

        Ok(())
    }

    pub fn export_block(&self, block: &[u8]) -> BonzoResult<Option<u32>> {
        let hash = crypto::hash_block(block);

        if let Some(id) = self.database.block_id_from_hash(hash.as_slice()) {
            return Ok(Some(id))
        }

        /* TODO: we could replace the vector in FileBlock by a 16 byte array */
        let mut iv = repeat(0).take(16).collect::<Vec<u8>>();
        let mut rng = try!(OsRng::new()); // FIXME: make one rng at struct creation and recycle?

        rng.fill_bytes(iv.as_mut_slice());

        let mut compressor = BzCompressor::new(BufReader::new(block), CompressionLevel::Smallest);
        let compressed_bytes = try!(compressor.read_to_end());

        let _ = self.sender.try_send(FileInstruction::NewBlock(FileBlock {
            bytes: try!(crypto::encrypt_block(compressed_bytes.as_slice(), self.encryption_key.as_slice(), iv.as_slice())),
            iv: iv,
            hash: hash
        }));

        Ok(None)
    }
}

pub fn start_export_thread(database_path: &Path, encryption_key: Vec<u8>, block_size: u32, source_path: Path) -> Receiver<FileInstruction> {
    // FIXME: make this literal a constant
    let (tx, rx) = sync_channel::<FileInstruction>(5);
    let path = database_path.clone();

    Thread::spawn(move|| {
        let result = match Database::from_file(path) {
            Err(e) => Err(e),
            Ok(database) => {
                ExportBlockSender::new(database, encryption_key, block_size, tx.clone())
                    .export_directory(&source_path, 0)
            }
        };
    
        if let Err(e) = result {
            let _ = tx.send(FileInstruction::Error(e));
        }
    }).detach();

    rx

    // FIXME: are we sure all messages sent at this point will be seen by receiver?
}
