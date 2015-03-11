use std::io::{Read, Write, Result};
use std::path::{PathBuf, Path};
use std::thread::spawn;
use std::fs::{read_dir, PathExt};
use std::borrow::ToOwned;
use std::iter::IteratorExt;

use bzip2::{Compress};
use bzip2::reader::BzCompressor;

use Directory;
use super::error::{BonzoResult, BonzoError};
use super::database::Database;
use super::crypto;
use super::file_chunks::file_chunks;
use super::comm::spsc::bounded::{self, Producer, Consumer};
use super::iter_reduce::{Reduce, IteratorReduce};

// The number of messages that should be buffered for the export thread. A large 
// buffer will take up lots of memory and make will make the exporter do more
// unnecessary work when the receiver quits due to a time out. A small buffer
// increases the likelihood of buffer underruns, especially when a sequence of
// small files is being processed.
static CHANNEL_BUFFER_SIZE: usize = 10;

// Specification of messsages sent over the channel
pub enum FileInstruction {
    NewBlock(FileBlock),
    Complete(FileComplete),
    Error(BonzoError),
    Done
}

// Sent after the encryption and compression of a block is completed. It is the
// receiver's resposibility to write the bytes to disk and persist the details
// to the index
pub struct FileBlock {
    pub bytes: Vec<u8>,
    pub hash: String,
    pub source_byte_count: u64
}

#[derive(Debug)]
pub enum BlockReference {
    ById(u32),
    ByHash(String)
}

// This is sent *after* all the blocks of a file have been transferred. It is
// the receiver's responsibility to persist the file to the index.
#[derive(Debug)]
pub struct FileComplete {
    pub filename: String,
    pub hash: String,
    pub last_modified: u64,
    pub directory: Directory,
    pub block_reference_list: Vec<BlockReference>
}

// Manager which walks the file system and prepares files for backup. This
// entails splitting them into blocks and subsequently compressing and
// encrypting these blocks. Blocks which have not previously been encountered
// are transferred over a channel for the receiver to write to disk. This way,
// the processing and writing of blocks can be done in parallel.
pub struct ExportBlockSender<'sender> {
    database: Database,
    encryption_key: Box<[u8; 32]>,
    block_size: usize,
    sender: &'sender mut Producer<'static, FileInstruction>
}

impl<'sender> ExportBlockSender<'sender> {
    pub fn new(database: Database, encryption_key: Box<[u8; 32]>, block_size: usize, sender: &'sender mut Producer<'static, FileInstruction>) -> BonzoResult<ExportBlockSender<'sender>> {
        Ok(ExportBlockSender {
            database: database,
            encryption_key: encryption_key,
            block_size: block_size,
            sender: sender
        })
    }

    // Recursively walks the given directory, processing all files within.
    // Deletes references to deleted files which were previously found from the
    // database. Processes files in descending order of last mutation.
    pub fn export_directory(&mut self, path: &Path, directory: Directory) -> BonzoResult<()> {
        let content_list = try!(read_dir_sorted(path));
        let mut deleted_filenames = try!(self.database.get_directory_filenames(directory));
        
        for &(last_modified, ref content_path) in content_list.iter() {
            let filename = try!(
                content_path
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .ok_or(BonzoError::from_str("Could not convert filename to string"))
                    .map(String::from_str)
            );
            
            if content_path.is_dir() {
                let child_directory = try!(self.database.get_directory(directory, &filename));
            
                try!(self.export_directory(content_path, child_directory));
            }
            else {
                // FIXME: don't String::from_str if filename is dir
                if directory != Directory::Root || filename != super::DATABASE_FILENAME {
                    deleted_filenames.remove(&filename);
                    try!(self.export_file(directory, content_path, filename, last_modified));
                }
            }
        }

        deleted_filenames
            .iter()
            .map(|filename| {
                self.database
                    .persist_null_alias(directory, filename.as_slice())
                    .map_err(|e| BonzoError::Database(e))
            })
            .reduce()
    }

    // Tries to backup file. When the file was already in the database, it does
    // nothing. If the file contents were previously backed up, a new reference
    // is created. For unknown files, its (compressed and encrypted) blocks are
    // sent over the channel. When all blocks are transmitted, a FileComplete
    // message is sent, so the receiver can persist the file to the
    // database. 
    fn export_file(&mut self, directory: Directory, path: &Path, filename: String, last_modified: u64) -> BonzoResult<()> {        
        if try!(self.database.alias_known(directory, filename.as_slice(), last_modified)) {           
            return Ok(());
        }
        
        let hash = try!(crypto::hash_file(path));

        if let Some(file_id) = try!(self.database.file_from_hash(hash.as_slice())) {
            return Ok(try!(self.database.persist_alias(directory, Some(file_id), filename.as_slice(), Some(last_modified))));
        }
        
        let mut chunks = try!(file_chunks(path, self.block_size));
        let mut block_reference_list = Vec::new();

        // TODO: we can make this into a map, just have to implement it on chunks
        while let Some(slice) = chunks.next() {
            let unwrapped_slice = try!(slice);
            let block_reference = try!(self.export_block(unwrapped_slice));
            
            block_reference_list.push(block_reference);
        }
        
        try!(self.sender.send_sync(FileInstruction::Complete(FileComplete {
            filename: filename,
            hash: hash,
            last_modified: last_modified,
            directory: directory,
            block_reference_list: block_reference_list
        })).map_err(|_| BonzoError::from_str("Failed sending file")));

        Ok(())
    }

    // Returns the id of the block when its hash is already in the database.
    // Otherwise, it compresses and encrypts a block and sends the result on
    // the channel to be processed.
    pub fn export_block(&mut self, block: &[u8]) -> BonzoResult<BlockReference> {
        let hash = crypto::hash_block(block);

        if let Some(id) = try!(self.database.block_id_from_hash(hash.as_slice())) {
            return Ok(BlockReference::ById(id))
        }

        let processed_bytes = try!(process_block(block, &*self.encryption_key));

        try!(self.sender.send_sync(FileInstruction::NewBlock(FileBlock {
            bytes: processed_bytes,
            hash: hash.clone(),
            source_byte_count: block.len() as u64
        })).map_err(|_| BonzoError::from_str("Failed sending block")));

        Ok(BlockReference::ByHash(hash))
    }
}

fn read_dir_sorted(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {    
    let mut vec: Vec<(u64, PathBuf)> = try!(
        read_dir(dir)
        .and_then(|list| list
            .map(|possible_entry| {
                possible_entry.and_then(|entry| {
                    let path = entry.path();
                    
                    path.metadata()
                        .map(move |stats| {
                            (stats.modified(), path.to_owned())
                        })
                })
            })
            .collect()
        )
    );

    vec.sort_by(|&(a, _), &(b, _)| a.cmp(&b).reverse());

    Ok(vec)
}

pub fn process_block(clear_text: &[u8], key: &[u8; 32]) -> BonzoResult<Vec<u8>> {    
    let mut compressor = BzCompressor::new(clear_text, Compress::Best);
    let mut buffer = Vec::new();    
    try!(compressor.read_to_end(&mut buffer));
    Ok(try!(crypto::encrypt_block(buffer.as_slice(), key)))
}

// Starts a new thread in which the given source path is recursively walked
// and backed up. Returns a receiver to which new processed blocks and files
// will be sent.
pub fn start_export_thread(database: &Database, encryption_key: Box<[u8; 32]>, block_size: usize, source_path: PathBuf) -> Consumer<'static, FileInstruction> {
    let (mut transmitter, receiver) = bounded::new(CHANNEL_BUFFER_SIZE);
    let new_database = database.clone();

    spawn(move|| {
        let result = ExportBlockSender::new(new_database, encryption_key, block_size, &mut transmitter)
                                       .and_then(|mut exporter| {
                                           exporter.export_directory(&source_path, Directory::Root)
                                       });

        let instruction = match result {
            Err(e) => FileInstruction::Error(e),
            _      => FileInstruction::Done
        };

        let _ = transmitter.send_sync(instruction);
    });

    receiver
}

#[cfg(test)]
mod test {
    use std::old_io::Timer;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::super::tempdir::TempDir;
    use super::super::write_to_disk;

    #[test]
    fn read_dir() {
        let temp_dir = TempDir::new("readdir-test").unwrap();

        {
            let file_path = temp_dir.path().join("firstfile");
            write_to_disk(&file_path, b"test123").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = temp_dir.path().join("second");
            write_to_disk(&file_path, b"hello").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = temp_dir.path().join("third");
            write_to_disk(&file_path, b"waddaa").unwrap();
        }

        let list = super::read_dir_sorted(temp_dir.path()).unwrap();

        let filenames: Vec<String> = list
            .into_iter()
            .map(|(_, path)| {
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["third", "second", "firstfile"], &filenames);
    }
    
    #[test]
    fn channel_buffer() {
        let temp_dir = TempDir::new("buffer-test").unwrap();

        let file_count = 3 * super::CHANNEL_BUFFER_SIZE;

        for i in range(0, file_count) {
            let content = format!("file{}", i);
            let file_path = temp_dir.path().join(content.as_slice());

            write_to_disk(&file_path, content.as_bytes()).unwrap();
        }

        let password = "password123";
        let database_path = temp_dir.path().join(".backbonzo.db3");
        let key = super::super::crypto::derive_key(password);

        super::super::init(
            PathBuf::new(temp_dir.path()),
            PathBuf::new(temp_dir.path()),
            password
        ).unwrap();

        let database = super::super::database::Database::from_file(database_path).unwrap();
        let receiver = super::start_export_thread(&database, key, 10000000, PathBuf::new(temp_dir.path()));

        // give the export thread plenty of time to process all files
        Timer::new().unwrap().sleep(Duration::milliseconds(200));

        // we should receive two messages for each file: one for its block and
        // one for the file completion.
        // One file one when done
        let expected_message_count = 1 + 2 * file_count;

        let mut count = 0;

        while let Ok(msg) = receiver.recv_sync() {
            count += 1;
            
            match msg {
                super::FileInstruction::Done     => break,
                super::FileInstruction::Error(e) => panic!("{:?}", e),
                _ => {}
            }
        }

        assert_eq!(expected_message_count, count);
    }
}
