use std::io::Read;
use std::path::Path;
use std::thread::spawn;
use std::convert::From;
use std::borrow::ToOwned;

use bzip2::{Compress};
use bzip2::reader::BzCompressor;

use Directory;
use super::error::{BonzoResult, BonzoError};
use super::database::Database;
use super::crypto::{self, CryptoScheme};
use super::file_chunks::file_chunks;
use super::comm::mpsc::bounded_fast as mpsc;
use super::comm::spmc::bounded_fast as spmc;
use super::BlockId;

use self::filesystem_walker::{send_files, FileInfoMessage};

mod filesystem_walker;

// The number of messages that should be buffered for the export thread. A large 
// buffer will take up lots of memory and make will make the exporter do more
// unnecessary work when the receiver quits due to a time out. A small buffer
// increases the likelihood of buffer underruns, especially when a sequence of
// small files is being processed.
static CHANNEL_BUFFER_SIZE: usize = 16;
static EXPORT_THREAD_COUNT: usize = 4;

// Specification of messsages sent over the channel
pub enum FileInstruction {
    NewBlock(FileBlock),
    Complete(FileComplete),
    Error(BonzoError)
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
    ById(BlockId),
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
pub struct ExportBlockSender<'sender, C> where C: CryptoScheme {
    database: Database,
    crypto_scheme: Box<C>,
    block_size: usize,
    path_receiver: spmc::Consumer<'static, FileInfoMessage>,
    sender: &'sender mut mpsc::Producer<'static, FileInstruction>
}

impl<'sender, C: CryptoScheme> ExportBlockSender<'sender, C> {
    fn listen_for_paths(&self) -> BonzoResult<()> {
        while let Ok(msg) = self.path_receiver.recv_sync() {
            let info = try!(msg);
            
            try!(self.export_file(info.directory, &info.path, info.filename, info.modified));
        }
        
        Ok(())
    }
    
    // Tries to backup file. When the file was already in the database, it does
    // nothing. If the file contents were previously backed up, a new reference
    // is created. For unknown files, its (compressed and encrypted) blocks are
    // sent over the channel. When all blocks are transmitted, a FileComplete
    // message is sent, so the receiver can persist the file to the
    // database. 
    fn export_file(&self, directory: Directory, path: &Path, filename: String, last_modified: u64) -> BonzoResult<()> {        
        if try!(self.database.alias_known(directory, &filename, last_modified)) {           
            return Ok(());
        }
        
        let hash = try!(crypto::hash_file(path));

        if let Some(file_id) = try!(self.database.file_from_hash(&hash)) {
            return Ok(try!(self.database.persist_alias(directory, Some(file_id), &filename, Some(last_modified))));
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
    pub fn export_block(&self, block: &[u8]) -> BonzoResult<BlockReference> {
        let hash = crypto::hash_block(block);

        if let Some(id) = try!(self.database.block_id_from_hash(&hash)) {
            return Ok(BlockReference::ById(id))
        }

        let processed_bytes = try!(process_block(block, &*self.crypto_scheme));

        try!(self.sender.send_sync(FileInstruction::NewBlock(FileBlock {
            bytes: processed_bytes,
            hash: hash.clone(),
            source_byte_count: block.len() as u64
        })).map_err(|_| BonzoError::from_str("Failed sending block")));

        Ok(BlockReference::ByHash(hash))
    }
}

pub fn process_block<C: CryptoScheme>(clear_text: &[u8], crypto_scheme: &C) -> BonzoResult<Vec<u8>> {
    let mut compressor = BzCompressor::new(clear_text, Compress::Best);
    let mut buffer = Vec::new();
    try!(compressor.read_to_end(&mut buffer));

    crypto_scheme.encrypt_block(&buffer).map_err(From::from)
}

// Starts a new thread in which the given source path is recursively walked
// and backed up. Returns a receiver to which new processed blocks and files
// will be sent.
pub fn start_export_thread<C: CryptoScheme + 'static>(database: &Database, crypto_scheme: &C, block_size: usize, source_path: &Path) -> BonzoResult<mpsc::Consumer<'static, FileInstruction>> {
    let (block_transmitter, block_receiver) = unsafe { mpsc::new(CHANNEL_BUFFER_SIZE) };
    let (path_transmitter, path_receiver) = unsafe { spmc::new(CHANNEL_BUFFER_SIZE) };
    let sender_database = try!(database.try_clone());
    let path = source_path.to_owned();

    // spawn thread that sends file paths
    spawn(move || {
        send_files(&path, sender_database, path_transmitter);
    });

    // spawn encoder threads
    for _ in 0..EXPORT_THREAD_COUNT {
        let mut transmitter = block_transmitter.clone();
        let new_database = try!(database.try_clone());
        let receiver = path_receiver.clone();
        let scheme = Box::new(*crypto_scheme);

        spawn(move|| {
            let result = {
                let exporter = ExportBlockSender {
                    database: new_database,
                    crypto_scheme: scheme,
                    block_size: block_size,
                    path_receiver: receiver,
                    sender: &mut transmitter
                };
                
                exporter.listen_for_paths()
            };

            if let Err(e) = result {
                let _ = transmitter.send_sync(FileInstruction::Error(e));
            }
        });
    }

    Ok(block_receiver)
}

#[cfg(test)]
mod test {
    use std::thread::sleep_ms;

    use super::super::tempdir::TempDir;
    use super::super::write_to_disk;
    
    #[test]
    fn channel_buffer() {
        let temp_dir = TempDir::new("buffer-test").unwrap();

        let file_count = 3 * super::CHANNEL_BUFFER_SIZE;

        for i in 0..file_count {
            let content = format!("file{}", i);
            let file_path = temp_dir.path().join(&content);

            write_to_disk(&file_path, content.as_bytes()).unwrap();
        }

        let password = "password123";
        let database_path = temp_dir.path().join(".backbonzo.db3");
        let crypto_scheme = super::super::crypto::AesEncrypter::new(password);

        super::super::init(
            &temp_dir.path(),
            &temp_dir.path(),
            &crypto_scheme
        ).unwrap();

        let database = super::super::database::Database::from_file(database_path).unwrap();
        let receiver = super::start_export_thread(&database, &crypto_scheme, 10000000, temp_dir.path()).unwrap();

        // give the export thread plenty of time to process all files
        sleep_ms(200);

        // we should receive two messages for each file: one for its block and
        // one for the file completion.
        // One for each finished thread
        let expected_message_count = 2 * file_count;

        let mut count = 0;

        while let Ok(msg) = receiver.recv_sync() {
            count += 1;
            
            if let super::FileInstruction::Error(e) = msg {
                panic!("{:?}", e);
            }
        }

        assert_eq!(expected_message_count, count);
    }
}
