#![allow(unstable)]
#![feature(plugin)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate time;
extern crate bzip2;
extern crate glob;
extern crate "crypto" as rust_crypto;
extern crate spsc;
extern crate "iter-reduce" as iter_reduce;

#[cfg(test)]
extern crate regex;

use std::io::{IoError, IoResult, TempDir, BufReader};
use std::io::fs::{unlink, copy, File, mkdir_recursive};
use std::error::FromError;
use std::path::Path;
use std::cmp::Ordering;
use std::fmt;
use std::os::getcwd;

use bzip2::reader::BzDecompressor;
use glob::Pattern;
use rust_crypto::symmetriccipher::SymmetricCipherError;
use iter_reduce::{Reduce, IteratorReduce};
use time::get_time;
use rustc_serialize::hex::{ToHex, FromHex};

use export::{process_block, FileInstruction, BlockReference};
use database::{Database, SqliteError};
use summary::{RestorationSummary, BackupSummary};

mod database;
mod crypto;
mod export;
mod summary;
mod file_chunks;

static DATABASE_FILENAME: &'static str = "index.db3";

pub enum BonzoError {
    Database(SqliteError),
    Io(IoError),
    Crypto(SymmetricCipherError),
    Other(String)
}

impl FromError<IoError> for BonzoError {
    fn from_error(error: IoError) -> BonzoError {
        BonzoError::Io(error)
    }
}

impl FromError<SymmetricCipherError> for BonzoError {
    fn from_error(error: SymmetricCipherError) -> BonzoError {
        BonzoError::Crypto(error)
    }
}

impl FromError<SqliteError> for BonzoError {
    fn from_error(error: SqliteError) -> BonzoError {
        BonzoError::Database(error)
    }
}

impl fmt::Show for BonzoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BonzoError::Database(ref e) => write!(f, "Database error: {}", e.message),
            BonzoError::Io(ref e)       => write!(f, "IO error: {}, {}", e.desc, e.detail.clone().unwrap_or_default()),
            BonzoError::Crypto(..)      => write!(f, "Crypto error!"),
            BonzoError::Other(ref str)  => write!(f, "Error: {}", str)
        }
    }
}

pub type BonzoResult<T> = Result<T, BonzoError>;

#[derive(Copy, Eq, PartialEq, Show)]
enum Directory {
    Root,
    Child(i64)
}

pub struct BackupManager {
    database: Database,
    source_path: Path,
    backup_path: Path,
    encryption_key: Box<[u8; 32]>
}

impl BackupManager {
    pub fn new(database_path: Path, source_path: Path, password: &str, key: Box<[u8; 32]>) -> BonzoResult<BackupManager> {
        let database = try!(Database::from_file(database_path));
        
        let backup_path = try!(
            database.get_key("backup_path")
            .map_err(|error| BonzoError::Database(error))
            .and_then(|encoded| {
                encoded.ok_or(BonzoError::Other(format!("Could not find backup path in database")))
            })
            .and_then(|hex| {
                hex.from_hex().map_err(|_| BonzoError::Other(format!("Could not decode hex")))
            })
            .and_then(|byte_vector| {
                Path::new_opt(byte_vector).ok_or(BonzoError::Other(format!("Could not create path from byte vector")))
            })
        );
        
        let manager = BackupManager {
            database: database,
            source_path: source_path,
            backup_path: backup_path,
            encryption_key: key
        };

        try!(manager.check_password(password));

        Ok(manager)
    }

    // Update the state of the backup. Starts a walker thread and listens
    // to its messages. Exits after the time has surpassed the deadline, even
    // when the update hasn't been fully completed
    pub fn update(&mut self, block_bytes: u32, deadline: time::Tm) -> BonzoResult<BackupSummary> {
        let channel_receiver = export::start_export_thread(
            self.database.get_path(),
            self.encryption_key.clone(),
            block_bytes,
            self.source_path.clone()
        );
        
        let mut summary = BackupSummary::new();

        for msg in channel_receiver.iter() {
            match msg {
                FileInstruction::Done => break,
                FileInstruction::Error(e) => return Err(e),
                FileInstruction::NewBlock(block) => {
                    // make sure block has not already been persisted
                    if let Some(..) = try!(self.database.block_id_from_hash(block.hash.as_slice())) {
                        continue;
                    }
                    
                    let path = block_output_path(&self.backup_path, block.hash.as_slice());
                    let byte_slice = block.bytes.as_slice();

                    try!(mkdir_recursive(&path.dir_path(), std::io::FilePermission::all())
                        .and(write_to_disk(&path, byte_slice)));
        
                    try!(self.database.persist_block(block.hash.as_slice(), &*block.iv));

                    summary.add_block(byte_slice, block.source_byte_count);
                },
                FileInstruction::Complete(file) => {
                    let block_id_list = try!(file.block_reference_list
                        .iter()
                        .map(|reference| match *reference {
                            BlockReference::ById(id)         => Ok(id),
                            BlockReference::ByHash(ref hash) => {
                                let id_option = try!(self.database.block_id_from_hash(hash.as_slice()));
                                id_option.ok_or(BonzoError::Other(format!("Could not find block with hash {}", hash)))
                            }
                        })
                        .collect::<BonzoResult<Vec<u32>>>());

                    // only persist file to database if it's not already there
                    if let file_id@Some(..) = try!(self.database.file_from_hash(file.hash.as_slice())) {
                        try!(self.database.persist_alias(
                            file.directory,
                            file_id,
                            file.filename.as_slice(),
                            Some(file.last_modified)
                        ));
                    }
                    else {
                        try!(self.database.persist_file(
                            file.directory,
                            file.filename.as_slice(),
                            file.hash.as_slice(),
                            file.last_modified,
                            block_id_list.as_slice()
                        ));
                    }

                    summary.add_file();
                }
            }

            if deadline.cmp(&time::now_utc()) != Ordering::Greater {
                summary.timeout = true;                
                break;
            }
        }

        Ok(summary)
    }

    pub fn restore(&self, timestamp: u64, filter: String) -> BonzoResult<RestorationSummary> {
        let pattern = Pattern::new(filter.as_slice());
        let mut summary = RestorationSummary::new();

        try!(database::Aliases::new(
            &self.database,
            self.source_path.clone(),
            Directory::Root,
            timestamp
        ))
            .filter(|&(ref path, _)| pattern.matches_path(path))
            .map(|(ref path, ref block_list)| {
                self.restore_file(path, block_list.as_slice(), &mut summary)
            })
            .reduce()
            .and_then(move |_| Ok(summary))
    }

    // Restores a single file by decrypting and inflating a sequence of blocks
    // and writing them to the given path in order
    pub fn restore_file(&self, path: &Path, block_list: &[u32], summary: &mut RestorationSummary) -> BonzoResult<()> {
        try!(mkdir_recursive(&path.dir_path(), std::io::FilePermission::all()));
        
        let mut file = try!(File::create(path));

        for block_id in block_list.iter() {
            let (hash, iv) = try!(self.database.block_from_id(*block_id));
            let block_path = block_output_path(&self.backup_path, hash.as_slice());
            let bytes = try!(load_processed_block(&block_path, &*self.encryption_key, &*iv));
            let byte_slice = bytes.as_slice();

            summary.add_block(byte_slice);

            try!(file.write(byte_slice));
        }

        try!(file.fsync());

        summary.add_file();

        Ok(())
    }

    // Returns an error when the given password does not match the one saved
    // in the index
    fn check_password(&self, password: &str) -> BonzoResult<()> {
        let hash = try!(self.database.get_key("password"));
        let real_hash = try!(hash.ok_or(BonzoError::Other(format!("Saved hash is NULL"))));

        match crypto::check_password(password, real_hash.as_slice()) {
            true  => Ok(()),
            false => Err(BonzoError::Other(format!("Password is not the same as in database")))
        }
    }

    // Closes the database connection and saves it to the backup destination in
    // encrypted form
    fn export_index(self) -> BonzoResult<()> {
        let bytes = try!(self.database.to_bytes());
        let procesed_bytes = try!(process_block(bytes.as_slice(), &*self.encryption_key, &[0u8; 16]));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");
        
        try!(write_to_disk(&new_index, procesed_bytes.as_slice()));
        try!(copy(&new_index, &index));
        
        Ok(try!(unlink(&new_index)))
    }
}

pub fn init(source_path: Path, backup_path: Path, password: &str) -> BonzoResult<()> {
    let database_path = source_path.join(DATABASE_FILENAME);
    let database = try!(Database::create(database_path));
    let hash = try!(crypto::hash_password(password));

    try!(database.setup());
    try!(database.set_key("password", hash.as_slice()));

    let encoded_backup_path = try!(encode_path(&backup_path));
    
    try!(database.set_key("backup_path", encoded_backup_path.as_slice()));

    Ok(())
}

// Takes a path, turns it into an absolute path if necessary and hex encodes it
fn encode_path(path: &Path) -> BonzoResult<String> {
    if path.is_relative() {
        let absolute = try!(getcwd()).join(path);

        return Ok(absolute.as_vec().to_hex());
    }

    Ok(path.as_vec().to_hex())
}

pub fn backup(source_path: Path, block_bytes: u32, password: &str, deadline: time::Tm) -> BonzoResult<BackupSummary> {
    let database_path = source_path.join(DATABASE_FILENAME);
    let mut manager = try!(BackupManager::new(database_path, source_path, password, crypto::derive_key(password)));
    let summary = try!(manager.update(block_bytes, deadline));

    try!(manager.export_index());

    Ok(summary)
}

pub fn restore(source_path: Path, backup_path: Path, password: &str, timestamp: u64, filter: String) -> BonzoResult<RestorationSummary> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let key = crypto::derive_key(password);
    let decrypted_index_path = try!(decrypt_index(&backup_path, temp_directory.path(), &*key));
    let manager = try!(BackupManager::new(decrypted_index_path, source_path, password, key));
    
    manager.restore(timestamp, filter)
}

pub fn epoch_milliseconds() -> u64 {
    let stamp = get_time();
    
    stamp.nsec as u64 / 1000 / 1000 + stamp.sec as u64 * 1000
}

fn decrypt_index(backup_path: &Path, temp_dir: &Path, key: &[u8; 32]) -> BonzoResult<Path> {
    let decrypted_index_path = temp_dir.join(DATABASE_FILENAME);
    let bytes = try!(load_processed_block(&backup_path.join("index"), key, &[0u8; 16]));

    try!(write_to_disk(&decrypted_index_path, bytes.as_slice()));

    Ok(decrypted_index_path)
}

fn load_processed_block(path: &Path, key: &[u8; 32], iv: &[u8; 16]) -> BonzoResult<Vec<u8>> {
    let contents = try!(File::open(path).and_then(|mut file| file.read_to_end()));
    let decrypted_bytes = try!(crypto::decrypt_block(contents.as_slice(), key, iv));
    let mut decompressor = BzDecompressor::new(BufReader::new(decrypted_bytes.as_slice()));
    
    Ok(try!(decompressor.read_to_end()))
}

fn block_output_path(base_path: &Path, hash: &str) -> Path {
    base_path.join_many(&[hash.slice(0, 2), hash])
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> IoResult<()> {
    let mut file = try!(File::create(path));

    try!(file.write(bytes));
    file.fsync()
}

#[cfg(test)]
mod test {
    use std::io::{BufReader, TempDir};
    use std::io::fs::File;
    use std::rand::{Rng, OsRng};
    use super::bzip2::reader::{BzDecompressor, BzCompressor};
    use super::bzip2::CompressionLevel;

    #[test]
    fn process_reversability() {
        let dir = TempDir::new("reverse").unwrap();
        let bytes = "71d6e2f35502c03743f676449c503f487de29988".as_bytes();
        let file_path = dir.path().join("hash.txt");

        let mut key: [u8; 32] = [0; 32];
        let mut iv: [u8; 16] = [0; 16];
        let mut rng = OsRng::new().ok().unwrap();
        
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut iv);

        let processed_bytes = super::export::process_block(bytes, &key, &iv).unwrap();
        
        let mut file = File::create(&file_path).unwrap();
        assert!(file.write(processed_bytes.as_slice()).is_ok());
        assert!(file.fsync().is_ok());

        let retrieved_bytes = super::load_processed_block(&file_path, &key, &iv).unwrap();

        assert_eq!(bytes.as_slice(), retrieved_bytes.as_slice());
    }
    
    #[test]
    fn write_to_disk() {
        let temp_dir = TempDir::new("write-test").unwrap();
        let file_path = temp_dir.path().join("hello.txt");
        let message = "what's up?";

        let _ = super::write_to_disk(&file_path, message.as_bytes());

        let mut file = File::open(&file_path).unwrap();

        assert!(file.read_to_end().unwrap().as_slice() == message.as_bytes());
    }

    #[test]
    fn compression() {
        let mut rng = OsRng::new().ok().unwrap();
        let mut original: [u8; 10000] = [0; 10000];
        
        for _ in 0..10 {
            rng.fill_bytes(&mut original);
            let index = rng.gen::<u32>() % 10000;
            let slice = original.slice(0, index as usize);

            let mut compressor = BzCompressor::new(BufReader::new(slice), CompressionLevel::Smallest);
            let compressed_bytes = compressor.read_to_end().unwrap();
            
            let mut decompressor = BzDecompressor::new(BufReader::new(compressed_bytes.as_slice()));
                
            let decompresed_bytes = decompressor.read_to_end().unwrap();

            assert_eq!(slice, decompresed_bytes.as_slice());
        }
    }
}
