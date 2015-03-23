#![feature(collections)]
#![feature(libc)]
#![feature(path_ext)]
#![feature(std_misc)]
#![feature(core)]
#![feature(plugin)]
#![feature(fs_time)]
#![feature(thread_sleep)]

#![plugin(regex_macros)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate time;
extern crate bzip2;
extern crate glob;
extern crate comm;
extern crate "iter-reduce" as iter_reduce;
extern crate rand;
extern crate tempdir;

#[cfg(test)]
extern crate regex;

use std::io::{self, Read, Write, BufReader};
use std::fs::{remove_file, copy, File, create_dir_all};
use std::path::{PathBuf, Path};
use std::ffi::AsOsStr;
use std::env::current_dir;
use std::error::FromError;

use tempdir::TempDir;
use bzip2::reader::BzDecompressor;
use glob::Pattern;
use iter_reduce::{Reduce, IteratorReduce};
use time::get_time;

use export::{process_block, FileInstruction, FileBlock, FileComplete, BlockReference};
use database::Database;
use summary::{RestorationSummary, BackupSummary};

pub use error::{BonzoError, BonzoResult};
pub use crypto::{CryptoScheme, AesEncrypter, hash_block};

mod database;
mod crypto;
mod export;
mod summary;
mod file_chunks;
mod error;

pub static DATABASE_FILENAME: &'static str = ".backbonzo.db3";
pub static MAX_ALIAS_AGE: u64 = 183 * 24 * 60 * 60 * 1000; // TODO: this should be a parameter

#[derive(Copy, Eq, PartialEq, Debug)]
pub enum Directory {
    Root,
    Child(i64)
}

#[derive(Copy, Eq, PartialEq, Debug)]
pub struct FileId(u64);

#[derive(Copy, Eq, PartialEq, Debug)]
pub struct BlockId(u64);

pub struct BackupManager<C> where C: CryptoScheme + 'static {
    database: Database,
    source_path: PathBuf,
    backup_path: PathBuf,
    crypto_scheme: Box<C>
}

impl<C: CryptoScheme + 'static> BackupManager<C> {
    pub fn new(database: Database, source_path: PathBuf, crypto_scheme: &C) -> BonzoResult<BackupManager<C>> {
        let backup_path = try!(
            database.get_key("backup_path")
                .map_err(|error| BonzoError::Database(error))
                .and_then(|encoded| {
                    encoded.ok_or(BonzoError::from_str("Could not find backup path in database"))
                })
                .map(|path_string| {
                    decode_path(&path_string)
                })
        );
        
        let manager = BackupManager {
            database: database,
            source_path: source_path,
            backup_path: backup_path,
            crypto_scheme: Box::new(*crypto_scheme)
        };

        try!(manager.check_password());

        Ok(manager)
    }

    // Update the state of the backup. Starts a walker thread and listens
    // to its messages. Exits after the time has surpassed the deadline, even
    // when the update hasn't been fully completed
    pub fn update(&mut self, block_bytes: usize, deadline: time::Tm) -> BonzoResult<BackupSummary> {
        let channel_receiver = try!(export::start_export_thread(
            &self.database,
            &*self.crypto_scheme,
            block_bytes,
            &self.source_path
        ));
        
        let mut summary = BackupSummary::new();

        while let Ok(msg) = channel_receiver.recv_sync() {
            if time::now_utc() > deadline {
                break;
            }            
            
            match msg {
                FileInstruction::Error(e)            => return Err(e),
                FileInstruction::NewBlock(ref block) => try!(self.handle_new_block(block, &mut summary)),
                FileInstruction::Complete(ref file)  => try!(self.handle_new_file (file,  &mut summary))
            }
        }

        Ok(summary)
    }

    pub fn restore(&self, timestamp: u64, filter: String) -> BonzoResult<RestorationSummary> {
        let pattern = try!(Pattern::new(filter.as_slice()).map_err(|_| BonzoError::from_str("Invalid glob pattern")));
        let mut summary = RestorationSummary::new();

        try!(database::Aliases::new(
            &self.database,
            self.source_path.clone(),
            Directory::Root,
            timestamp
        ))
            .filter(|alias| match alias {
                &Err(..)           => true,
                &Ok((ref path, _)) => pattern.matches_path(path)
            })
            .map(|alias| {
                alias
                    .map_err(FromError::from_error)
                    .and_then(|(ref path, ref block_list)| {
                        self.restore_file(path, &block_list, &mut summary)
                    })
            })
            .reduce()
            .and_then(move |_| Ok(summary))
    }

    // Restores a single file by decrypting and inflating a sequence of blocks
    // and writing them to the given path in order
    pub fn restore_file(&self, path: &Path, block_list: &[BlockId], summary: &mut RestorationSummary) -> BonzoResult<()> {
        try!(create_parent_dir(path));
        
        let mut file = try!(File::create(path));

        for block_id in block_list.iter() {
            let hash = try!(self.database.block_hash_from_id(*block_id));
            let block_path = block_output_path(&self.backup_path, hash.as_slice());
            let bytes = try!(load_processed_block(&block_path, &*self.crypto_scheme));

            if hash_block(&bytes) != hash {
                return Err(BonzoError::from_str("Block integrity check failed"));
            }

            summary.add_block(&bytes);

            try!(file.write_all(&bytes));
        }

        try!(file.sync_all());

        summary.add_file();

        Ok(())
    }

    fn handle_new_block(&self, block: &FileBlock, summary: &mut BackupSummary) -> BonzoResult<()> {
        // make sure block has not already been persisted
        if let Some(..) = try!(self.database.block_id_from_hash(block.hash.as_slice())) {
            return Ok(());
        }
        
        let path = block_output_path(&self.backup_path, block.hash.as_slice());
        let byte_slice = block.bytes.as_slice();

        try!(create_parent_dir(&path));
        try!(write_to_disk(&path, byte_slice));
        try!(self.database.persist_block(block.hash.as_slice()));

        summary.add_block(byte_slice, block.source_byte_count);

        Ok(())
    }

    fn handle_new_file(&self, file: &FileComplete, summary: &mut BackupSummary) -> BonzoResult<()> {
        // if file hash was already known, only add a new alias
        if let file_id@Some(..) = try!(self.database.file_from_hash(file.hash.as_slice())) {
            try!(self.database.persist_alias(
                file.directory,
                file_id,
                &file.filename,
                Some(file.last_modified)
            ));

            return Ok(summary.add_file());
        }
        
        let block_id_list: Vec<_> = try!(
            file.block_reference_list
            .iter()
            .map(|reference| match *reference {
                BlockReference::ById(id)         => Ok(id),
                BlockReference::ByHash(ref hash) => {
                    let id_option = try!(self.database.block_id_from_hash(hash.as_slice()));
                    id_option.ok_or(BonzoError::Other(format!("Could not find block with hash {}", hash)))
                }
            })
            .collect()
        );
        
        try!(self.database.persist_file(
            file.directory,
            &file.filename,
            &file.hash,
            file.last_modified,
            &block_id_list
        ));

        summary.add_file();

        Ok(())
    }

    // Returns an error when the given password does not match the one saved
    // in the index
    fn check_password(&self) -> BonzoResult<()> {
        let hash_opt = try!(self.database.get_key("password"));
        let hash = try!(hash_opt.ok_or(BonzoError::from_str("Saved hash is NULL")));

        match self.crypto_scheme.hash_password() == hash {
            true  => Ok(()),
            false => Err(BonzoError::from_str("Password is not the same as in database"))
        }
    }

    // Remove old aliases and unused blocks from database and disk
    fn cleanup(&self, max_age_milliseconds: u64) -> BonzoResult<()> {
        let now = epoch_milliseconds();

        let timestamp = match now < max_age_milliseconds {
            true  => 0,
            false => now - max_age_milliseconds
        };
        
        try!(self.database.remove_old_aliases(timestamp));
        
        try!(self.database.remove_unused_files());
        
        self.clean_unused_blocks()
    }

    fn clean_unused_blocks(&self) -> BonzoResult<()> {
        let unused_block_list = try!(self.database.get_unused_blocks());

        for (id, hash) in unused_block_list {
            let path = block_output_path(&self.backup_path, &hash);

            try!(remove_file(&path));
            try!(self.database.remove_block(id));
        }

        Ok(())
    }

    // Closes the database connection and saves it to the backup destination in
    // encrypted form
    fn export_index(self) -> BonzoResult<()> {
        let bytes = try!(self.database.to_bytes());
        let procesed_bytes = try!(process_block(&bytes, &*self.crypto_scheme));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");
        
        try!(write_to_disk(&new_index, &procesed_bytes));
        try!(copy(&new_index, &index));
        
        Ok(try!(remove_file(&new_index)))
    }
}

// TODO: move this to main.rs
pub fn init<C: CryptoScheme>(source_path: PathBuf, backup_path: PathBuf, crypto_scheme: &C) -> BonzoResult<()> {
    let database_path = source_path.join(DATABASE_FILENAME);
    let database = try!(Database::create(database_path));
    let hash = crypto_scheme.hash_password();

    try!(database.setup());
    try!(database.set_key("password", &hash));

    let encoded_backup_path = try!(encode_path(&backup_path));
    
    try!(database.set_key("backup_path", &encoded_backup_path));

    Ok(())
}

fn create_parent_dir(path: &Path) -> BonzoResult<()> {
    let parent = try!(path.parent().ok_or(BonzoError::from_str("Couldn't get parent directory")));

    Ok(try!(create_dir_all(parent)))
}

// Takes a path, turns it into an absolute path if necessary
fn encode_path(path: &Path) -> io::Result<String> {
    if path.is_relative() {
        let mut cwd = try!(current_dir());
        cwd.push(path);
        
        return Ok(cwd.to_string_lossy().into_owned())
    }

    Ok(path.to_string_lossy().into_owned())
}

fn decode_path(path: &AsOsStr) -> PathBuf {
    PathBuf::new(path)
}

pub fn backup<C: CryptoScheme + 'static>(source_path: PathBuf, block_bytes: usize, crypto_scheme: &C, deadline: time::Tm) -> BonzoResult<BackupSummary> {
    let database_path = source_path.join(DATABASE_FILENAME);
    let database = try!(Database::from_file(database_path));
    let mut manager = try!(BackupManager::new(database, source_path, crypto_scheme));
    let summary = try!(manager.update(block_bytes, deadline));

    try!(manager.cleanup(MAX_ALIAS_AGE));
    try!(manager.export_index());

    Ok(summary)
}

pub fn restore<C: CryptoScheme + 'static>(source_path: PathBuf, backup_path: PathBuf, crypto_scheme: &C, timestamp: u64, filter: String) -> BonzoResult<RestorationSummary> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let decrypted_index_path = try!(decrypt_index(&backup_path, temp_directory.path(), crypto_scheme));
    let database = try!(Database::from_file(decrypted_index_path));
    let manager = try!(BackupManager::new(database, source_path, crypto_scheme));
    
    manager.restore(timestamp, filter)
}

pub fn epoch_milliseconds() -> u64 {
    let stamp = get_time();
    
    stamp.nsec as u64 / 1000 / 1000 + stamp.sec as u64 * 1000
}

fn decrypt_index<C: CryptoScheme>(backup_path: &Path, temp_dir: &Path, crypto_scheme: &C) -> BonzoResult<PathBuf> {
    let decrypted_index_path = temp_dir.join(DATABASE_FILENAME);
    let bytes = try!(load_processed_block(&backup_path.join("index"), crypto_scheme));

    try!(write_to_disk(&decrypted_index_path, bytes.as_slice()));

    Ok(decrypted_index_path)
}

fn load_processed_block<C: CryptoScheme>(path: &Path, crypto_scheme: &C) -> BonzoResult<Vec<u8>> {
    let contents: Vec<u8> = try!(
        File::open(path).and_then(|mut file| {
            let mut buffer = Vec::new();
            try!(file.read_to_end(&mut buffer));
            Ok(buffer)
        })
    );
    
    let decrypted_bytes = try!(crypto_scheme.decrypt_block(&contents));
    let mut decompressor = BzDecompressor::new(BufReader::new(&decrypted_bytes[..]));
    
    let mut buffer = Vec::new();
    try!(decompressor.read_to_end(&mut buffer));
    Ok(buffer)
}

fn block_output_path(base_path: &Path, hash: &str) -> PathBuf {
    let mut path = base_path.join(&hash[0..2]);

    path.push(hash);

    path
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = try!(File::create(path));

    try!(file.write_all(bytes));
    file.sync_all()
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write, BufReader};
    use std::fs::{create_dir_all, File, copy};
    use std::time::duration::Duration;
    use std::path::PathBuf;

    use super::tempdir::TempDir;
    use super::rand::{Rng, OsRng};
    use super::bzip2::reader::{BzDecompressor, BzCompressor};
    use super::bzip2::Compress;
    use super::crypto::hash_file;
    use super::{write_to_disk, block_output_path, init, backup, restore, epoch_milliseconds, BonzoError};
    use super::time;
    
    // It can happen that a block is (partially) written, but not persisted to database
    // Therefore, backbonzo will retry to write this block. this should not err
    #[test]
    fn overwrite_block() {
        let bytes = b"71d6e2f35502c03743f676449c503f487de29988";

        let source_dir = TempDir::new("overwrite-source").unwrap();
        let dest_dir = TempDir::new("overwrite-dest").unwrap();
        let in_path = source_dir.path().join("whatev");
        
        write_to_disk(&in_path, bytes).ok().expect("write input");
        
        let hash = hash_file(&in_path).ok().expect("compute hash");
        let out_path = block_output_path(dest_dir.path(), &hash);

        create_dir_all(&out_path.parent().unwrap()).ok().expect("created dir");

        match write_to_disk(&out_path, b"sup") {
            Ok(..) => {},
            Err(e) => panic!("{:?}", e.to_string())
        }

        let deadline = time::now() + Duration::seconds(30);
        let crypto_scheme = super::crypto::AesEncrypter::new("passwerd");

        init(PathBuf::new(source_dir.path()), PathBuf::new(dest_dir.path()), &crypto_scheme).ok().expect("init ok");
        backup(PathBuf::new(source_dir.path()), 1_000_000, &crypto_scheme, deadline).ok().expect("backup successful");
    }

    // Checks that the hash of the restored data is as expected
    #[test]
    fn integrity() {
        let file_one_content = b"71d6e2f35502c03743f676449c503f487de29988";
        let file_two_content = b"i sure hope this works, yo!";

        let source_dir = TempDir::new("integ-source").unwrap();
        let dest_dir = TempDir::new("integ-dest").unwrap();
        let file_one_path = source_dir.path().join("file-one");
        let file_two_path = source_dir.path().join("file-two");
        
        write_to_disk(&file_one_path, file_one_content).ok().expect("write input file one ");
        write_to_disk(&file_two_path, file_two_content).ok().expect("write input file two");

        let deadline = time::now() + Duration::seconds(30);
        let crypto_scheme = super::crypto::AesEncrypter::new("passwerd");

        init(PathBuf::new(source_dir.path()), PathBuf::new(dest_dir.path()), &crypto_scheme).ok().expect("init ok");
        backup(PathBuf::new(source_dir.path()), 1_000_000, &crypto_scheme, deadline).ok().expect("backup successful");
        
        let file_one_hash = hash_file(&file_one_path).ok().expect("compute hash");
        let file_two_hash = hash_file(&file_two_path).ok().expect("compute hash");
        let file_one_out_path = block_output_path(dest_dir.path(), &file_one_hash);
        let file_two_out_path = block_output_path(dest_dir.path(), &file_two_hash);

        copy(file_one_out_path, file_two_out_path).ok().expect("copy files");

        let restore_dir = TempDir::new("integ-restore").unwrap();
        let result = restore(
            PathBuf::new(restore_dir.path()),
            PathBuf::new(dest_dir.path()),
            &crypto_scheme,
            epoch_milliseconds(),
            "**".to_string()
        );

        let is_expected = match result {
            Err(BonzoError::Other(ref str)) => str.as_slice() == "Block integrity check failed",
            _                               => false
        };

        assert!(is_expected);
    }

    #[test]
    fn process_reversability() {
        let dir = TempDir::new("reverse").unwrap();
        let bytes = "71d6e2f35502c03743f676449c503f487de29988".as_bytes();
        let file_path = dir.path().join("hash.txt");
        let crypto_scheme = super::crypto::AesEncrypter::new("test1234");

        let processed_bytes = super::export::process_block(bytes, &crypto_scheme).unwrap();
        
        let mut file = File::create(&file_path).unwrap();
        assert!(file.write_all(processed_bytes.as_slice()).is_ok());
        assert!(file.sync_all().is_ok());

        let retrieved_bytes = super::load_processed_block(&file_path, &crypto_scheme).unwrap();

        assert_eq!(bytes.as_slice(), retrieved_bytes.as_slice());
    }
    
    #[test]
    fn write_file() {
        let temp_dir = TempDir::new("write-test").unwrap();
        let file_path = temp_dir.path().join("hello.txt");
        let message = "what's up?";

        let _ = write_to_disk(&file_path, message.as_bytes());

        let mut file = File::open(&file_path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();

        assert!(buffer.as_slice() == message.as_bytes());
    }

    #[test]
    fn compression() {
        let mut rng = OsRng::new().ok().unwrap();
        let mut original: [u8; 10000] = [0; 10000];
        
        for _ in 0..10 {
            rng.fill_bytes(&mut original);
            let index = rng.gen::<u32>() % 10000;
            let slice = &original[0..index as usize];

            let mut compressor = BzCompressor::new(slice, Compress::Best);
            let mut compressed_bytes = Vec::new();
            compressor.read_to_end(&mut compressed_bytes).unwrap();
            
            let mut decompressor = BzDecompressor::new(BufReader::new(compressed_bytes.as_slice()));
            let mut decompressed_bytes = Vec::new();
            decompressor.read_to_end(&mut decompressed_bytes).unwrap();

            assert_eq!(slice, decompressed_bytes.as_slice());
        }
    }
}
