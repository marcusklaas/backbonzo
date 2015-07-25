#![feature(libc, path_ext, plugin, duration, into_cow, vec_push_all, fs_time)]

extern crate rustc_serialize;
extern crate time;
extern crate bzip2;
extern crate glob;
extern crate comm;
extern crate iter_reduce;
extern crate rand;
extern crate tempdir;

#[cfg(test)]
extern crate regex;

use std::io::{self, Read, Write, BufReader};
use std::fs::{remove_file, copy, File, create_dir_all, set_file_times, metadata, PathExt};
use std::path::{PathBuf, Path};
use std::env::current_dir;
use std::convert::{From, AsRef};
use std::borrow::IntoCow;

use tempdir::TempDir;
use bzip2::reader::BzDecompressor;
use glob::Pattern;
use iter_reduce::{Reduce, IteratorReduce};
use time::get_time;

use export::{process_block, FileInstruction, FileBlock, FileComplete, BlockReference};
use database::Database;
use summary::{RestorationSummary, BackupSummary, InitSummary, CleanupSummary};

pub use error::{BonzoError, BonzoResult};
pub use crypto::{CryptoScheme, AesEncrypter, hash_block};

#[macro_use]
mod error;
mod database;
mod crypto;
mod export;
mod summary;
mod file_chunks;

// TODO: Move this constant to main.rs 
pub static DATABASE_FILENAME: &'static str = ".backbonzo.db3";

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Directory {
    Root,
    Child(i64)
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct FileId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct BlockId(u64);

pub struct BackupManager<C> where C: CryptoScheme {
    database: Database,
    source_path: PathBuf,
    backup_path: PathBuf,
    crypto_scheme: Box<C>
}

impl<C: CryptoScheme> BackupManager<C> {
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
                summary.timeout = true;
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
        let pattern = try!(Pattern::new(&filter).map_err(|_| BonzoError::from_str("Invalid glob pattern")));
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
                    .map_err(From::from)
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

        let mut file = try_io!(File::create(path), path);

        for block_id in block_list.iter() {
            let hash = try!(self.database.block_hash_from_id(*block_id));
            let block_path = block_output_path(&self.backup_path, &hash);
            let bytes = try!(load_processed_block(&block_path, &*self.crypto_scheme));

            if hash_block(&bytes) != hash {
                //return Err(BonzoError::from_str("Block integrity check failed"));
                println!("block integrity check failed for path: {:?}", path);
            }

            summary.add_block(&bytes);

            try_io!(file.write_all(&bytes), path);
        }

        try_io!(file.sync_all(), path);

        summary.add_file();

        Ok(())
    }

    fn handle_new_block(&self, block: &FileBlock, summary: &mut BackupSummary) -> BonzoResult<()> {
        // make sure block has not already been persisted
        if let Some(..) = try!(self.database.block_id_from_hash(&block.hash)) {
            return Ok(());
        }

        let path = block_output_path(&self.backup_path, &block.hash);
        let byte_slice = &block.bytes;

        try!(create_parent_dir(&path));
        try!(write_to_disk(&path, byte_slice));
        try!(self.database.persist_block(&block.hash));

        summary.add_block(byte_slice, block.source_byte_count);

        Ok(())
    }

    fn handle_new_file(&self, file: &FileComplete, summary: &mut BackupSummary) -> BonzoResult<()> {
        // if file hash was already known, only add a new alias
        if let file_id@Some(..) = try!(self.database.file_from_hash(&file.hash)) {
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
                    let id_option = try!(self.database.block_id_from_hash(&hash));
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
    fn cleanup(&self, max_age_milliseconds: u64) -> BonzoResult<CleanupSummary> {
        let now = epoch_milliseconds();

        let timestamp = match now < max_age_milliseconds {
            true  => 0,
            false => now - max_age_milliseconds
        };

        let aliases = try!(self.database.remove_old_aliases(timestamp));
        try!(self.database.remove_unused_files());
        let (blocks, bytes) = try!(self.clean_unused_blocks());

        Ok(CleanupSummary { aliases: aliases,
                            blocks: blocks,
                            bytes: bytes, })
    }

    // Returns the number of unused blocks and the total number of bytes within.
    fn clean_unused_blocks(&self) -> BonzoResult<(u64, u64)> {
        let unused_block_list = try!(self.database.get_unused_blocks());
        let block_count = unused_block_list.len();
        let mut bytes = 0;

        for (id, hash) in unused_block_list {
            let path = block_output_path(&self.backup_path, &hash);

            // Do not err when the file was already removed. We may need to
            // revisit this decision later as it is indicative of potential
            // issues.
            if !path.exists() {
                continue;
            }

            bytes += try_io!(metadata(&path), &path).len();
            try_io!(remove_file(&path), &path);
            try!(self.database.remove_block(id));
        }

        Ok((block_count as u64, bytes))
    }

    // Closes the database connection and saves it to the backup destination in
    // encrypted form
    fn export_index(self) -> BonzoResult<()> {
        let bytes = try!(self.database.to_bytes());
        let procesed_bytes = try!(process_block(&bytes, &*self.crypto_scheme));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");

        try_io!(write_to_disk(&new_index, &procesed_bytes), &new_index);
        try_io!(copy(&new_index, &index), &new_index);

        Ok(try_io!(remove_file(&new_index), new_index))
    }
}

// TODO: move this to main.rs
pub fn init<C: CryptoScheme, P: AsRef<Path>>(source_path: &P,
                                             backup_path: &P,
                                             crypto_scheme: &C)
    -> BonzoResult<InitSummary>
{
    let database_path = source_path.as_ref().join(DATABASE_FILENAME);
    let database = try!(Database::create(database_path));
    let hash = crypto_scheme.hash_password();

    try!(database.setup());
    try!(database.set_key("password", &hash));

    let encoded_backup_path = try!(encode_path(backup_path));

    try!(database.set_key("backup_path", &encoded_backup_path));

    Ok(InitSummary)
}

fn create_parent_dir(path: &Path) -> BonzoResult<()> {
    let parent = try!(path.parent().ok_or(BonzoError::from_str("Couldn't get parent directory")));

    Ok(try_io!(create_dir_all(parent), path))
}

// Takes a path, turns it into an absolute path if necessary
fn encode_path<P: AsRef<Path>>(path: &P) -> io::Result<String> {
    if path.as_ref().is_relative() {
        let mut cwd = try!(current_dir());
        cwd.push(path);

        return Ok(cwd.to_string_lossy().into_owned())
    }

    Ok(path.as_ref().to_string_lossy().into_owned())
}

fn decode_path<P: AsRef<Path>>(path: &P) -> PathBuf {
    PathBuf::from(path.as_ref())
}

pub fn backup<'p, C: CryptoScheme, SP: IntoCow<'p, Path>>(
    source_path: SP,
    block_bytes: usize,
    crypto_scheme: &C,
    max_age_milliseconds: u64,
    deadline: time::Tm
) -> BonzoResult<BackupSummary> {
    let source_cow = source_path.into_cow();
    let database_path = source_cow.join(DATABASE_FILENAME);
    let database = try!(Database::from_file(database_path));
    let mut manager = try!(BackupManager::new(database, source_cow.into_owned(), crypto_scheme));
    let mut summary = try!(manager.update(block_bytes, deadline));

    if ! summary.timeout {
        let cleanup_summary = try!(manager.cleanup(max_age_milliseconds));
        summary.add_cleanup_summary(cleanup_summary);
    }
    
    try!(manager.export_index());

    Ok(summary)
}

pub fn restore<'p, 's, C: CryptoScheme, SP: IntoCow<'p, Path>, S: IntoCow<'s, str>>(
    source_path: SP,
    backup_path: SP,
    crypto_scheme: &C,
    timestamp: u64,
    filter: S
) -> BonzoResult<RestorationSummary> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let decrypted_index_path = try!(decrypt_index(&backup_path.into_cow(), temp_directory.path(), crypto_scheme));
    let database = try!(Database::from_file(decrypted_index_path));
    let manager = try!(BackupManager::new(database, source_path.into_cow().into_owned(), crypto_scheme));
    
    manager.restore(timestamp, filter.into_cow().into_owned())
}

pub fn epoch_milliseconds() -> u64 {
    let stamp = get_time();
    
    stamp.nsec as u64 / 1000 / 1000 + stamp.sec as u64 * 1000
}

fn decrypt_index<C: CryptoScheme>(backup_path: &Path, temp_dir: &Path, crypto_scheme: &C) -> BonzoResult<PathBuf> {
    let decrypted_index_path = temp_dir.join(DATABASE_FILENAME);
    let bytes = try!(load_processed_block(&backup_path.join("index"), crypto_scheme));

    try_io!(write_to_disk(&decrypted_index_path, &bytes), &decrypted_index_path);

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

    if let Err(..) = decompressor.read_to_end(&mut buffer) {
        println!("failed decompressing {:?}", path);
    }
    
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
    try!(file.sync_all());

    set_file_times(path, 0, 0)
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write, BufReader};
    use std::fs::{create_dir_all, File, copy};

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

        let deadline = time::now() + time::Duration::seconds(30);
        let crypto_scheme = super::crypto::AesEncrypter::new("passwerd");

        init(&source_dir.path(), &dest_dir.path(), &crypto_scheme).ok().expect("init ok");
        backup(source_dir.path(), 1_000_000, &crypto_scheme, 0, deadline).ok().expect("backup successful");
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

        let deadline = time::now() + time::Duration::seconds(30);
        let crypto_scheme = super::crypto::AesEncrypter::new("passwerd");

        init(&source_dir.path(), &dest_dir.path(), &crypto_scheme).ok().expect("init ok");
        backup(source_dir.path(), 1_000_000, &crypto_scheme, 0, deadline).ok().expect("backup successful");
        
        let file_one_hash = hash_file(&file_one_path).ok().expect("compute hash");
        let file_two_hash = hash_file(&file_two_path).ok().expect("compute hash");
        let file_one_out_path = block_output_path(dest_dir.path(), &file_one_hash);
        let file_two_out_path = block_output_path(dest_dir.path(), &file_two_hash);

        copy(file_one_out_path, file_two_out_path).ok().expect("copy files");

        let restore_dir = TempDir::new("integ-restore").unwrap();
        let result = restore(
            restore_dir.path(),
            dest_dir.path(),
            &crypto_scheme,
            epoch_milliseconds(),
            "**".to_string()
        );

        let is_expected = match result {
            Err(BonzoError::Other(ref str)) => &str[..] == "Block integrity check failed",
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
        assert!(file.write_all(&processed_bytes).is_ok());
        assert!(file.sync_all().is_ok());

        let retrieved_bytes = super::load_processed_block(&file_path, &crypto_scheme).unwrap();

        assert_eq!(&bytes[..], &retrieved_bytes[..]);
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

        assert!(&buffer[..] == message.as_bytes());
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
            
            let mut decompressor = BzDecompressor::new(BufReader::new(&compressed_bytes[..]));
            let mut decompressed_bytes = Vec::new();
            decompressor.read_to_end(&mut decompressed_bytes).unwrap();

            assert_eq!(slice, &decompressed_bytes[..]);
        }
    }
}
