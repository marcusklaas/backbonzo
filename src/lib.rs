#![feature(associated_types)]

extern crate serialize;
extern crate time;
extern crate bzip2;
extern crate glob;

use std::io::{IoError, IoResult, TempDir, BufReader};
use std::io::fs::{unlink, copy, File, mkdir_recursive};
use std::error::FromError;
use std::path::Path;
use std::collections::RingBuf;
use std::cmp::Ordering;

use bzip2::reader::BzDecompressor;
use glob::Pattern;

use export::FileInstruction;
use database::{Database, SqliteError};
use crypto::SymmetricCipherError;

mod database;
mod crypto;
mod export;

pub enum BonzoError {
    Database(SqliteError),
    Io(IoError),
    Crypto(SymmetricCipherError),
    Other(String)
}

// TODO: once again put directory id in enum

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

pub type BonzoResult<T> = Result<T, BonzoError>;

pub struct BackupManager {
    database: Database,
    source_path: Path,
    backup_path: Path,
    encryption_key: Vec<u8>
}

impl BackupManager {
    pub fn new(database_path: Path, source_path: Path, backup_path: Path, password: String) -> BonzoResult<BackupManager> {
        let manager = BackupManager {
            database: try!(Database::from_file(database_path)),
            source_path: source_path,
            backup_path: backup_path,
            encryption_key: crypto::derive_key(password.as_slice())
        };

        try!(manager.check_password(password.as_slice()));

        Ok(manager)
    }

    pub fn update(&mut self, block_bytes: u32, deadline: time::Tm) -> BonzoResult<()> {
        let channel_receiver = export::start_export_thread(
            self.database.get_path(),
            self.encryption_key.clone(),
            block_bytes,
            self.source_path.clone()
        );
        
        let mut id_queue: RingBuf<u32> = RingBuf::new();

        for msg in channel_receiver.iter() {
            match msg {
                FileInstruction::Error(e) => return Err(e),
                FileInstruction::NewBlock(block) => {
                    let path = block_output_path(&self.backup_path, block.hash.as_slice());
                    
                    try!(mkdir_recursive(&path.dir_path(), std::io::FilePermission::all())
                        .and(write_to_disk(&path, block.bytes.as_slice())));
        
                    try!(self.database.persist_block(block.hash.as_slice(), block.iv.as_slice())
                        .map(|id| id_queue.push_back(id)));
                },
                FileInstruction::Complete(file) => {
                    let real_id_list = try!(file.block_id_list.iter()
                        .map(|&id| id.or(id_queue.pop_front()))
                        .collect::<Option<Vec<u32>>>()
                        .ok_or(BonzoError::Other(format!("Block buffer is empty"))));

                    try!(self.database.persist_file(
                        file.directory_id,
                        file.filename.as_slice(),
                        file.hash.as_slice(),
                        file.last_modified,
                        real_id_list.as_slice()
                    ));
                }
            }

            if deadline.cmp(&time::now_utc()) != Ordering::Greater {
                break;
            }
        }

        Ok(())
    }

    pub fn restore(&self, timestamp: u64, filter: String) -> BonzoResult<()> {
        let pattern = Pattern::new(filter.as_slice());
        
        try!(database::Aliases::new(&self.database, self.source_path.clone(), 0, timestamp))
            .filter(|&(ref path, _)| pattern.matches_path(path))
            .map(|(path, block_list)| self.restore_file(&path, block_list.as_slice()))
            .fold(Ok(()), |a, b| a.and(b))
    }

    pub fn restore_file(&self, path: &Path, block_list: &[u32]) -> BonzoResult<()> {
        try!(mkdir_recursive(&path.dir_path(), std::io::FilePermission::all()));
        
        let mut file = try!(File::create(path));

        for block_id in block_list.iter() {
            let (hash, iv) = try!(self.database.block_from_id(*block_id));
            let block_path = block_output_path(&self.backup_path, hash.as_slice());
            let mut block_file = try!(File::open(&block_path));
            let bytes = try!(block_file.read_to_end());
            let decrypted_bytes = try!(crypto::decrypt_block(bytes.as_slice(), self.encryption_key.as_slice(), iv.as_slice()));
            let mut decompressor = BzDecompressor::new(BufReader::new(decrypted_bytes.as_slice()));
            let decompresed_bytes = try!(decompressor.read_to_end());

            try!(file.write(decompresed_bytes.as_slice()));
            try!(file.fsync());
        }

        Ok(())
    }

    fn check_password(&self, password: &str) -> BonzoResult<()> {
        let hash = self.database.get_key("password");
        let real_hash = try!(hash.ok_or(BonzoError::Other(format!("Saved hash is NULL"))));

        match crypto::check_password(password, real_hash.as_slice()) {
            true  => Ok(()),
            false => Err(BonzoError::Other(format!("Password is not the same as in database")))
        }
    }

    fn export_index(self) -> BonzoResult<()> {
        let bytes = try!(self.database.to_bytes());
        let iv = [0u8; 16];
        let encrypted_bytes = try!(crypto::encrypt_block(bytes.as_slice(), self.encryption_key.as_slice(), &iv));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");
        
        try!(write_to_disk(&new_index, encrypted_bytes.as_slice()));
        try!(copy(&new_index, &index));
        
        Ok(try!(unlink(&new_index)))
    }
}

pub fn init(database_path: Path, password: String) -> BonzoResult<()> {
    let database = try!(Database::create(database_path));
    let hash = try!(crypto::hash_password(password.as_slice()));

    Ok(try!(database.setup().and(database.set_key("password", hash.as_slice()).map(|_|()))))
}

pub fn backup(database_path: Path, source_path: Path, backup_path: Path, block_bytes: u32, password: String, deadline: time::Tm) -> BonzoResult<()> {
    let mut manager = try!(BackupManager::new(database_path, source_path, backup_path, password));
            
    manager.update(block_bytes, deadline).and(manager.export_index())
}

pub fn restore(source_path: Path, backup_path: Path, password: String, timestamp: u64, filter: String) -> BonzoResult<()> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let decrypted_index_path = try!(decrypt_index(&backup_path, temp_directory.path(), password.as_slice()));
    let manager = try!(BackupManager::new(decrypted_index_path, source_path, backup_path, password));
    
    manager.restore(timestamp, filter)
}

// TODO: find a better solution for this
fn decrypt_index(backup_path: &Path, temp_dir: &Path, password: &str) -> BonzoResult<Path> {
    let encrypted_index_path = backup_path.join("index");
    let decrypted_index_path = temp_dir.join("index.db3");
    let mut file = try!(File::open(&encrypted_index_path));
    let contents = try!(file.read_to_end());
    let iv = [0u8; 16];
    let key = crypto::derive_key(password.as_slice());
    let decrypted_content = try!(crypto::decrypt_block(contents[], key[], &iv));

    try!(write_to_disk(&decrypted_index_path, decrypted_content[]));

    Ok(decrypted_index_path)
}

fn block_output_path(base_path: &Path, hash: &str) -> Path {
    base_path.join_many(&[hash.slice(0, 2), hash])
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> IoResult<()> {
    File::create(path).and_then(|mut file| {
        file.write(bytes).and(file.fsync())
    })
}
