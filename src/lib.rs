#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "crypto" as rust_crypto;

use std::io::{IoError, IoResult, TempDir};
use std::io::fs::{unlink, copy, readdir, File, PathExtensions, mkdir_recursive};
use std::error::FromError;
use std::path::Path;
use rusqlite::{SqliteConnection, SqliteError};
use rust_crypto::symmetriccipher::SymmetricCipherError;

mod database;
mod crypto;

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

pub type BonzoResult<T> = Result<T, BonzoError>;

struct Blocks<'a> {
    file: File,
    buffer: Box<[u8]>
}

impl<'a> Blocks<'a> {
    pub fn from_path(path: &Path, block_size: uint) -> IoResult<Blocks> {
        let file = try!(File::open(path));
        
        Ok(Blocks {
            file: file,
            buffer: Vec::from_elem(block_size, 0).into_boxed_slice()
        })
    }
    
    pub fn next(&'a mut self) -> Option<&'a [u8]> {
        match self.file.read(&mut *self.buffer) {
            Err(..)   => None,
            Ok(bytes) => Some(self.buffer[0..bytes])
        }
    }
}

#[deriving(Copy)]
enum Directory {
    Root,
    Child(uint)
}

pub struct BackupManager {
    connection: SqliteConnection,
    database_path: Path,
    source_path: Path,
    backup_path: Path,
    block_size: uint,
    password: String,
    encryption_key: Vec<u8>
}

impl BackupManager {
    pub fn new(database_path: Path, source_path: Path, backup_path: Path, block_size: uint, password: String) -> BonzoResult<BackupManager> {
        if !database_path.exists() {
            return Err(BonzoError::Other(format!("Database file not found"))); 
        }

        let key = crypto::derive_key(password.as_slice());
                
        Ok(BackupManager {
            connection: try!(open_connection(&database_path)),
            database_path: database_path,
            source_path: source_path,
            backup_path: backup_path,
            block_size: block_size,
            password: password,
            encryption_key: key
        })
    }

    pub fn update(&self) -> BonzoResult<()> {
        try!(self.check_key());

        try!(self.export_directory(&self.source_path, Directory::Root));
    
        self.export_index()
    }

    pub fn restore(&self, timestamp: u64) -> BonzoResult<()> {
        let mut aliases = try!(database::Aliases::new(&self.connection, self.source_path.clone(), Directory::Root, timestamp));

        for (path, block_list) in aliases {
            try!(self.restore_file(&path, block_list.as_slice()));
        }

        Ok(())
    }

    pub fn restore_file(&self, path: &Path, block_list: &[uint]) -> BonzoResult<()> {
        // create output directory
        let mut file_directory = path.clone();
        file_directory.pop();
        try!(mkdir_recursive(&file_directory, std::io::FilePermission::all()));
        
        // open file
        let mut file = try!(File::create(path));

        for block_id in block_list.iter() {
            // get hash
            let hash = database::block_hash_from_id(&self.connection, *block_id);

            // open block
            let block_path = try!(block_output_path(&self.backup_path, hash.as_slice()));
            let mut block_file = try!(File::open(&block_path));
            let bytes = try!(block_file.read_to_end());

            // decrypt block
            let decrypted_bytes = try!(crypto::decrypt_block(bytes.as_slice(), self.encryption_key.as_slice()));

            // write to file
            try!(file.write(decrypted_bytes.as_slice()));
            try!(file.fsync());
        }

        Ok(())
    }

    fn check_key(&self) -> BonzoResult<()> {
        let hash = database::get_key(&self.connection, "password");
        let real_hash = try!(hash.ok_or(BonzoError::Other(format!("Saved hash is NULL"))));

        match crypto::check_password(self.password.as_slice(), real_hash.as_slice()) {
            true  => Ok(()),
            false => Err(BonzoError::Other(format!("Password is not the same as in database")))
        }
    }

    fn export_index(&self) -> BonzoResult<()> {
        let mut file = try!(File::open(&self.database_path));
        let bytes = try!(file.read_to_end());
        let encrypted_bytes = try!(crypto::encrypt_block(bytes.as_slice(), self.encryption_key.as_slice()));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");
        
        try!(write_to_disk(&new_index, encrypted_bytes.as_slice()));
        try!(copy(&new_index, &index));
        
        Ok(try!(unlink(&new_index)))
    }

    fn export_directory(&self, path: &Path, directory: Directory) -> BonzoResult<()> {
        let content_list = try!(readdir(path));
        let (directory_list, file_list) = content_list.partition(|p| p.is_dir());
        
        for file_path in file_list.iter() {
            try!(self.export_file(directory, file_path));
        }
        
        for directory_path in directory_list.iter() {
            let relative_path = try!(directory_path.path_relative_from(path).ok_or(BonzoError::Other(format!("Could not get relative path"))));
            let name = try!(relative_path.as_str().ok_or(BonzoError::Other(format!("Cannot express directory name in UTF8"))));
            let child_directory = try!(database::get_directory(&self.connection, directory, name));
        
            try!(self.export_directory(directory_path, child_directory));
        }

        Ok(())
    }

    fn export_file(&self, directory: Directory, path: &Path) -> BonzoResult<()> {
        let hash: String = try!(crypto::hash_file(path));
        
        if database::file_known(&self.connection, hash.as_slice()) {
            return Ok(());
        }
        
        let mut blocks = try!(Blocks::from_path(path, self.block_size));
        let mut block_id_list = Vec::new();
        
        loop {
            match blocks.next() {
                Some(slice) => block_id_list.push(try!(self.export_block(slice))),
                None        => break
            }
        }
        
        let filename_bytes = try!(path.filename().ok_or(BonzoError::Other(format!("Could not convert path to string"))));
        let filename = String::from_utf8_lossy(filename_bytes).into_owned();
        
        Ok(try!(database::persist_file(
            &self.connection,
            directory,
            filename.as_slice(),
            hash.as_slice(),
            block_id_list.as_slice()
        )))
    }

    fn export_block(&self, block: &[u8]) -> BonzoResult<uint> {
        let hash = crypto::hash_block(block);

        if let Some(id) = database::block_id_from_hash(&self.connection, hash.as_slice()) {
            return Ok(id)
        }
        
        let bytes: Vec<u8> = try!(crypto::encrypt_block(block, self.encryption_key.as_slice()));
        let path = try!(block_output_path(&self.backup_path, hash.as_slice()));
            
        try!(write_to_disk(&path, bytes.as_slice()));
        
        Ok(try!(database::persist_block(&self.connection, hash.as_slice())))
    }
}

pub fn init(database_path: &Path, password: String) -> BonzoResult<()> {
    if database_path.exists() {
        return Err(BonzoError::Other(format!("Database file already exists"))); 
    }
    
    let connection = try!(open_connection(database_path));
    let hash = try!(crypto::hash_password(password.as_slice()));
    
    try!(database::setup(&connection));

    Ok(try!(database::set_key(&connection, "password", hash.as_slice()).map(|_| ())))
}

pub fn backup(database_path: Path, source_path: Path, backup_path: Path, block_bytes: uint, password: String) -> BonzoResult<()> {
    let manager = try!(BackupManager::new(database_path, source_path, backup_path, block_bytes, password));
            
    manager.update()
}

pub fn restore(source_path: Path, backup_path: Path, block_bytes: uint, password: String, timestamp: u64) -> BonzoResult<()> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let decrypted_index_path = try!(decrypt_index(&backup_path, temp_directory.path(), password.as_slice()));
    let manager = try!(BackupManager::new(decrypted_index_path, source_path, backup_path, block_bytes, password));
    
    manager.restore(timestamp)
}

fn decrypt_index(backup_path: &Path, temp_dir: &Path, password: &str) -> BonzoResult<Path> {
    let encrypted_index_path = backup_path.join("index");
    let decrypted_index_path = temp_dir.join("index.db3");

    let mut file = try!(File::open(&encrypted_index_path));
    let contents = try!(file.read_to_end());

    let key = crypto::derive_key(password.as_slice());
    let decrypted_content = try!(crypto::decrypt_block(contents[], key[]));

    try!(write_to_disk(&decrypted_index_path, decrypted_content[]));

    Ok(decrypted_index_path)
}

fn open_connection(path: &Path) -> BonzoResult<SqliteConnection> {
    let error = BonzoError::Other(format!("Couldn't convert database path to string"));
    let filename = try!(path.as_str().ok_or(error)); 

    Ok(try!(SqliteConnection::open(filename)))
}

fn block_output_path(base_path: &Path, hash: &str) -> IoResult<Path> {
    let path = base_path.join(hash[0..2]);
    
    try!(mkdir_recursive(&path, std::io::FilePermission::all()));
    
    Ok(path.join(hash))
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> IoResult<()> {
    let mut file = try!(File::create(path));
    
    try!(file.write(bytes));
    
    file.fsync()
}
