#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "crypto" as rust_crypto;

use std::io::{IoError, IoResult};
use std::io::fs::{unlink, copy, readdir, File, PathExtensions, mkdir_recursive};
use std::path::posix::Path;
use rusqlite::{SqliteConnection, SqliteError};
use rust_crypto::symmetriccipher::SymmetricCipherError;

mod database;
mod crypto;

const BLOCK_SIZE: uint = 1024 * 1024;

pub enum BonzoError {
    Database(SqliteError),
    Io(IoError),
    Crypto(SymmetricCipherError),
    Other(String)
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
    password: Vec<u8>
}

impl BackupManager {
    pub fn new(database_path: Path, source_path: Path, backup_path: Path, password: Vec<u8>) -> BonzoResult<BackupManager> {
        if !database_path.exists() {
            return Err(BonzoError::Other(format!("Database file not found"))); 
        }
                
        Ok(BackupManager {
            connection: try!(open_connection(&database_path)),
            database_path: database_path,
            source_path: source_path,
            backup_path: backup_path,
            password: password
        })
    }

    pub fn init(database_path: &Path) -> BonzoResult<()> {
        if database_path.exists() {
            return Err(BonzoError::Other(format!("Database file already exists"))); 
        }
        
        let connection = try!(open_connection(database_path));
        
        database::setup(&connection).map_err(database_to_bonzo)
    }

    pub fn update(&self) -> BonzoResult<()> {
        try!(self.export_directory(&self.source_path, Directory::Root));
    
        self.export_index()
    }

    pub fn restore(&self, timestamp: u64) -> BonzoResult<()> {
        let mut aliases = try!(database::Aliases::new(&self.connection, self.source_path.clone(), Directory::Root, timestamp).map_err(database_to_bonzo));

        for (path, block_list) in aliases {
            try!(self.restore_file(&path, block_list.as_slice()));
        }

        Ok(())
    }

    pub fn restore_file(&self, path: &Path, block_list: &[uint]) -> BonzoResult<()> {
        // create output directory and  ignore error - it most likely already exists.
        let mut file_directory = path.clone();
        file_directory.pop();
        mkdir_recursive(&file_directory, std::io::FilePermission::all());
        
        // open file
        let mut file = try!(File::create(path).map_err(io_to_bonzo));

        for block_id in block_list.iter() {
            // get hash
            let hash = database::block_hash_from_id(&self.connection, *block_id);

            // open block
            let block_path = block_output_path(&self.backup_path, hash.as_slice());
            let mut block_file = try!(File::open(&block_path).map_err(io_to_bonzo));
            let bytes = try!(block_file.read_to_end().map_err(io_to_bonzo));

            // decrypt block
            let decrypted_bytes = try!(crypto::decrypt_block(bytes.as_slice(), self.password.as_slice()).map_err(crypto_to_bonzo));

            // write to file
            try!(file.write(decrypted_bytes.as_slice()).map_err(io_to_bonzo));
            try!(file.fsync().map_err(io_to_bonzo));
        }

        Ok(())
    }

    fn export_index(&self) -> BonzoResult<()> {
        let mut file = try!(File::open(&self.database_path).map_err(io_to_bonzo));
        let bytes = try!(file.read_to_end().map_err(io_to_bonzo));
        let encrypted_bytes = try!(crypto::encrypt_block(bytes.as_slice(), self.password.as_slice()).map_err(crypto_to_bonzo));
        
        let mut new_index = self.backup_path.clone();
        new_index.push("index-new");
        
        let mut index = self.backup_path.clone();
        index.push("index");
        
        try!(write_to_disk(&new_index, encrypted_bytes.as_slice()).map_err(io_to_bonzo));
        try!(copy(&new_index, &index).map_err(io_to_bonzo));
        unlink(&new_index).map_err(io_to_bonzo)
    }

    fn export_directory(&self, path: &Path, directory: Directory) -> BonzoResult<()> {
        let content_list = try!(readdir(path).map_err(io_to_bonzo));
        let (directory_list, file_list) = content_list.partition(|p| p.is_dir());
        
        for file_path in file_list.iter() {
            try!(self.export_file(directory, file_path));
        }
        
        for directory_path in directory_list.iter() {
            let relative_path = try!(directory_path.path_relative_from(path).ok_or(BonzoError::Other("Could not get relative path".to_string())));
            let name = try!(relative_path.as_str().ok_or(BonzoError::Other("Cannot express directory name in UTF8".to_string())));
            let child_directory = try!(database::get_directory(&self.connection, directory, name).map_err(database_to_bonzo));
        
            try!(self.export_directory(directory_path, child_directory));
        }

        Ok(())
    }

    fn export_file(&self, directory: Directory, path: &Path) -> BonzoResult<()> {
        let hash: String = try!(crypto::hash_file(path).map_err(io_to_bonzo));
        
        if database::file_known(&self.connection, hash.as_slice()) {
            return Ok(());
        }
        
        let mut blocks = try!(Blocks::from_path(path, BLOCK_SIZE).map_err(io_to_bonzo));
        let mut block_id_list = Vec::new();
        
        loop {
            match blocks.next() {
                Some(slice) => block_id_list.push(try!(self.export_block(slice))),
                None        => break
            }
        }
        
        let filename_bytes = try!(path.filename().ok_or(BonzoError::Other("Could not convert path to string".to_string())));
        let filename = String::from_utf8_lossy(filename_bytes).into_owned();
        
        database::persist_file(
            &self.connection,
            directory,
            filename.as_slice(),
            hash.as_slice(),
            block_id_list.as_slice()
        ).map_err(database_to_bonzo)
    }

    fn export_block(&self, block: &[u8]) -> BonzoResult<uint> {
        let hash = crypto::hash_block(block);

        if let Some(id) = database::block_id_from_hash(&self.connection, hash.as_slice()) {
            return Ok(id)
        }
        
        let bytes: Vec<u8> = try!(crypto::encrypt_block(block, self.password.as_slice()).map_err(crypto_to_bonzo));
        let path = block_output_path(&self.backup_path, hash.as_slice());
            
        try!(write_to_disk(&path, bytes.as_slice()).map_err(io_to_bonzo));
        
        database::persist_block(&self.connection, hash.as_slice())
            .map_err(database_to_bonzo)
    }
}

fn io_to_bonzo(err: IoError) -> BonzoError {
    BonzoError::Io(err)
}

fn database_to_bonzo(err: SqliteError) -> BonzoError {
    BonzoError::Database(err)
}

fn crypto_to_bonzo(err: SymmetricCipherError) -> BonzoError {
    BonzoError::Crypto(err)
}

fn open_connection(path: &Path) -> BonzoResult<SqliteConnection> {
    let error = BonzoError::Other(format!("Couldn't convert database path to string"));
    let filename = try!(path.as_str().ok_or(error)); 

    SqliteConnection::open(filename).map_err(database_to_bonzo)
}

fn block_output_path(base_path: &Path, hash: &str) -> Path {
    let mut path = base_path.clone();
    path.push(hash[0..2]);
    
    // ignore error - it most likely already exists.
    mkdir_recursive(&path, std::io::FilePermission::all());
    
    path.push(hash);
    
    path
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> IoResult<()> {
    let mut file = try!(File::create(path));
    
    try!(file.write(bytes));
    
    file.fsync()
}
