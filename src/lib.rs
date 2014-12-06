#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "rust-crypto" as rust_crypto;

use std::io::{IoError, IoResult};
use std::io::fs::{unlink, copy, readdir, File, PathExtensions, mkdir_recursive};
use std::path::posix::Path;
use rusqlite::{SqliteConnection, SqliteError};
use rust_crypto::symmetriccipher::SymmetricCipherError;

mod database;
mod crypto;

static TEMP_OUTPUT_DIRECTORY: &'static str = "/tmp/backbonzo/";
static BLOCK_SIZE: uint = 1024 * 1024;

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

enum BlockExportResult {
    Known,
    New(uint)
}

// CONSIDER: if we make this an Option<i64>, we won't have to cast when inserting/selecting to database
enum Directory {
    Root,
    Child(uint)
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

/// Start backup from scratch 
pub fn init(database_path: &Path) -> BonzoResult<()> {
    if database_path.exists() {
        return Err(BonzoError::Other(format!("Database already exists"))); 
    }
    
    let connection = try!(open_connection(database_path));
    
    database::setup(&connection).map_err(database_to_bonzo)
}

/// Update previous backup
pub fn update(path: &Path, database_path: &Path) -> BonzoResult<()> {
    let connection = try!(open_connection(database_path));
    
    try!(export_directory(&connection, path, Directory::Root));
    
    export_index(database_path)
}

fn export_index(database_path: &Path) -> BonzoResult<()> {
	let mut file = try!(File::open(database_path).map_err(io_to_bonzo));
    let bytes = try!(file.read_to_end().map_err(io_to_bonzo));
    let encrypted_bytes = try!(crypto::encrypt_block(bytes.as_slice()).map_err(crypto_to_bonzo));
    let output_directory = Path::new(TEMP_OUTPUT_DIRECTORY);
    
    let mut new_index = output_directory.clone();
    new_index.push("index-new");
    
    let mut index = output_directory.clone();
    index.push("index");
    
    try!(write_to_disk(&new_index, encrypted_bytes.as_slice()).map_err(io_to_bonzo));
    try!(copy(&new_index, &index).map_err(io_to_bonzo));
    unlink(&new_index).map_err(io_to_bonzo)
}

fn open_connection(path: &Path) -> BonzoResult<SqliteConnection> {
    let error = BonzoError::Other(format!("Couldn't convert database path to string"));
    let filename = try!(path.as_str().ok_or(error)); 

    SqliteConnection::open(filename).map_err(database_to_bonzo)
}

fn export_directory(connection: &SqliteConnection, path: &Path, directory: Directory) -> BonzoResult<()> {
    let content_list = try!(readdir(path).map_err(io_to_bonzo));
    let (directory_list, file_list) = content_list.partition(|path| path.is_dir());
    
    for file_path in file_list.iter() {
        try!(export_file(connection, directory, file_path));
    }
    
    for directory_path in directory_list.iter() {
        let relative_path = try!(directory_path.path_relative_from(path).ok_or(BonzoError::Other("... no words".to_string())));
    
        let name = try!(relative_path.as_str().ok_or(BonzoError::Other("Cannot express directory name in UTF8".to_string())));
        
        let child_directory = try!(database::get_directory_id(connection, directory, name).map_err(database_to_bonzo));
    
        try!(export_directory(connection, directory_path, child_directory));
    }

    Ok(())
}

fn export_file(connection: &SqliteConnection, directory: Directory, path: &Path) -> BonzoResult<()> {
    let hash: String = try!(crypto::hash_file(path).map_err(io_to_bonzo));
    
    if database::file_known(connection, hash.as_slice()) {
        return Ok(());
    }
    
    let mut blocks = try!(Blocks::from_path(path, BLOCK_SIZE).map_err(io_to_bonzo));
    
    /* FIXME: this next block should be done functionally */
    let mut block_id_list = Vec::new();
    
    loop {
        match blocks.next() {
            Some(slice) => match export_block(connection, slice) {
                Err(e)                         => return Err(e),
                Ok(BlockExportResult::New(id)) => block_id_list.push(id),
                Ok(BlockExportResult::Known)   => ()
            },
            None        => break
        }
    }
    
    let filename_bytes = try!(path.filename().ok_or(BonzoError::Other("Could not convert path to string".to_string())));

    let filename: String = String::from_utf8_lossy(filename_bytes).into_owned();
    
    // FIXME: this is critically bugged! if some blocks are already known for a file, they won't be included in its block list!
    database::persist_file(
        connection,
        directory,
        filename.as_slice(),
        hash.as_slice(),
        block_id_list.as_slice()
    ).map_err(database_to_bonzo)
}

fn export_block(connection: &SqliteConnection, block: &[u8]) -> BonzoResult<BlockExportResult> {
    let hash = crypto::hash_block(block);

    if database::block_known(connection, hash.as_slice()) {
        return Ok(BlockExportResult::Known);
    }
    
    let bytes: Vec<u8> = try!(crypto::encrypt_block(block).map_err(crypto_to_bonzo));
    let path = block_output_path(hash.as_slice());
        
    try!(write_to_disk(&path, bytes.as_slice()).map_err(io_to_bonzo));
    
    database::persist_block(connection, hash.as_slice())
        .map(|id| BlockExportResult::New(id))
        .map_err(database_to_bonzo)
}

fn block_output_path(hash: &str) -> Path {
    let mut path = Path::new(TEMP_OUTPUT_DIRECTORY);
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
