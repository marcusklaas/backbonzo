#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "rust-crypto" as rust_crypto;

use std::io::{IoError, IoResult};
use std::io::fs::{readdir, walk_dir, File, PathExtensions};
use std::path::posix::Path;
use rusqlite::{SqliteConnection, SqliteError};
use rust_crypto::symmetriccipher::SymmetricCipherError;

mod database;
mod crypto;

static DATABASE_FILE: &'static str = "index.db3";
static TEMP_INPUT_DIRECTORY: &'static str = ".";
static TEMP_OUTPUT_DIRECTORY: &'static str = "/tmp/";
static BLOCK_SIZE: uint = 1024 * 1024;

/* TODO: there should be a different type Result<T, SomeEnum> because we use this all the time. using String instead of &'static str allows to return dynamic error messages */

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

fn io_error_to_bonzo(err: IoError) -> BonzoError {
    BonzoError::Io(err)
}

fn database_error_to_bonzo(err: SqliteError) -> BonzoError {
    BonzoError::Database(err)
}

fn crypto_error_to_bonzo(err: SymmetricCipherError) -> BonzoError {
    BonzoError::Crypto(err)
}

pub fn init() -> BonzoResult<()> {
    println!("Setting up database..");
    
    let database_path = Path::new(DATABASE_FILE);
    
    let filename = try!(database_path.as_str().ok_or(BonzoError::Other("Couldn't convert database path to string".to_string()))); 
    
    let connection = try!(database::create(filename).map_err(database_error_to_bonzo));
    
    try!(database::setup(&connection).map_err(database_error_to_bonzo));
    
    println!("Populating database..");
    
    populate_database(&connection)
}

fn populate_database(connection: &SqliteConnection) -> BonzoResult<()> {
    let working_dir = Path::new(TEMP_INPUT_DIRECTORY);
    
    export_directory(connection, &working_dir, Directory::Root)
}

fn export_directory(connection: &SqliteConnection, path: &Path, directory: Directory) -> BonzoResult<()> {
    let content_list = try!(readdir(path).map_err(io_error_to_bonzo));
    let (directory_list, file_list) = content_list.partition(|path| path.is_dir());
    
    for file_path in file_list.iter() {
        try!(export_file(connection, directory, file_path));
    }
    
    for directory_path in directory_list.iter() {
        let relative_path = try!(directory_path.path_relative_from(path).ok_or(BonzoError::Other("... no words".to_string())));
    
        let name = try!(relative_path.as_str().ok_or(BonzoError::Other("Cannot express directory name in UTF8".to_string())));
        
        let child_directory = try!(database::get_directory_id(connection, directory, name).map_err(database_error_to_bonzo));
    
        try!(export_directory(connection, directory_path, child_directory));
    }

    Ok(())
}

fn export_file(connection: &SqliteConnection, directory: Directory, path: &Path) -> BonzoResult<()> {
    let hash: String = try!(crypto::hash_file(path).map_err(io_error_to_bonzo));
    
    if database::file_known(connection, hash.as_slice()) {
        return Ok(());
    }
    
    let mut blocks = try!(Blocks::from_path(path, BLOCK_SIZE).map_err(io_error_to_bonzo));
    
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
    
    database::persist_file(
        connection,
        directory,
        filename.as_slice(),
        hash.as_slice(),
        block_id_list.as_slice()
    ).map_err(database_error_to_bonzo)
}

fn export_block(connection: &SqliteConnection, block: &[u8]) -> BonzoResult<BlockExportResult> {
    let hash = crypto::hash_block(block);

    if database::block_known(connection, hash.as_slice()) {
        return Ok(BlockExportResult::Known);
    }
    
    let bytes: Vec<u8> = try!(crypto::encrypt_block(block).map_err(crypto_error_to_bonzo));
    
    try!(write_block(hash.as_slice(), bytes.as_slice()).map_err(io_error_to_bonzo));
    
    database::persist_block(connection, hash.as_slice())
        .map(|id| BlockExportResult::New(id))
        .map_err(database_error_to_bonzo)
}

fn write_block(hash: &str, bytes: &[u8]) -> IoResult<()> {
    let mut path = Path::new(TEMP_OUTPUT_DIRECTORY);
    path.push(hash);

    let mut file = try!(File::create(&path));
    
    try!(file.write(bytes));
    
    file.fsync()
}
