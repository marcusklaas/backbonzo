#![feature(slicing_syntax)]

extern crate rusqlite;

use std::io::IoResult;
use std::io::fs::{readdir, walk_dir, File, PathExtensions};
use std::path::posix::Path;
use rusqlite::{SqliteConnection, SqliteError};

mod database;
mod crypto;

static DATABASE_FILE: &'static str = "index.db3";
static TEMP_INPUT_DIRECTORY: &'static str = ".";
static TEMP_OUTPUT_DIRECTORY: &'static str = "/tmp/";
static BLOCK_SIZE: uint = 1024 * 1024;

/* TODO: there should be a different type Result<T, SomeEnum> because we use this all the time. using String instead of &'static str allows to return dynamic error messages */

enum BonzoError {
    DatabaseError(SqliteError),
    IoError
}

type BonzoResult<T> = Result<T, BonzoError>;

struct Blocks<'a> {
    file: File,
    buffer: Box<[u8]>
}

impl<'a> Blocks<'a> {
    pub fn from_path(path: &Path, block_size: uint) -> Option<Blocks> {
        let file = match File::open(path) {
            Ok(f)   => f,
            Err(..) => return None
        };
        
        Some(Blocks {
            file: file,
            buffer: Vec::from_elem(block_size, 0).into_boxed_slice()
        })
    }
    
    pub fn next(&'a mut self) -> Option<&'a [u8]> {
        match self.file.read(&mut *self.buffer) {
            Err(..)   => None,
            Ok(bytes) => {
                let slice: &'a [u8] = self.buffer[0..bytes];
            
                Some(slice)
            }
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

pub fn init() -> Result<(), &'static str> {
    println!("Setting up database..");
    
    let database_path = Path::new(DATABASE_FILE);
    
    let connection = match database::setup(&Path::new(DATABASE_FILE)) {
        Ok(conn) => conn,
        Err(e)   => return Err(e)
    };
    
    //let connection = match SqliteConnection::open(DATABASE_FILE) {
    //    Ok(conn) => conn,
    //    Err(_)   => return Err("Couldn't create database")
    //};
    
    println!("Populating database..");
    
    populate_database(&connection)
}

fn populate_database(connection: &SqliteConnection) -> Result<(), &'static str> {
    let working_dir = Path::new(TEMP_INPUT_DIRECTORY);
    
    export_directory(connection, &working_dir, Directory::Root)
}

fn export_directory(connection: &SqliteConnection, path: &Path, directory: Directory) -> Result<(), &'static str> {
    let content_list = match readdir(path) {
        Ok(content) => content,
        Err(..)     => return Err("Could not read directory!")
    };
    
    let (directory_list, file_list) = content_list.partition(|path| path.is_dir());
    
    for file_path in file_list.iter() {
        try!(export_file(connection, directory, file_path));
    }
    
    for directory_path in directory_list.iter() {
        let relative_path = match directory_path.path_relative_from(path) {
            None    => return Err("... no words"),
            Some(p) => p
        };
    
        let name = match relative_path.as_str() {
            None    => return Err("Cannot express directory name in UTF8"),
            Some(s) => s
        };
        
        let child_directory = try!(database::get_directory_id(connection, directory, name));
    
        try!(export_directory(connection, directory_path, child_directory));
    }

    Ok(())
}

fn export_file(connection: &SqliteConnection, directory: Directory, path: &Path) -> Result<(), &'static str> {
    let hash = match database::file_known(connection, path) {
        Ok((true, _))     => return Ok(()),
        Err(e)            => return Err(e),
        Ok((false, hash)) => hash
    };
    
    let mut blocks = match Blocks::from_path(path, BLOCK_SIZE) {
        Some(blocks) => blocks,
        None         => return Err("Couldn't read file")
    };
    
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
    
    if !database::persist_file(connection, directory, path, hash.as_slice(), block_id_list.as_slice()) {
        println!("Failed persisting file to database");
    }
    
    Ok(())
}

fn export_block(connection: &SqliteConnection, block: &[u8]) -> Result<BlockExportResult, &'static str> {
    let hash = crypto::hash_block(block);

    if database::block_known(connection, hash.as_slice()) {
        return Ok(BlockExportResult::Known);
    }
    
    let bytes = match crypto::encrypt_block(block) {
        Some(vector) => vector,
        None         => return Err("Failed encrypting block")
    };
    
    match write_block(hash.as_slice(), bytes.as_slice()) {
        Err(_) => return Err("Failed writing block to disk"),
        Ok(..) => ()
    }
    
    match database::persist_block(connection, hash.as_slice()) {
        None     => Err("Failed persisting block to database"),
        Some(id) => Ok(BlockExportResult::New(id))
    }
}

fn write_block(hash: &str, bytes: &[u8]) -> IoResult<()> {
    let mut path = Path::new(TEMP_OUTPUT_DIRECTORY);
    path.push(hash);

    let mut file = match File::create(&path) {
        Ok(f)  => f,
        Err(e) => return Err(e)
    };
    
    match file.write(bytes) {
        Ok(..) => file.fsync(),
        Err(e) => Err(e)
    }
}
