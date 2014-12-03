#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "rust-crypto" as crypto;
extern crate time;

use rusqlite::{SqliteConnection, types};
use std::io::IoResult;
use std::io::fs::{readdir, walk_dir, File, PathExtensions};
use std::path::posix::Path;

use crypto::digest::Digest;
use crypto::aes;
use crypto::buffer;
use crypto::buffer::{WriteBuffer, ReadBuffer};
use crypto::symmetriccipher;
use crypto::blockmodes::{NoPadding, PkcsPadding};

static DATABASE_FILE: &'static str = "index.db3";
static BLOCK_SIZE: uint = 1024 * 1024;
static TEST_KEY: &'static str = "testkey123";
static TEMP_OUTPUT_DIRECTORY: &'static str = "/tmp/";

/* TODO: check out try! macro. may be useful for this project */

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

enum Directory {
    Root,
    Child(uint)
}

pub fn init() -> Result<(), &'static str> {
    println!("Setting up database..");
    
    let connection = match setup_database() {
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
    let working_dir = Path::new(".");
    
    let path_list = match walk_dir(&working_dir) {
        Ok(list) => list,
        Err(..)  => return Err("Couldn't scan filesystem")
    };
    
    for path in path_list.filter(|path| path.is_file()) {        
        export_file(connection, &path);
    }

    Ok(())
}

fn hash_file(path: &Path) -> Result<String, &'static str> {
    let mut hasher = crypto::sha2::Sha256::new();
    
    let mut blocks = match Blocks::from_path(path, 1024) {
        Some(blocks) => blocks,
        None         => return Err("Couldn't read file")
    };
    
    loop {
        match blocks.next() {
            Some(slice) => hasher.input(slice),
            None        => break
        }
    }
    
    Ok(hasher.result_str())
}

fn file_known(connection: &SqliteConnection, path: &Path) -> Result<(bool, String), &'static str> {
    let hash: String = match hash_file(path) {
        Err(e)     => return Err(e),
        Ok(string) => string
    };

    let known = connection.query_row(
        "SELECT COUNT(id) FROM file
        WHERE hash = $1;",
        &[&hash.as_slice()],
        |row| row.get::<i64>(0) > 0
    );
    
    Ok((known, hash))
}

fn export_file(connection: &SqliteConnection, path: &Path) -> Result<(), &'static str> {
    let hash = match file_known(connection, path) {
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
    
    if !index_persist_file(connection, path, hash.as_slice(), block_id_list.as_slice()) {
        println!("Failed persisting file to database");
    }
    
    Ok(())
}

// FIXME: should return Result<(), &'static str> so we can more info what potentially went wrong
fn index_persist_file(connection: &SqliteConnection, path: &Path, hash: &str, block_id_list: &[uint]) -> bool {
    let filename_bytes = match path.filename() {
        None        => return false,
        Some(bytes) => bytes
    };

    let filename: String = String::from_utf8_lossy(filename_bytes).into_owned();

    // FIXME: this will probably change
    //let directory_id = match get_directory_id(path) {
    //    None     => return false,
    //    Some(id) => id
    //};    

    let transaction = match connection.transaction() {
        Ok(tx) => tx,
        Err(_) => return false
    };

    match connection.execute("INSERT INTO file (hash) VALUES ($1);", &[&hash]) {
        Err(_) => return false,
        Ok(..) => ()
    }
    
    let file_id = connection.last_insert_rowid();
    
    for (ordinal, block_id) in block_id_list.iter().enumerate() {
        let result = connection.execute(
            "INSERT INTO fileblock (file_id, block_id, ordinal) VALUES ($1, $2, $3);"
            , &[&(file_id as i64), &(*block_id as i64), &(ordinal as i64)]
        );
    
        match result {
            Err(_) => return false,
            Ok(..) => ()
        }
    }
    
    let alias_query = "INSERT INTO alias (directory_id, file_id, name, timestamp) VALUES ($1, $2, $3, $4);";
    let timestamp = time::get_time().sec;
    
    match connection.execute(alias_query, &[&types::Null, &(file_id as i64), &filename, &(timestamp as i64)]) {
        Err(e) => return false,
        Ok(..) => ()
    }

    transaction.commit().is_ok()
}

// FIXME: we probably shouldn't figure out directory for every single file.
// instead, keep track of directory when walking the filesystem and pass on the
// id of the working directory to fn_export_file
fn get_directory_id(path: &Path) -> Option<Directory> {
    let mut copy: Path = path.clone();
    
    if !copy.pop() {
        return Some(Directory::Root);
    }
    
    //TODO: implement
    
    None
}

fn hash_block(block: &[u8]) -> String {
    let mut hasher = crypto::sha2::Sha256::new();
    
    hasher.input(block);
    
    hasher.result_str()
}

fn block_known(connection: &SqliteConnection, hash: &str) -> bool {
    connection.query_row(
        "SELECT COUNT(id) FROM block
        WHERE hash = $1;",
        &[&hash],
        |row| row.get::<i64>(0) > 0
    )
}

fn export_block(connection: &SqliteConnection, block: &[u8]) -> Result<BlockExportResult, &'static str> {
    let hash = hash_block(block);

    if block_known(connection, hash.as_slice()) {
        return Ok(BlockExportResult::Known);
    }
    
    let bytes = match encrypt_block(block) {
        Some(vector) => vector,
        None         => return Err("Failed encrypting block")
    };
    
    match write_block(hash.as_slice(), bytes.as_slice()) {
        Err(_) => return Err("Failed writing block to disk"),
        Ok(..) => ()
    }
    
    match index_persist_block(connection, hash.as_slice()) {
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

fn index_persist_block(connection: &SqliteConnection, hash: &str) -> Option<uint> {
    let result = connection.execute("INSERT INTO block (hash) VALUES ($1);", &[&hash]);
    
    match result {
        Err(_) => None,
        Ok(..) => Some(connection.last_insert_rowid() as uint)
    }
}

fn encrypt_block(block: &[u8]) -> Option<Vec<u8>> {
    let mut encryptor: Box<symmetriccipher::Encryptor> = aes::cbc_encryptor(
        aes::KeySize::KeySize256,
        TEST_KEY.as_bytes(),
        &[],
        PkcsPadding
    );
    
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0, ..4096];
    let mut read_buffer = buffer::RefReadBuffer::new(block);
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);
    
    while !read_buffer.is_empty() {
        match encryptor.encrypt(&mut read_buffer, &mut write_buffer, true) {
            Err(_) => return None,
            Ok(..) => ()
        }
        
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
    }
    
    Some(final_result)
}

fn setup_database() -> Result<SqliteConnection, &'static str> {
    let connection = match create_database() {
        Ok(conn) => conn,
        Err(e)   => return Err(e)
    };

    if ! create_directory_table(&connection) {
        return Err("Couldn't create directory table")
    }
    
    if ! create_file_table(&connection) {
        return Err("Couldn't create file table")
    }
    
    if ! create_file_alias_table(&connection) {
        return Err("Couldn't create file alias table")
    }
    
    if ! create_block_table(&connection) {
        return Err("Couldn't create block table")
    }
    
    if ! create_file_block_table(&connection) {
        return Err("Couldn't create file<->block table")
    }
    
    Ok(connection)
}

fn create_directory_table(connection: &SqliteConnection) -> bool {
    connection
        .execute("CREATE TABLE directory (
            id        INTEGER PRIMARY KEY,
            parent_id INTEGER,
            name      TEXT NOT NULL,
            FOREIGN KEY(parent_id) REFERENCES directory(id)
        );", &[])
        .is_ok()
}

fn create_file_table(connection: &SqliteConnection) -> bool {
    connection
        .execute("CREATE TABLE file (
            id           INTEGER PRIMARY KEY,
            hash         TEXT NOT NULL
        );", &[])
        .is_ok()
}

fn create_file_alias_table(connection: &SqliteConnection) -> bool {
    connection
        .execute("CREATE TABLE alias (
            id           INTEGER PRIMARY KEY,
            directory_id INTEGER,
            file_id      INTEGER,
            name         TEXT NOT NULL,
            timestamp    INTEGER,
            FOREIGN KEY(directory_id) REFERENCES directory(id),
            FOREIGN KEY(file_id) REFERENCES file(id)
        );", &[])
        .is_ok()
}

fn create_block_table(connection: &SqliteConnection) -> bool {
    connection
        .execute("CREATE TABLE block (
            id           INTEGER PRIMARY KEY,
            hash         TEXT NOT NULL
        );", &[])
        .is_ok()
}

fn create_file_block_table(connection: &SqliteConnection) -> bool {
    connection
        .execute("CREATE TABLE fileblock (
            id           INTEGER PRIMARY KEY,
            file_id      INTEGER NOT NULL,
            ordinal      INTEGER NOT NULL,
            block_id     INTEGER NOT NULL,
            FOREIGN KEY(file_id) REFERENCES file(id),
            FOREIGN KEY(block_id) REFERENCES block(id)
        );", &[])
        .is_ok()
}

fn create_database() -> Result<SqliteConnection, &'static str> {
    let working_dir = Path::new(".");
    let database_path = Path::new(DATABASE_FILE);
    
    let file_list = match readdir(&working_dir) {
        Ok(list) => list,
        Err(_)   => return Err("Failed reading directory")
    };
    
    if file_list.into_iter().find(|path| *path == database_path).is_some() {
        return Err("Database already exists");
    }
    
    match SqliteConnection::open(DATABASE_FILE) {
        Ok(conn) => Ok(conn),
        Err(_)   => Err("Couldn't create database")
    }
}
