extern crate time;

use super::rusqlite::{SqliteError, SqliteConnection};
use super::Directory;

use super::crypto;

// FIXME: should return Result<(), &'static str> so we can more info what potentially went wrong
pub fn persist_file(connection: &SqliteConnection, directory: Directory, path: &Path, hash: &str, block_id_list: &[uint]) -> bool {
    let filename_bytes = match path.filename() {
        None        => return false,
        Some(bytes) => bytes
    };

    let filename: String = String::from_utf8_lossy(filename_bytes).into_owned();

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
    let directory_id: Option<i64> = match directory {
        Directory::Root      => None,
        Directory::Child(id) => Some(id as i64)
    };
    
    match connection.execute(alias_query, &[&directory_id, &(file_id as i64), &filename, &(timestamp as i64)]) {
        Err(e) => return false,
        Ok(..) => ()
    }

    transaction.commit().is_ok()
}

pub fn persist_block(connection: &SqliteConnection, hash: &str) -> Option<uint> {
    let result = connection.execute("INSERT INTO block (hash) VALUES ($1);", &[&hash]);
    
    match result {
        Err(_) => None,
        Ok(..) => Some(connection.last_insert_rowid() as uint)
    }
}

pub fn file_known(connection: &SqliteConnection, path: &Path) -> Result<(bool, String), &'static str> {
    let hash: String = match crypto::hash_file(path) {
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

pub fn block_known(connection: &SqliteConnection, hash: &str) -> bool {
    connection.query_row(
        "SELECT COUNT(id) FROM block
        WHERE hash = $1;",
        &[&hash],
        |row| row.get::<i64>(0) > 0
    )
}

pub fn get_directory_id(connection: &SqliteConnection, parent: Directory, name: &str) -> Result<Directory, &'static str> {
    let parent_id: Option<i64> = match parent {
        Directory::Root      => None,
        Directory::Child(id) => Some(id as i64)
    };

    // TODO: escape name!
    let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;"; 
    let directory_id: Option<i64> = connection.query_row(select_query, &[&name, &parent_id], |row| row.get(0));
    
    match directory_id {
        Some(id) => return Ok(Directory::Child(id as uint)),
        None     => ()
    }
    
    let insert_query = "INSERT INTO directory (parent_id, name) VALUES ($1, $2);";
    
    match connection.execute(insert_query, &[&parent_id, &name]) {
        Ok(..) => Ok(Directory::Child(connection.last_insert_rowid() as uint)),
        Err(_) => Err("Failed persisting new directory to database")
    }
}

pub fn setup(path: &Path) -> Result<SqliteConnection, &'static str> {
    let connection = try!(create(path));

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

pub fn create(database_path: &Path) -> Result<SqliteConnection, &'static str> {
    //let working_directory = database_path.clone();
    
    //if !working_directory.pop() {
    //    return Err("Received incorrect database location");
    //}
    
    //let file_list = match readdir(&working_dir) {
    //    Ok(list) => list,
    //    Err(_)   => return Err("Failed reading directory")
    //};
    
    //if file_list.into_iter().find(|path| *path == database_path).is_some() {
    //    return Err("Database already exists");
    //}
    
    let filename = match database_path.as_str() {
        None    => return Err("Couldn't convert database path to string"),
        Some(s) => s
    }; 
    
    match SqliteConnection::open(filename) {
        Ok(conn) => Ok(conn),
        Err(_)   => Err("Couldn't create database")
    }
}
