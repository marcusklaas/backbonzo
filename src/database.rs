extern crate time;

use super::rusqlite::{SqliteResult, SqliteError, SqliteConnection};
use super::{BonzoError, Directory};
use super::crypto;

pub fn persist_file(connection: &SqliteConnection, directory: Directory, filename: &str, hash: &str, block_id_list: &[uint]) -> SqliteResult<()> {
    let transaction = try!(connection.transaction());

    try!(connection.execute("INSERT INTO file (hash) VALUES ($1);", &[&hash]));
    
    let file_id = connection.last_insert_rowid();
    
    for (ordinal, block_id) in block_id_list.iter().enumerate() {
        try!(connection.execute(
            "INSERT INTO fileblock (file_id, block_id, ordinal) VALUES ($1, $2, $3);"
            , &[&(file_id as i64), &(*block_id as i64), &(ordinal as i64)]
        ));
    }
    
    let alias_query = "INSERT INTO alias (directory_id, file_id, name, timestamp) VALUES ($1, $2, $3, $4);";
    let timestamp = time::get_time().sec;
    let directory_id: Option<i64> = match directory {
        Directory::Root      => None,
        Directory::Child(id) => Some(id as i64)
    };
    
    try!(connection.execute(alias_query, &[&directory_id, &(file_id as i64), &filename, &(timestamp as i64)]));

    transaction.commit()
}

pub fn persist_block(connection: &SqliteConnection, hash: &str) -> SqliteResult<uint> {
    connection.execute("INSERT INTO block (hash) VALUES ($1);", &[&hash])
}

pub fn file_known(connection: &SqliteConnection, hash: &str) -> bool {
    connection.query_row(
        "SELECT COUNT(id) FROM file
        WHERE hash = $1;",
        &[&hash],
        |row| row.get::<i64>(0) > 0
    )
}

pub fn block_known(connection: &SqliteConnection, hash: &str) -> bool {
    connection.query_row(
        "SELECT COUNT(id) FROM block
        WHERE hash = $1;",
        &[&hash],
        |row| row.get::<i64>(0) > 0
    )
}

pub fn get_directory_id(connection: &SqliteConnection, parent: Directory, name: &str) -> SqliteResult<Directory> {
    let parent_id: Option<i64> = match parent {
        Directory::Root      => None,
        Directory::Child(id) => Some(id as i64)
    };

    // TODO: escape name!
    let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;"; 
    let directory_id: Option<i64> = connection.query_row(select_query, &[&name, &parent_id], |row| row.get(0));
    
    if directory_id.is_some() {
        return Ok(Directory::Child(directory_id.unwrap() as uint));
    }
    
    let insert_query = "INSERT INTO directory (parent_id, name) VALUES ($1, $2);";
    
    connection.execute(insert_query, &[&parent_id, &name])
        .map(|_| Directory::Child(connection.last_insert_rowid() as uint))
}

pub fn setup(connection: &SqliteConnection) -> SqliteResult<()> {
    try!(create_directory_table(connection));
    try!(create_file_table(connection));
    try!(create_file_alias_table(connection));
    try!(create_block_table(connection));
    try!(create_file_block_table(connection));
    
    Ok(())
}

fn create_directory_table(connection: &SqliteConnection) -> SqliteResult<uint> {
    connection.execute("CREATE TABLE directory (
        id        INTEGER PRIMARY KEY,
        parent_id INTEGER,
        name      TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES directory(id)
    );", &[])
}

fn create_file_table(connection: &SqliteConnection) -> SqliteResult<uint> {
    connection.execute("CREATE TABLE file (
        id           INTEGER PRIMARY KEY,
        hash         TEXT NOT NULL
    );", &[])
}

fn create_file_alias_table(connection: &SqliteConnection) -> SqliteResult<uint> {
    connection.execute("CREATE TABLE alias (
        id           INTEGER PRIMARY KEY,
        directory_id INTEGER,
        file_id      INTEGER,
        name         TEXT NOT NULL,
        timestamp    INTEGER,
        FOREIGN KEY(directory_id) REFERENCES directory(id),
        FOREIGN KEY(file_id) REFERENCES file(id)
    );", &[])
}

fn create_block_table(connection: &SqliteConnection) -> SqliteResult<uint> {
    connection.execute("CREATE TABLE block (
        id           INTEGER PRIMARY KEY,
        hash         TEXT NOT NULL
    );", &[])
}

fn create_file_block_table(connection: &SqliteConnection) -> SqliteResult<uint> {
    connection.execute("CREATE TABLE fileblock (
        id           INTEGER PRIMARY KEY,
        file_id      INTEGER NOT NULL,
        ordinal      INTEGER NOT NULL,
        block_id     INTEGER NOT NULL,
        FOREIGN KEY(file_id) REFERENCES file(id),
        FOREIGN KEY(block_id) REFERENCES block(id)
    );", &[])
}

pub fn create(filename: &str) -> SqliteResult<SqliteConnection> {
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
    
    SqliteConnection::open(filename)
}
