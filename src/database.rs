use super::rusqlite::{SqliteResult, SqliteConnection};
use super::Directory;
use serialize::hex::{ToHex, FromHex};
use super::{BonzoResult, BonzoError};

pub struct Aliases<'a> {
    connection: &'a SqliteConnection,
    path: Path,
    timestamp: u64,
    file_list: Vec<(uint, String)>,
    directory_id_list: Vec<uint>,
    subdirectory: Option<Box<Aliases<'a>>>
}

impl<'a> Aliases<'a> {
    pub fn new(connection: &'a SqliteConnection, path: Path, directory: Directory, timestamp: u64) -> SqliteResult<Aliases<'a>> {
        let mut row_statement = match directory {
            Directory::Child(..) => try!(connection.prepare("SELECT MAX(id) FROM alias WHERE directory_id = $1 AND timestamp <= $2 GROUP BY name;")),
            Directory::Root      => try!(connection.prepare("SELECT MAX(id) FROM alias WHERE directory_id IS NULL AND timestamp <= $1 GROUP BY name;"))
        };

        let rows = match directory {
            Directory::Child(id) => try!(row_statement.query(&[&(id as i64), &(timestamp as i64)])),
            Directory::Root      => try!(row_statement.query(&[&(timestamp as i64)]))
        };

        let mut directory_statement = match directory {
            Directory::Child(..) => try!(connection.prepare("SELECT id FROM directory WHERE parent_id = $1;")),
            Directory::Root      => try!(connection.prepare("SELECT id FROM directory WHERE parent_id IS NULL;"))
        };

        let directories = match directory {
            Directory::Child(id) => try!(directory_statement.query(&[&(id as i64)])),
            Directory::Root      => try!(directory_statement.query(&[]))
        };

        let alias_id_list: Vec<uint> = try!(rows.map(|row| row.map(|r| r.get::<i64>(0) as uint)).collect()); // TODO: this collect should be unnecessary
        let directory_id_list: Vec<uint> = try!(directories.map(|dir| dir.map(|d| d.get::<i64>(0) as uint)).collect()); // FIXME: duplicate code!

        let file_list: Vec<(uint, String)> = alias_id_list
            .iter()
            .filter_map(|id| alias_to_file(connection, *id))
            .collect();
        
        Ok(Aliases {
            connection: connection,
            path: path,
            timestamp: timestamp,
            file_list: file_list,
            directory_id_list: directory_id_list,
            subdirectory: None
        })
    }
}

impl<'a> Iterator<(Path, Box<[uint]>)> for Aliases<'a> {
    fn next(&mut self) -> Option<(Path, Box<[uint]>)> {
        // return file from child directory
        loop {
            if let Some(ref mut dir) = self.subdirectory {
                if let Some(alias) = dir.next() {
                    return Some(alias);
                }
            }

            match self.directory_id_list.pop() {
                None     => break,
                Some(id) =>
                    self.subdirectory = Aliases::new(
                        self.connection,
                        self.path.join(get_directory_name(self.connection, id)),
                        Directory::Child(id),
                        self.timestamp
                    ).ok().map(|alias| box alias)
            }
        }

        // return file from current directory
        self.file_list.pop().and_then(|(id, name)|
            get_file_block_list(self.connection, id).ok().map(|boxed_slice|
                (self.path.join(name.clone()), boxed_slice)
        ))
    }
}

fn get_directory_name(connection: &SqliteConnection, directory_id: uint) -> String {
    connection.query_row(
        "SELECT name FROM directory WHERE id = $1;",
        &[&(directory_id as i64)],
        |row| row.get::<String>(0)
    )
}

fn alias_to_file(connection: &SqliteConnection, alias_id: uint) -> Option<(uint, String)> {
    connection.query_row(
        "SELECT file_id, name FROM alias WHERE id = $1;",
        &[&(alias_id as i64)],
        |row| row.get::<Option<i64>>(0).map(|id| (id as uint, row.get::<String>(1)))
    )
}

fn get_file_block_list(connection: &SqliteConnection, file_id: uint) -> SqliteResult<Box<[uint]>> {
    let mut statement = try!(connection.prepare("SELECT block_id FROM fileblock WHERE file_id = $1 ORDER BY ordinal ASC;"));
    
    try!(statement.query(&[&(file_id as i64)]))
        .map(|row_result| row_result.map(|row| row.get::<i64>(0) as uint))
        .collect::<SqliteResult<Vec<uint>>>()
        .map(|vec| vec.into_boxed_slice())
}

pub fn persist_file(connection: &SqliteConnection, directory: Directory, filename: &str, hash: &str, last_modified: u64, block_id_list: &[uint]) -> SqliteResult<()> {
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
    
    try!(connection.execute(alias_query, &[&directory.into_option(), &(file_id as i64), &filename, &(last_modified as i64)]));

    transaction.commit()
}

pub fn persist_block(connection: &SqliteConnection, hash: &str, iv: &[u8]) -> SqliteResult<uint> {
    try!(connection.execute(
        "INSERT INTO block (hash, iv_hex) VALUES ($1, $2);",
        &[&hash, &iv.to_hex().as_slice()]
    ));

    Ok(connection.last_insert_rowid() as uint)
}

pub fn file_known(connection: &SqliteConnection, hash: &str) -> bool {
    connection.query_row(
        "SELECT COUNT(id) FROM file WHERE hash = $1;",
        &[&hash],
        |row| row.get::<i64>(0) > 0
    )
}

/* TODO: we may want to further normalize database. otherwise, put some indices on these tables */
pub fn alias_known(connection: &SqliteConnection, directory: Directory, filename: &str, timestamp: u64) -> bool {
    match directory.into_option() {
        Some(id) => connection.query_row(
            "SELECT COUNT(id) FROM alias WHERE directory_id = $1 AND name = $2 AND timestamp >= $3;",
            &[&id, &filename, &(timestamp as i64)],
            |row| row.get::<i64>(0) > 0
        ),
        None => connection.query_row(
            "SELECT COUNT(id) FROM alias WHERE directory_id IS NULL AND name = $2 AND timestamp >= $3;",
            &[&filename, &(timestamp as i64)],
            |row| row.get::<i64>(0) > 0
        )
    }
}

pub fn block_from_id(connection: &SqliteConnection, id: uint) -> BonzoResult<(String, Vec<u8>)> {
    connection.query_row(
        "SELECT hash, iv_hex FROM block WHERE id = $1;",
        &[&(id as i64)],
        |row| match row.get::<String>(1).as_slice().from_hex() {
            Ok(vec) => Ok((row.get(0), vec)),
            Err(..) => Err(BonzoError::Other(format!("Couldn't parse hex")))
        }
    )
}

pub fn block_id_from_hash(connection: &SqliteConnection, hash: &str) -> Option<uint> {
    connection.query_row::<Option<i64>>(
        "SELECT SUM(id) FROM block WHERE hash = $1;",
        &[&hash],
        |row| row.get(0)
    ).map(|signed| signed as uint)
}

pub fn get_directory(connection: &SqliteConnection, parent: Directory, name: &str) -> SqliteResult<Directory> {
    let parent_id = parent.into_option();
    
    let directory_id: Option<i64> = match parent_id {
        Some(parent) => {
            let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;"; 
            connection.query_row(select_query, &[&name, &parent], |row| row.get(0))
        },
        None => {
            let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id IS NULL;"; 
            connection.query_row(select_query, &[&name], |row| row.get(0))
        }
    };
    
    if directory_id.is_some() {
        return Ok(Directory::Child(directory_id.unwrap() as uint));
    }
    
    let insert_query = "INSERT INTO directory (parent_id, name) VALUES ($1, $2);";
    
    connection.execute(insert_query, &[&parent_id, &name])
        .and(Ok(Directory::Child(connection.last_insert_rowid() as uint)))
}

pub fn set_key(connection: &SqliteConnection, key: &str, value: &str) -> SqliteResult<uint> {
    connection.execute("INSERT INTO setting (key, value) VALUES ($1, $2);", &[&key, &value])
}

pub fn get_key(connection: &SqliteConnection, key: &str) -> Option<String> {
    connection.query_row(
        "SELECT value FROM setting WHERE key = $1;",
        &[&key],
        |row| row.get(0)
    )
}

pub fn setup(connection: &SqliteConnection) -> SqliteResult<()> {
    let queries = [
        "CREATE TABLE directory (
            id        INTEGER PRIMARY KEY,
            parent_id INTEGER,
            name      TEXT NOT NULL,
            FOREIGN KEY(parent_id) REFERENCES directory(id),
            UNIQUE(parent_id, name)
        );",
        "CREATE TABLE file (
            id           INTEGER PRIMARY KEY,
            hash         TEXT NOT NULL
        );",
        "CREATE INDEX file_hash_index on file (hash)",
        "CREATE TABLE alias (
            id           INTEGER PRIMARY KEY,
            directory_id INTEGER,
            file_id      INTEGER,
            name         TEXT NOT NULL,
            timestamp    INTEGER,
            FOREIGN KEY(directory_id) REFERENCES directory(id),
            FOREIGN KEY(file_id) REFERENCES file(id)
        );",
        "CREATE TABLE block (
            id           INTEGER PRIMARY KEY,
            hash         TEXT NOT NULL,
            iv_hex       TEXT NOT NULL
        );",
        "CREATE INDEX block_hash_index on block (hash)",
        "CREATE TABLE fileblock (
            id           INTEGER PRIMARY KEY,
            file_id      INTEGER NOT NULL,
            ordinal      INTEGER NOT NULL,
            block_id     INTEGER NOT NULL,
            FOREIGN KEY(file_id) REFERENCES file(id),
            FOREIGN KEY(block_id) REFERENCES block(id)
        );",
        "CREATE TABLE setting (
            key          TEXT PRIMARY KEY,
            value        TEXT
        );"
    ];

    for query in queries.iter() {
        try!(connection.execute(*query, &[]));
    }
        
    Ok(())
}
