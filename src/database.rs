use super::rusqlite::{SqliteResult, SqliteConnection, SqliteRow, SqliteOpenFlags, SQLITE_OPEN_FULL_MUTEX, SQLITE_OPEN_READ_WRITE, SQLITE_OPEN_CREATE};
use std::io::TempDir;
use std::io::fs::{File, PathExtensions};
use serialize::hex::{ToHex, FromHex};
use std::collections::HashSet;
use super::{BonzoResult, BonzoError};

pub struct Aliases<'a> {
    database: &'a Database,
    path: Path,
    timestamp: u64,
    file_list: Vec<(uint, String)>,
    directory_id_list: Vec<uint>,
    subdirectory: Option<Box<Aliases<'a>>>
}

impl<'a> Aliases<'a> {
    pub fn new(database: &'a Database, path: Path, directory_id: uint, timestamp: u64) -> SqliteResult<Aliases<'a>> {
        Ok(Aliases {
            database: database,
            path: path,
            timestamp: timestamp,
            file_list: try!(database.get_directory_content_at(directory_id, timestamp)),
            directory_id_list: try!(database.get_subdirectories(directory_id)),
            subdirectory: None
        })
    }
}

impl<'a> Iterator<(Path, Vec<uint>)> for Aliases<'a> {
    fn next(&mut self) -> Option<(Path, Vec<uint>)> {
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
                        self.database,
                        self.path.join(self.database.get_directory_name(id)),
                        id,
                        self.timestamp
                    ).ok().map(|alias| box alias)
            }
        }

        // return file from current directory
        self.file_list.pop().and_then(|(id, name)|
            self.database.get_file_block_list(id).ok().map(|block_list|
                (self.path.join(name.as_slice()), block_list)
        ))
    }
}

pub struct Database {
    connection: SqliteConnection,
    path: Path
}

impl Database {
    fn new(path: Path, flags: SqliteOpenFlags) -> BonzoResult<Database> {
        Ok(Database {
            connection: try!(path
                .as_str()
                .ok_or(BonzoError::Other(format!("Couldn't convert database path to string")))
                .and_then(|filename|
                    Ok(try!(SqliteConnection::open_with_flags(filename, flags)))
                )),
            path: path
        })
    }

    pub fn try_clone(&self) -> BonzoResult<Database> {
        Database::from_file(self.path.clone())
    }
    
    pub fn from_file(path: Path) -> BonzoResult<Database> {
        Database::new(path, SQLITE_OPEN_FULL_MUTEX | SQLITE_OPEN_READ_WRITE)
    }

    pub fn create(path: Path) -> BonzoResult<Database> {
        if path.exists() {
            return Err(BonzoError::Other(format!("Database file already exists"))); 
        }
        
        Database::new(path, SQLITE_OPEN_FULL_MUTEX | SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE)
    }

    pub fn to_bytes(self) -> BonzoResult<Vec<u8>> {
        try!(self.connection.close());

        let mut file = File::open(&self.path);

        Ok(try!(file.read_to_end()))
    }

    pub fn get_subdirectories(&self, directory_id: uint) -> SqliteResult<Vec<uint>> {
        let mut directory_statement = try!(self.connection.prepare("SELECT id FROM directory WHERE parent_id = $1;"));
        
        let directories = try!(directory_statement.query(&[&(directory_id as i64)]));

        directories.map(extract_uint).collect()
    }

    pub fn get_directory_content_at(&self, directory_id: uint, timestamp: u64) -> SqliteResult<Vec<(uint, String)>> {
        let mut statement = try!(self.connection.prepare(
            "SELECT alias.file_id, alias.name FROM alias
            INNER JOIN (SELECT MAX(id) FROM alias WHERE directory_id = $1 AND timestamp <= $2 GROUP BY name) a ON alias.id = a.max_id
            WHERE file_id IS NOT NULL;"
        ));
        
        statement
            .query(&[&(directory_id as i64), &(timestamp as i64)])
            .and_then(|result| result
                .map(|row_result| row_result
                    .map(|row| (row.get::<i64>(0) as uint, row.get(1)))
                ).collect()
            )
    }

    pub fn get_directory_filenames(&self, directory_id: uint) -> SqliteResult<HashSet<String>> {
        let mut statement = try!(self.connection.prepare(
            "SELECT alias.name FROM alias
            INNER JOIN (SELECT MAX(id) AS max_id FROM alias WHERE directory_id = $1 GROUP BY name) a ON alias.id = a.max_id
            WHERE file_id IS NOT NULL;"
        ));
        let filenames = try!(statement.query(&[&(directory_id as i64)]));

        filenames.map(|row_result| row_result.map(|row| row.get::<String>(0))).collect()
    }

    fn get_directory_name(&self, directory_id: uint) -> String {
        self.connection.query_row(
            "SELECT name FROM directory WHERE id = $1;",
            &[&(directory_id as i64)],
            |row| row.get::<String>(0)
        )
    }

    fn get_file_block_list(&self, file_id: uint) -> SqliteResult<Vec<uint>> {
        let mut statement = try!(self.connection.prepare("SELECT block_id FROM fileblock WHERE file_id = $1 ORDER BY ordinal ASC;"));
        
        try!(statement.query(&[&(file_id as i64)]))
            .map(extract_uint)
            .collect()
    }

    pub fn persist_file(&self, directory_id: uint, filename: &str, hash: &str, last_modified: u64, block_id_list: &[uint]) -> SqliteResult<()> {
        let transaction = try!(self.connection.transaction());

        try!(self.connection.execute("INSERT INTO file (hash) VALUES ($1);", &[&hash]));
        
        let file_id = self.connection.last_insert_rowid();
        
        for (ordinal, block_id) in block_id_list.iter().enumerate() {
            try!(self.connection.execute(
                "INSERT INTO fileblock (file_id, block_id, ordinal) VALUES ($1, $2, $3);"
                , &[&(file_id as i64), &(*block_id as i64), &(ordinal as i64)]
            ));
        }
        
        try!(self.persist_alias(directory_id, Some(file_id as uint), filename, last_modified));

        transaction.commit()
    }

    pub fn persist_alias(&self, directory_id: uint, file_id: Option<uint>, filename: &str, last_modified: u64) -> SqliteResult<()> {
        let signed_file_id = file_id.map(|unsigned| unsigned as i64);
        
        self.connection.execute(
            "INSERT INTO alias (directory_id, file_id, name, timestamp) VALUES ($1, $2, $3, $4);",
            &[&(directory_id as i64), &signed_file_id, &filename, &(last_modified as i64)]
        ).map(|_|())
    }

    pub fn persist_null_alias(&self, directory_id: uint, filename: &str) -> BonzoResult<()> {    
        Ok(try!(self.persist_alias(directory_id, None, filename, try!(get_filesystem_time()))))
    }

    pub fn persist_block(&self, hash: &str, iv: &[u8]) -> SqliteResult<uint> {
        try!(self.connection.execute(
            "INSERT INTO block (hash, iv_hex) VALUES ($1, $2);",
            &[&hash, &iv.to_hex().as_slice()]
        ));

        Ok(self.connection.last_insert_rowid() as uint)
    }

    pub fn file_from_hash(&self, hash: &str) -> Option<uint> {
        self.connection.query_row(
            "SELECT SUM(id) FROM file WHERE hash = $1;",
            &[&hash],
            |row| row.get::<Option<i64>>(0).map(|signed| signed as uint)
        )
    }

    /* TODO: we may want to further normalize database. otherwise, put some indices on these tables */
    pub fn alias_known(&self, directory_id: uint, filename: &str, timestamp: u64) -> bool {
        self.connection.query_row(
            "SELECT COUNT(alias.id) FROM alias
            INNER JOIN (SELECT MAX(id) AS max_id FROM alias WHERE directory_id = $1 AND name = $2) a ON alias.id = a.max_id
            WHERE timestamp >= $3 AND file_id IS NOT NULL;",
            &[&(directory_id as i64), &filename, &(timestamp as i64)],
            |row| row.get::<i64>(0) > 0
        )
    }

    pub fn block_from_id(&self, id: uint) -> BonzoResult<(String, Vec<u8>)> {
        self.connection.query_row(
            "SELECT hash, iv_hex FROM block WHERE id = $1;",
            &[&(id as i64)],
            |row| match row.get::<String>(1).as_slice().from_hex() {
                Ok(vec) => Ok((row.get(0), vec)),
                Err(..) => Err(BonzoError::Other(format!("Couldn't parse hex")))
            }
        )
    }

    pub fn block_id_from_hash(&self, hash: &str) -> Option<uint> {
        self.connection.query_row::<Option<i64>>(
            "SELECT SUM(id) FROM block WHERE hash = $1;",
            &[&hash],
            |row| row.get(0)
        ).map(|signed| signed as uint)
    }

    pub fn get_directory(&self, parent: uint, name: &str) -> SqliteResult<uint> {
        let directory_id: Option<i64> = {
            let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;"; 
            self.connection.query_row(select_query, &[&name, &(parent as i64)], |row| row.get(0))
        };
        
        if let Some(id) = directory_id {
            return Ok(id as uint);
        }
        
        let insert_query = "INSERT INTO directory (parent_id, name) VALUES ($1, $2);";
        
        self.connection.execute(insert_query, &[&(parent as i64), &name])
            .and(Ok(self.connection.last_insert_rowid() as uint))
    }

    pub fn set_key(&self, key: &str, value: &str) -> SqliteResult<uint> {
        self.connection.execute("INSERT INTO setting (key, value) VALUES ($1, $2);", &[&key, &value])
    }

    pub fn get_key(&self, key: &str) -> Option<String> {
        self.connection.query_row(
            "SELECT value FROM setting WHERE key = $1;",
            &[&key],
            |row| row.get(0)
        )
    }

    pub fn setup(&self) -> SqliteResult<()> {
        [
            "CREATE TABLE directory (
                id        INTEGER PRIMARY KEY,
                parent_id INTEGER,
                name      TEXT NOT NULL,
                FOREIGN KEY(parent_id) REFERENCES directory(id),
                UNIQUE(parent_id, name)
            );",
            "INSERT INTO directory (id, name) VALUES (0, \".\");",
            "CREATE TABLE file (
                id           INTEGER PRIMARY KEY,
                hash         TEXT NOT NULL
            );",
            "CREATE INDEX file_hash_index ON file (hash)",
            "CREATE TABLE alias (
                id           INTEGER PRIMARY KEY,
                directory_id INTEGER NOT NULL,
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
            "CREATE INDEX block_hash_index ON block (hash)",
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
        ].iter().map(|query| self.connection.execute(*query, &[])).fold(Ok((0)), |a, b| a.and(b)).map(|_|())
    }
}

/* FIXME: we can probably use the time crate for this */
fn get_filesystem_time() -> BonzoResult<u64> {
    let temp_directory = try!(TempDir::new("bbtime"));

    Ok(try!(temp_directory.path().stat()).modified)
}

fn extract_uint(row: SqliteResult<SqliteRow>) -> SqliteResult<uint> {
    row.map(|result| result.get::<i64>(0) as uint)
}
