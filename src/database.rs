extern crate rusqlite;
extern crate libc;

use self::rusqlite::{SqliteResult, SqliteConnection, SqliteRow, SqliteOpenFlags, SQLITE_OPEN_FULL_MUTEX, SQLITE_OPEN_READ_WRITE, SQLITE_OPEN_CREATE};
use self::rusqlite::types::{FromSql, ToSql};
use self::rusqlite::ffi::sqlite3_stmt;
use self::libc::c_int;

use {BonzoResult, BonzoError, Directory};

use std::io::TempDir;
use std::io::fs::{File, PathExtensions};
use std::collections::HashSet;
use std::iter::FromIterator;
use super::rustc_serialize::hex::{ToHex, FromHex};

pub use self::rusqlite::SqliteError;

impl ToSql for Directory {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let i = match *self {
            Directory::Root     => 0,
            Directory::Child(i) => i
        };

        i.bind_parameter(stmt, col)
    }
}

impl FromSql for Directory {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> SqliteResult<Directory> {
        FromSql::column_result(stmt, col).map(|i| {
            match i {
                0 => Directory::Root,
                v => Directory::Child(v)
            }
        })
    }
}

// TODO: abstractify database error
// TODO: use references to Database instead of the value itself, so we may remove Copy trait?

// An iterator over files in a state determined by the given timestamp. A file
// is represented by its path and a list of block id's. 
pub struct Aliases<'a> {
    database: &'a Database,
    path: Path,
    timestamp: u64,
    file_list: Vec<(u32, String)>,
    directory_list: Vec<Directory>,
    subdirectory: Option<Box<Aliases<'a>>>
}

impl<'a> Aliases<'a> {
    pub fn new(database: &'a Database, path: Path, directory: Directory, timestamp: u64) -> SqliteResult<Aliases<'a>> {
        Ok(Aliases {
            database: database,
            path: path,
            timestamp: timestamp,
            file_list: try!(database.get_directory_content_at(directory, timestamp)),
            directory_list: try!(database.get_subdirectories(directory)),
            subdirectory: None
        })
    }
}

impl<'a> Iterator for Aliases<'a> {
    type Item = (Path, Vec<u32>);
    
    fn next(&mut self) -> Option<(Path, Vec<u32>)> {
        // return file from child directory
        loop {
            if let Some(ref mut dir) = self.subdirectory {
                if let Some(alias) = dir.next() {
                    return Some(alias);
                }
            }

            match self.directory_list.pop() {
                None     => break,
                Some(id) => self.subdirectory =
                    self.database.get_directory_name(id).and_then(|directory_name| {
                        Aliases::new(
                            self.database,
                            self.path.join(directory_name),
                            id,
                            self.timestamp
                        )
                    }).ok().map(|alias| Box::new(alias))
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

// TODO: can we not make database a trait and let it be implemented using sqlite?
// also, should we have one monolytic class which contains all queries?
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

    fn query_and_collect<T, F, C>(&self, sql: &str, params: &[&ToSql], f: F) -> SqliteResult<C>
                                  where F: Fn(SqliteRow) -> T,
                                        C: FromIterator<T> {
        let mut statement = try!(self.connection.prepare(sql));
        
        statement
            .query(params)
            .and_then(|rows| {
                rows
                    .map(|possible_row| {
                        possible_row.map(|row| {
                            f(row)
                        })
                    })
                    .collect()
            })
    }

    pub fn get_path<'a>(&'a self) -> &'a Path {
        &self.path
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

        Ok(try!(File::open(&self.path).and_then(|mut file| file.read_to_end())))
    }

    pub fn get_subdirectories(&self, directory: Directory) -> SqliteResult<Vec<Directory>> {
        self.query_and_collect(
            "SELECT id FROM directory WHERE parent_id = $1;",
            &[&directory],
            |result| result.get(0)
        )
    }

    pub fn get_directory_content_at(&self, directory: Directory, timestamp: u64) -> SqliteResult<Vec<(u32, String)>> {
        self.query_and_collect(
            "SELECT alias.file_id, alias.name FROM alias
            INNER JOIN (SELECT MAX(id) AS max_id FROM alias WHERE directory_id = $1 AND timestamp <= $2 GROUP BY name) a ON alias.id = a.max_id
            WHERE file_id IS NOT NULL;",
            &[&directory, &(timestamp as i64)],
            |row| (row.get::<i64>(0) as u32, row.get(1))
        )
    }

    pub fn get_directory_filenames(&self, directory: Directory) -> SqliteResult<HashSet<String>> {
        self.query_and_collect(
            "SELECT alias.name FROM alias
            INNER JOIN (SELECT MAX(id) AS max_id FROM alias WHERE directory_id = $1 GROUP BY name) a ON alias.id = a.max_id
            WHERE file_id IS NOT NULL;",
            &[&directory],
            |row| row.get(0)
        )
    }

    fn get_directory_name(&self, directory: Directory) -> SqliteResult<String> {
        self.connection.query_row_safe(
            "SELECT name FROM directory WHERE id = $1;",
            &[&directory],
            |row| row.get::<String>(0)
        )
    }

    fn get_file_block_list(&self, file_id: u32) -> SqliteResult<Vec<u32>> {
        self.query_and_collect(
            "SELECT block_id FROM fileblock WHERE file_id = $1 ORDER BY ordinal ASC;",
            &[&(file_id as i64)],
            |row| row.get::<i64>(0) as u32
        )
    }

    pub fn persist_file(&self, directory: Directory, filename: &str, hash: &str, last_modified: u64, block_id_list: &[u32]) -> SqliteResult<()> {
        let transaction = try!(self.connection.transaction());

        try!(self.connection.execute("INSERT INTO file (hash) VALUES ($1);", &[&hash]));
        
        let file_id = self.connection.last_insert_rowid();
        
        for (ordinal, block_id) in block_id_list.iter().enumerate() {
            try!(self.connection.execute(
                "INSERT INTO fileblock (file_id, block_id, ordinal) VALUES ($1, $2, $3);"
                , &[&(file_id as i64), &(*block_id as i64), &(ordinal as i64)]
            ));
        }
        
        try!(self.persist_alias(directory, Some(file_id as u32), filename, last_modified));

        transaction.commit()
    }

    pub fn persist_alias(&self, directory: Directory, file_id: Option<u32>, filename: &str, last_modified: u64) -> SqliteResult<()> {
        let signed_file_id = file_id.map(|unsigned| unsigned as i64);
        
        self.connection.execute(
            "INSERT INTO alias (directory_id, file_id, name, timestamp) VALUES ($1, $2, $3, $4);",
            &[&directory, &signed_file_id, &filename, &(last_modified as i64)]
        ).map(|_|())
    }

    pub fn persist_null_alias(&self, directory: Directory, filename: &str) -> BonzoResult<()> {    
        Ok(try!(self.persist_alias(directory, None, filename, try!(get_filesystem_time()))))
    }

    pub fn persist_block(&self, hash: &str, iv: &[u8]) -> SqliteResult<u32> {
        try!(self.connection.execute(
            "INSERT INTO block (hash, iv_hex) VALUES ($1, $2);",
            &[&hash, &iv.to_hex().as_slice()]
        ));

        Ok(self.connection.last_insert_rowid() as u32)
    }

    pub fn file_from_hash(&self, hash: &str) -> SqliteResult<Option<u32>> {
        self.connection.query_row_safe(
            "SELECT SUM(id) FROM file WHERE hash = $1;",
            &[&hash],
            |row| row.get::<Option<i64>>(0).map(|signed| signed as u32)
        )
    }

    pub fn alias_known(&self, directory: Directory, filename: &str, timestamp: u64) -> SqliteResult<bool> {
        self.connection.query_row_safe(
            "SELECT COUNT(alias.id) FROM alias
            INNER JOIN (SELECT MAX(id) AS max_id FROM alias WHERE directory_id = $1 AND name = $2) a ON alias.id = a.max_id
            WHERE timestamp >= $3 AND file_id IS NOT NULL;",
            &[&directory, &filename, &(timestamp as i64)],
            |row| row.get::<i64>(0) > 0
        )
    }

    pub fn block_from_id(&self, id: u32) -> BonzoResult<(String, Vec<u8>)> {
        let (hash, iv_hex) = try!(self.connection.query_row_safe(
            "SELECT hash, iv_hex FROM block WHERE id = $1;",
            &[&(id as i64)],
            |row| (row.get::<String>(0), row.get::<String>(1))
        ));

        match iv_hex.as_slice().from_hex() {
            Ok(iv)  => Ok((hash, iv)),
            Err(..) => Err(BonzoError::Other(format!("Couldn't parse hex")))
        }
    }

    pub fn block_id_from_hash(&self, hash: &str) -> SqliteResult<Option<u32>> {
        self.connection.query_row_safe(
            "SELECT SUM(id) FROM block WHERE hash = $1;",
            &[&hash],
            |&: row: SqliteRow| {
                row.get::<Option<i64>>(0).map(|&: signed: i64| {
                    signed as u32
                })
            }
        )
    }

    pub fn get_directory(&self, parent: Directory, name: &str) -> SqliteResult<Directory> {
        let possible_directory: Option<Directory> = try!({
            let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;"; 
            self.connection.query_row_safe(select_query, &[&name, &parent], |row| row.get(0))
        });
        
        if let Some(directory) = possible_directory {
            return Ok(directory);
        }
        
        let insert_query = "INSERT INTO directory (parent_id, name) VALUES ($1, $2);";
        
        self.connection.execute(insert_query, &[&parent, &name])
            .and(Ok(Directory::Child(self.connection.last_insert_rowid())))
    }

    pub fn set_key(&self, key: &str, value: &str) -> SqliteResult<i32> {
        self.connection.execute("INSERT INTO setting (key, value) VALUES ($1, $2);", &[&key, &value])
    }

    pub fn get_key(&self, key: &str) -> SqliteResult<Option<String>> {
        self.connection.query_row_safe(
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
            "CREATE INDEX alias_directory_index ON alias (directory_id)",
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
        ]
            .iter()
            .map(|query| self.connection.execute(*query, &[]))
            .fold(Ok((0)), |a, b| a.and(b))
            .map(|_|())
    }
}

/* FIXME: we can probably use the time crate for this */
fn get_filesystem_time() -> BonzoResult<u64> {
    let temp_directory = try!(TempDir::new("bbtime"));

    Ok(try!(temp_directory.path().stat()).modified)
}

#[cfg(test)]
mod test {
    use std::io::TempDir;
    use Directory;

    #[test]
    fn directory_queries() {
        let temp = TempDir::new("query-collect").unwrap();
        let path = temp.path().join("index.db3");
        let db = super::Database::create(path).unwrap();
        let _ = db.setup().unwrap();
        
        let child1 = db.get_directory(Directory::Root, "child1").unwrap();
        let child2 = db.get_directory(Directory::Root, "child2").unwrap();
        let grand_child = db.get_directory(child1, "grand child1").unwrap();

        let child1_copy = db.get_directory(Directory::Root, "child1").unwrap();

        assert_eq!(child1, child1_copy);

        let children = db.get_subdirectories(Directory::Root).unwrap();

        assert!(children.len() == 2);
        assert!(children.iter().any(|x| *x == child1));
        assert!(children.iter().any(|x| *x == child2));

        let grand_children = db.get_subdirectories(child1).unwrap();

        assert_eq!(grand_children[0], grand_child);

        let great_grand_children = db.get_subdirectories(grand_child).unwrap();

        assert_eq!(0us, great_grand_children.len());
    }
}
