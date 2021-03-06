extern crate rusqlite;
extern crate libc;
extern crate libsqlite3_sys as libsqlite;

use ::{epoch_milliseconds, Directory};
use ::error::{BonzoResult, BonzoError};
use ::{BlockId, FileId};
use ::itertools::Itertools;

use self::rusqlite::{SqliteResult, SqliteConnection, SqliteRow, SqliteOpenFlags,
                     SQLITE_OPEN_FULL_MUTEX, SQLITE_OPEN_READ_WRITE, SQLITE_OPEN_CREATE};
use self::rusqlite::types::{FromSql, ToSql};
use self::libc::c_int;

use std::io::Read;
use std::fs::File;
use std::path::PathBuf;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::error::Error;
use std::convert::From;
use std::fmt;

pub struct DatabaseError {
    description: String,
    cause: Option<Box<Error>>,
}

impl Error for DatabaseError {
    fn description(&self) -> &str {
        &self.description
    }

    fn cause(&self) -> Option<&Error> {
        match self.cause {
            Some(ref b) => Some(&**b),
            None => None,
        }
    }
}

impl From<SqliteError> for DatabaseError {
    fn from(error: SqliteError) -> DatabaseError {
        DatabaseError { description: error.description().to_string(), cause: Some(Box::new(error)) }
    }
}

impl fmt::Debug for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

unsafe impl Send for DatabaseError { }

pub type DatabaseResult<T> = Result<T, DatabaseError>;

use self::rusqlite::SqliteError;

macro_rules! impl_from_to_sql (
    ($t: ident) => (
        impl FromSql for $t {
            unsafe fn column_result(stmt: *mut libsqlite::sqlite3_stmt,
                                    col: c_int)
                                    -> SqliteResult<$t> {
                <i64 as FromSql>::column_result(stmt, col).map(|i| $t(i as u64))
            }
        }

        impl ToSql for $t {
            unsafe fn bind_parameter(&self,
                                     stmt: *mut libsqlite::sqlite3_stmt,
                                     col: c_int)
                                     -> c_int {
                let i = self.0 as i64;

                i.bind_parameter(stmt, col)
            }
        }
    )
);

impl_from_to_sql!(FileId);
impl_from_to_sql!(BlockId);

// TODO: this should be easier now
impl ToSql for Directory {
    unsafe fn bind_parameter(&self, stmt: *mut libsqlite::sqlite3_stmt, col: c_int) -> c_int {
        let i = match *self {
            Directory::Root => 0,
            Directory::Child(i) => i,
        };

        i.bind_parameter(stmt, col)
    }
}

impl FromSql for Directory {
    unsafe fn column_result(stmt: *mut libsqlite::sqlite3_stmt,
                            col: c_int)
                            -> SqliteResult<Directory> {
        FromSql::column_result(stmt, col).map(|i| {
            match i {
                0 => Directory::Root,
                v => Directory::Child(v),
            }
        })
    }
}

// An iterator over files in a state determined by the given timestamp. A file
// is represented by its path and a list of block id's.
// TODO: should be associated type?
pub struct Aliases<'a> {
    database: &'a Database,
    path: PathBuf, // FIXME: maybe this can be a &Path instead?
    timestamp: u64,
    file_list: Vec<(FileId, String)>,
    directory_list: Vec<Directory>,
    subdirectory: Option<Box<Aliases<'a>>>,
}

impl<'a> Aliases<'a> {
    pub fn new(database: &'a Database,
               path: PathBuf,
               directory: Directory,
               timestamp: u64)
               -> DatabaseResult<Aliases<'a>> {
        Ok(Aliases {
            database: database,
            path: path,
            timestamp: timestamp,
            file_list: try!(database.get_directory_content_at(directory, timestamp)),
            directory_list: try!(database.get_subdirectories(directory)),
            subdirectory: None,
        })
    }
}

impl<'a> Iterator for Aliases<'a> {
    type Item = DatabaseResult<(PathBuf, Vec<BlockId>)>;

    fn next(&mut self) -> Option<DatabaseResult<(PathBuf, Vec<BlockId>)>> {
        // return file from child directory
        loop {
            if let Some(ref mut dir) = self.subdirectory {
                if let result@Some(_) = dir.next() {
                    return result;
                }
            }

            match self.directory_list.pop() {
                None => break,
                Some(id) => {
                    let subdirectory = self.database
                                           .get_directory_name(id)
                                           .and_then(|directory_name| {
                                               Aliases::new(self.database,
                                                            self.path.join(&directory_name),
                                                            id,
                                                            self.timestamp)
                                           });

                    match subdirectory {
                        Ok(subdir) => {
                            self.subdirectory = Some(Box::new(subdir));
                        }
                        Err(e) => {
                            self.directory_list.push(id);

                            return Some(Err(e));
                        }
                    }
                }
            }
        }

        // return file from current directory
        self.file_list.pop().map(|(id, name)| {
            self.database
                .get_file_block_list(id)
                .map(|block_list| (self.path.join(&name), block_list))
        })
    }
}

pub struct Database {
    connection: SqliteConnection,
    path: PathBuf,
}

unsafe impl Send for Database { }

impl Database {
    fn new(path: PathBuf, flags: SqliteOpenFlags) -> DatabaseResult<Database> {
        let db = Database {
            connection: try!(SqliteConnection::open_with_flags(&path, flags)),
            path: path,
        };

        // set write lock timeout to 1 day
        let timeout: i64 = 24 * 60 * 60 * 1000;
        let pragma_query = format!("PRAGMA busy_timeout={};", timeout);
        let query_result = try!(db.connection.query_row(&pragma_query, &[], |row| row.get(0)));

        if timeout != query_result {
            return Err(DatabaseError {
                description: "Could not set timeout".to_string(),
                cause: None
            });
        }

        try!(db.connection.execute("PRAGMA synchronous=OFF;", &[]));
        try!(db.connection.execute("PRAGMA temp_store=MEMORY;", &[]));

        Ok(db)
    }

    pub fn from_file(path: PathBuf) -> DatabaseResult<Database> {
        Database::new(path, SQLITE_OPEN_FULL_MUTEX | SQLITE_OPEN_READ_WRITE)
    }

    pub fn create(path: PathBuf) -> BonzoResult<Database> {
        match path.exists() {
            true => Err(BonzoError::from_str("Database file already exists")),
            false => {
                let open_options = SQLITE_OPEN_FULL_MUTEX | SQLITE_OPEN_READ_WRITE |
                                   SQLITE_OPEN_CREATE;
                Ok(try!(Database::new(path, open_options)))
            }
        }
    }

    pub fn try_clone(&self) -> DatabaseResult<Database> {
        Database::from_file(self.path.clone())
    }

    fn query_and_collect<T, F, C>(&self, sql: &str, params: &[&ToSql], f: F) -> DatabaseResult<C>
        where F: Fn(SqliteRow) -> T,
              C: FromIterator<T>
    {
        let mut statement = try!(self.connection.prepare(sql));

        statement.query(params)
                 .and_then(|rows| rows.map(|possible_row| possible_row.map(|row| f(row))).collect())
                 .map_err(From::from)
    }

    pub fn to_bytes(self) -> BonzoResult<Vec<u8>> {
        try!(
            self.connection
                .close()
                .map_err(DatabaseError::from)
        );

        let mut buffer = Vec::new();

        try_io!(
            File::open(&self.path)
            .and_then(|mut file| {
                file.read_to_end(&mut buffer)
            })
        , &self.path);

        Ok(buffer)
    }

    pub fn get_subdirectories(&self, directory: Directory) -> DatabaseResult<Vec<Directory>> {
        self.query_and_collect("SELECT id FROM directory WHERE parent_id = $1;",
                               &[&directory],
                               |result| result.get(0))
    }

    pub fn get_directory_content_at(&self,
                                    directory: Directory,
                                    timestamp: u64)
                                    -> DatabaseResult<Vec<(FileId, String)>> {
        self.query_and_collect("SELECT alias.file_id, alias.name
                                  FROM alias
                                 INNER JOIN (SELECT MAX(id) AS max_id
                                               FROM alias
                                              WHERE directory_id = $1
                                                AND timestamp <= $2
                                              GROUP BY name) a ON alias.id = a.max_id
                                 WHERE file_id IS NOT NULL;",
                               &[&directory, &(timestamp as i64)],
                               |row| (row.get::<FileId>(0), row.get(1)))
    }

    pub fn get_directory_filenames(&self, directory: Directory) -> DatabaseResult<HashSet<String>> {
        self.query_and_collect("SELECT alias.name FROM alias
                                 INNER JOIN (SELECT MAX(id) AS max_id
                                               FROM alias
                                              WHERE directory_id = $1
                                              GROUP BY name) a ON alias.id = a.max_id
                                 WHERE file_id IS NOT NULL;",
                               &[&directory],
                               |row| row.get(0))
    }

    fn get_directory_name(&self, directory: Directory) -> DatabaseResult<String> {
        self.connection
            .query_row_safe("SELECT name FROM directory WHERE id = $1;",
                            &[&directory],
                            |row| row.get::<String>(0))
            .map_err(From::from)
    }

    fn get_file_block_list(&self, file_id: FileId) -> DatabaseResult<Vec<BlockId>> {
        self.query_and_collect("SELECT block_id FROM fileblock WHERE file_id = $1 ORDER BY \
                                ordinal ASC;",
                               &[&file_id],
                               |row| row.get(0))
            .map_err(From::from)
    }

    pub fn persist_file(&self,
                        directory: Directory,
                        filename: &str,
                        hash: &[u8],
                        last_modified: u64,
                        block_id_list: &[BlockId])
                        -> DatabaseResult<()> {
        let transaction = try!(self.connection.transaction());

        try!(self.connection.execute("INSERT INTO file (hash) VALUES ($1);", &[&hash]));

        let file_id = self.connection.last_insert_rowid();

        let mut statement =
            try!(self.connection.prepare("INSERT INTO fileblock (file_id, block_id, ordinal)
                                          VALUES ($1, $2, $3);"));

        for (ordinal, block_id) in block_id_list.iter().enumerate() {
            try!(statement.execute(&[&file_id, block_id, &(ordinal as i64)]));
        }

        try!(self.persist_alias(directory,
                                Some(FileId(file_id as u64)),
                                filename,
                                Some(last_modified)));

        transaction.commit().map_err(From::from)
    }

    pub fn persist_alias(&self,
                         directory: Directory,
                         file_id: Option<FileId>,
                         filename: &str,
                         last_modified: Option<u64>)
                         -> DatabaseResult<()> {
        let signed_modified = last_modified.map(|unsigned| unsigned as i64);
        let timestamp = Some(epoch_milliseconds() as i64);

        self.connection
            .execute("INSERT INTO alias (directory_id, file_id, name, modified, timestamp)
                      VALUES ($1, $2, $3, $4, $5);",
                     &[&directory, &file_id, &filename, &signed_modified, &timestamp])
            .map(|_| ())
            .map_err(From::from)
    }

    pub fn persist_null_alias(&self, directory: Directory, filename: &str) -> DatabaseResult<()> {
        self.persist_alias(directory, None, filename, None).map_err(From::from)
    }

    pub fn persist_block(&self, hash: &[u8]) -> DatabaseResult<BlockId> {
        try!(self.connection.execute("INSERT INTO block (hash) VALUES ($1);", &[&hash]));

        Ok(BlockId(self.connection.last_insert_rowid() as u64))
    }

    pub fn file_from_hash(&self, hash: &[u8]) -> DatabaseResult<Option<FileId>> {
        self.connection
            .query_row_safe("SELECT SUM(id) FROM file WHERE hash = $1;", &[&hash], |row| row.get(0))
            .map_err(From::from)
    }

    pub fn alias_known(&self,
                       directory: Directory,
                       filename: &str,
                       modified: u64)
                       -> DatabaseResult<bool> {
        self.connection
            .query_row_safe("SELECT COUNT(alias.id) FROM alias
                              INNER JOIN (SELECT MAX(id) AS max_id
                                            FROM alias
                                           WHERE directory_id = $1 AND name = $2) a
                                         ON alias.id = a.max_id
                              WHERE modified >= $3
                                AND file_id IS NOT NULL;",
                            &[&directory, &filename, &(modified as i64)],
                            |row| row.get::<i64>(0) > 0)
            .map_err(From::from)
    }

    pub fn block_hash_from_id(&self, id: BlockId) -> DatabaseResult<Vec<u8>> {
        self.connection
            .query_row_safe("SELECT hash FROM block WHERE id = $1;", &[&id], |row| row.get(0))
            .map_err(From::from)
    }

    pub fn block_id_from_hash(&self, hash: &[u8]) -> DatabaseResult<Option<BlockId>> {
        self.connection
            .query_row_safe("SELECT SUM(id) FROM block WHERE hash = $1;",
                            &[&hash],
                            |row| row.get(0))
            .map_err(From::from)
    }

    pub fn get_directory(&self, parent: Directory, name: &str) -> DatabaseResult<Directory> {
        let possible_directory: Option<Directory> = try!({
            let select_query = "SELECT SUM(id) FROM directory WHERE name = $1 AND parent_id = $2;";
            self.connection.query_row_safe(select_query, &[&name, &parent], |row| row.get(0))
        });

        if let Some(directory) = possible_directory {
            return Ok(directory);
        }

        try!(self.connection.execute("INSERT INTO directory (parent_id, name) VALUES ($1, $2);",
                                     &[&parent, &name]));

        Ok(Directory::Child(self.connection.last_insert_rowid()))
    }

    pub fn set_key(&self, key: &str, value: &str) -> DatabaseResult<i32> {
        self.connection
            .execute("INSERT INTO setting (key, value) VALUES ($1, $2);", &[&key, &value])
            .map_err(From::from)
    }

    pub fn get_key(&self, key: &str) -> DatabaseResult<Option<String>> {
        self.connection
            .query_row_safe("SELECT value FROM setting WHERE key = $1;", &[&key], |row| row.get(0))
            .map_err(From::from)
    }

    pub fn remove_old_aliases(&self, timestamp: u64) -> DatabaseResult<u64> {
        self.connection
            .execute("DELETE FROM alias
                       WHERE timestamp < $1
                         AND (file_id IS NULL
                              OR
                              id NOT IN (SELECT MAX(id) FROM alias GROUP BY name, directory_id));",
                     &[&(timestamp as i64)])
            .map(|rows_deleted| rows_deleted as u64)
            .map_err(From::from)
    }

    pub fn remove_unused_files(&self) -> DatabaseResult<()> {
        self.connection
            .execute("DELETE FROM fileblock
                       WHERE file_id not in (SELECT file_id FROM alias);",
                     &[])
            .and_then(|_| {
                self.connection.execute("DELETE FROM file
                                          WHERE id not in (SELECT file_id FROM alias);",
                                        &[])
            })
            .map(|_| ())
            .map_err(From::from)
    }

    pub fn get_unused_blocks(&self) -> DatabaseResult<Vec<(BlockId, Vec<u8>)>> {
        self.query_and_collect("SELECT id, hash
                                  FROM block
                                 WHERE id not in (SELECT id FROM fileblock);",
                               &[],
                               |row| (row.get(0), row.get(1)))
    }

    pub fn remove_block(&self, id: BlockId) -> DatabaseResult<()> {
        self.connection
            .execute("DELETE FROM block WHERE id = $1;", &[&id])
            .map(|_| ())
            .map_err(From::from)
    }

    pub fn setup(&self) -> DatabaseResult<()> {
        ["CREATE TABLE directory (
              id        INTEGER PRIMARY KEY,
              parent_id INTEGER,
              name      TEXT NOT NULL,
              FOREIGN KEY(parent_id) REFERENCES directory(id),
              UNIQUE(parent_id, name)
          );",
         "INSERT INTO directory (id, name) VALUES (0, \".\");",
         "CREATE TABLE file (
              id           INTEGER PRIMARY KEY,
              hash         BLOB NOT NULL,
              UNIQUE(hash)
          );",
         "CREATE INDEX file_hash_index ON file (hash)",
         "CREATE TABLE alias (
              id           INTEGER PRIMARY KEY,
              directory_id INTEGER NOT NULL,
              file_id      INTEGER,
              name         TEXT NOT NULL,
              modified     INTEGER,
              timestamp    INTEGER,
              FOREIGN KEY(directory_id) REFERENCES directory(id),
              FOREIGN KEY(file_id) REFERENCES file(id)
          );",
         "CREATE INDEX alias_directory_index ON alias (directory_id)",
         "CREATE TABLE block (
              id           INTEGER PRIMARY KEY,
              hash         BLOB NOT NULL,
              UNIQUE(hash)
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
          );"]
            .iter()
            .map(|&query| self.connection.execute(query, &[]))
            .fold_results((), |_, _| ())
            .map_err(From::from)
    }
}

#[cfg(test)]
mod test {
    use Directory;

    use super::super::tempdir::TempDir;

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

        assert_eq!(0usize, great_grand_children.len());
    }
}
