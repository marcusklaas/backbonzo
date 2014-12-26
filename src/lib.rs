#![feature(slicing_syntax)]

extern crate rusqlite;
extern crate "crypto" as rust_crypto;
extern crate serialize;
extern crate time;

use std::io::{IoError, IoResult, TempDir};
use std::io::fs::{unlink, copy, readdir, File, PathExtensions, mkdir_recursive};
use std::error::FromError;
use std::path::Path;
use rusqlite::{SqliteConnection, SqliteError, SQLITE_OPEN_FULL_MUTEX, SQLITE_OPEN_READ_WRITE, SQLITE_OPEN_CREATE};
use rust_crypto::symmetriccipher::SymmetricCipherError;
use std::thread::Thread;
use std::comm::sync_channel;
use std::collections::RingBuf;
use std::rand::{Rng, OsRng};

// FIXME: import crypto crate in the crypto module and re-export SymmetricCipherError there (or our own crypto error)

mod database;
mod crypto;

pub enum BonzoError {
    Database(SqliteError),
    Io(IoError),
    Crypto(SymmetricCipherError),
    Other(String)
}

impl FromError<IoError> for BonzoError {
    fn from_error(error: IoError) -> BonzoError {
        BonzoError::Io(error)
    }
}

impl FromError<SymmetricCipherError> for BonzoError {
    fn from_error(error: SymmetricCipherError) -> BonzoError {
        BonzoError::Crypto(error)
    }
}

impl FromError<SqliteError> for BonzoError {
    fn from_error(error: SqliteError) -> BonzoError {
        BonzoError::Database(error)
    }
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

enum FileInstruction {
    NewBlock(FileBlock),
    Complete(FileComplete),
    Error(BonzoError),
    Done
}

struct FileBlock {
    pub bytes: Vec<u8>,
    pub iv: Vec<u8>,
    pub hash: String
}

// okay this is kinda complex but here's how the block_id_list works. known blocks
// are represented by Some(id), and new blocks are represented by None as we don't
// known the id in the thread that does the encryption. the handling thread needs
// to keep track of the ids of the new blocks since the last completed file and
// replace them (in the right order!)
struct FileComplete {
    pub filename: String,
    pub hash: String,
    pub last_modified: u64,
    pub directory_id: uint,
    pub block_id_list: Vec<Option<uint>>
}

pub struct BackupManager {
    connection: SqliteConnection,
    database_path: Path,
    source_path: Path,
    backup_path: Path,
    block_size: uint,
    password: String,
    encryption_key: Vec<u8>
}

impl BackupManager {
    pub fn new(database_path: Path, source_path: Path, backup_path: Path, block_size: uint, password: String) -> BonzoResult<BackupManager> {
        if !database_path.exists() {
            return Err(BonzoError::Other(format!("Database file not found"))); 
        }

        let key = crypto::derive_key(password.as_slice());
                
        Ok(BackupManager {
            connection: try!(open_connection(&database_path)),
            database_path: database_path,
            source_path: source_path,
            backup_path: backup_path,
            block_size: block_size,
            password: password,
            encryption_key: key
        })
    }

    pub fn update(&mut self, deadline: time::Tm) -> BonzoResult<()> {
        try!(self.check_key());

        let (tx, rx) = sync_channel::<FileInstruction>(5);

        let key = self.encryption_key.clone();
        let path = self.database_path.clone();
        let block_size = self.block_size;
        let source_path = self.source_path.clone();

        Thread::spawn(move||
            tx.send(match ExportBlockSender::new(path, key, block_size, tx.clone()).and_then(|exporter| exporter.export_directory(&source_path, 0)) {
                Ok(..) => FileInstruction::Done,
                Err(e) => FileInstruction::Error(e)
            })
        ).detach();

        let mut id_queue: RingBuf<uint> = RingBuf::new();

        while deadline.cmp(&time::now_utc()) == Ordering::Greater {
            match rx.recv() {
                FileInstruction::Done     => break,
                FileInstruction::Error(e) => return Err(e),
                FileInstruction::NewBlock(block) => {
                    let path = try!(block_output_path(&self.backup_path, block.hash.as_slice()));
            
                    try!(write_to_disk(&path, block.bytes.as_slice()));
        
                    let id = try!(database::persist_block(&self.connection, block.hash.as_slice(), block.iv.as_slice()));

                    id_queue.push_back(id);
                },
                FileInstruction::Complete(file) => {
                    let mut real_id_list = Vec::new();

                    for id in file.block_id_list.iter() {
                        match *id {
                            Some(i) => real_id_list.push(i),
                            None    => match id_queue.pop_front() {
                                Some(i) => real_id_list.push(i),
                                None    => return Err(BonzoError::Other(format!("Block buffer is empty")))
                            }
                        }
                    }

                    try!(database::persist_file(
                        &self.connection,
                        file.directory_id,
                        file.filename.as_slice(),
                        file.hash.as_slice(),
                        file.last_modified,
                        real_id_list.as_slice()
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn restore(&self, timestamp: u64) -> BonzoResult<()> {
        try!(database::Aliases::new(&self.connection, self.source_path.clone(), 0, timestamp))
            .map(|(path, block_list)| self.restore_file(&path, block_list.as_slice()))
            .fold(Ok(()), |a, b| a.and(b))
    }

    pub fn restore_file(&self, path: &Path, block_list: &[uint]) -> BonzoResult<()> {
        let file_directory = path.dir_path();
        try!(mkdir_recursive(&file_directory, std::io::FilePermission::all()));
        
        let mut file = try!(File::create(path));

        for block_id in block_list.iter() {
            let (hash, iv) = try!(database::block_from_id(&self.connection, *block_id));
            let block_path = try!(block_output_path(&self.backup_path, hash.as_slice()));
            let mut block_file = try!(File::open(&block_path));
            let bytes = try!(block_file.read_to_end());
            let decrypted_bytes = try!(crypto::decrypt_block(bytes.as_slice(), self.encryption_key.as_slice(), iv.as_slice()));

            try!(file.write(decrypted_bytes.as_slice()));
            try!(file.fsync());
        }

        Ok(())
    }

    fn check_key(&self) -> BonzoResult<()> {
        let hash = database::get_key(&self.connection, "password");
        let real_hash = try!(hash.ok_or(BonzoError::Other(format!("Saved hash is NULL"))));

        match crypto::check_password(self.password.as_slice(), real_hash.as_slice()) {
            true  => Ok(()),
            false => Err(BonzoError::Other(format!("Password is not the same as in database")))
        }
    }

    fn export_index(self) -> BonzoResult<()> {
        try!(self.connection.close());
        
        let mut file = try!(File::open(&self.database_path));
        let bytes = try!(file.read_to_end());
        let iv = [0u8, ..16];
        let encrypted_bytes = try!(crypto::encrypt_block(bytes.as_slice(), self.encryption_key.as_slice(), &iv));
        let new_index = self.backup_path.join("index-new");
        let index = self.backup_path.join("index");
        
        try!(write_to_disk(&new_index, encrypted_bytes.as_slice()));
        try!(copy(&new_index, &index));
        
        Ok(try!(unlink(&new_index)))
    }
}

struct ExportBlockSender {
    connection: SqliteConnection,
    encryption_key: Vec<u8>,
    block_size: uint,
    sender: SyncSender<FileInstruction>
}

impl ExportBlockSender {
    pub fn new(database_path: Path, encryption_key: Vec<u8>, block_size: uint, sender: SyncSender<FileInstruction>) -> BonzoResult<ExportBlockSender> {
        if !database_path.exists() {
            return Err(BonzoError::Other(format!("Database file not found"))); 
        }

        Ok(ExportBlockSender {
            connection: try!(open_connection(&database_path)),
            encryption_key: encryption_key,
            block_size: block_size,
            sender: sender
        })
    }

    pub fn export_directory(&self, path: &Path, directory_id: uint) -> BonzoResult<()> {
        let mut content_list = try!(readdir(path));

        /* FIXME: we're doing lots of stats this way. better to do them once, pair them
         * with their paths and then sort those */
        content_list.sort_by(|a, b|
            match a.stat() {
                Err(..)    => Equal,
                Ok(a_stat) => match b.stat() {
                    Err(..)    => Equal,
                    Ok(b_stat) => b_stat.modified.cmp(&a_stat.modified)
                }
            }
        );

        let mut known_names = try!(database::get_directory_files(&self.connection, directory_id));
        
        for content_path in content_list.iter() {
            if content_path.is_dir() {
                let relative_path = try!(content_path.path_relative_from(path).ok_or(BonzoError::Other(format!("Could not get relative path"))));
                let name = try!(relative_path.as_str().ok_or(BonzoError::Other(format!("Cannot express directory name in UTF8"))));
                let child_directory_id = try!(database::get_directory(&self.connection, directory_id, name));
            
                try!(self.export_directory(content_path, child_directory_id));
            }
            else {
                let filename = String::from_str(try!(
                    content_path.filename_str()
                    .ok_or(BonzoError::Other(format!("Could not convert filename to string"))
                )));

                known_names.remove(&filename);
                
                try!(self.export_file(directory_id, content_path, filename));
            }
        }

        for deleted_filename in known_names.iter() {            
            try!(database::persist_null_alias(&self.connection, directory_id, deleted_filename.as_slice()));

            println!("Removed {} from directory {}", deleted_filename, directory_id);
        }

        Ok(())
    }

    #[allow(unused_must_use)]
    fn export_file(&self, directory_id: uint, path: &Path, filename: String) -> BonzoResult<()> {
        let last_modified = try!(path.stat()).modified;

        if database::alias_known(&self.connection, directory_id, filename.as_slice(), last_modified) {           
            return Ok(());
        }
        
        let hash = try!(crypto::hash_file(path));

        if let Some(file_id) = database::file_from_hash(&self.connection, hash.as_slice()) {
            return Ok(try!(database::persist_alias(&self.connection, directory_id, Some(file_id), filename.as_slice(), last_modified)));
        }
        
        let mut blocks = try!(Blocks::from_path(path, self.block_size));
        let mut block_id_list = Vec::new();
        
        while let Some(slice) = blocks.next() {
            block_id_list.push(try!(self.export_block(slice)));
        }
        
        self.sender.send_opt(FileInstruction::Complete(FileComplete {
            filename: filename,
            hash: hash,
            last_modified: last_modified,
            directory_id: directory_id,
            block_id_list: block_id_list
        }));

        Ok(())
    }

    #[allow(unused_must_use)]
    pub fn export_block(&self, block: &[u8]) -> BonzoResult<Option<uint>> {
        let hash = crypto::hash_block(block);

        if let Some(id) = database::block_id_from_hash(&self.connection, hash.as_slice()) {
            return Ok(Some(id))
        }

        let mut iv = Vec::from_elem(16, 0u8);
        let mut rng = try!(OsRng::new());

        rng.fill_bytes(iv.as_mut_slice());

        self.sender.send_opt(FileInstruction::NewBlock(FileBlock {
            bytes: try!(crypto::encrypt_block(block, self.encryption_key.as_slice(), iv.as_slice())),
            iv: iv,
            hash: hash
        }));

        Ok(None)
    }
}

pub fn init(database_path: &Path, password: String) -> BonzoResult<()> {
    if database_path.exists() {
        return Err(BonzoError::Other(format!("Database file already exists"))); 
    }
    
    let connection = try!(open_connection(database_path));
    let hash = try!(crypto::hash_password(password.as_slice()));
    
    try!(database::setup(&connection));

    Ok(try!(database::set_key(&connection, "password", hash.as_slice()).map(|_|())))
}

pub fn backup(database_path: Path, source_path: Path, backup_path: Path, block_bytes: uint, password: String, deadline: time::Tm) -> BonzoResult<()> {
    let mut manager = try!(BackupManager::new(database_path, source_path, backup_path, block_bytes, password));
            
    try!(manager.update(deadline));
    manager.export_index()
}

pub fn restore(source_path: Path, backup_path: Path, block_bytes: uint, password: String, timestamp: u64) -> BonzoResult<()> {
    let temp_directory = try!(TempDir::new("bonzo"));
    let decrypted_index_path = try!(decrypt_index(&backup_path, temp_directory.path(), password.as_slice()));
    let manager = try!(BackupManager::new(decrypted_index_path, source_path, backup_path, block_bytes, password));
    
    manager.restore(timestamp)
}

fn decrypt_index(backup_path: &Path, temp_dir: &Path, password: &str) -> BonzoResult<Path> {
    let encrypted_index_path = backup_path.join("index");
    let decrypted_index_path = temp_dir.join("index.db3");
    let mut file = try!(File::open(&encrypted_index_path));
    let contents = try!(file.read_to_end());
    let iv = [0u8, ..16];
    let key = crypto::derive_key(password.as_slice());
    let decrypted_content = try!(crypto::decrypt_block(contents[], key[], &iv));

    try!(write_to_disk(&decrypted_index_path, decrypted_content[]));

    Ok(decrypted_index_path)
}

fn open_connection(path: &Path) -> BonzoResult<SqliteConnection> {
    let error = BonzoError::Other(format!("Couldn't convert database path to string"));
    let filename = try!(path.as_str().ok_or(error)); 

    Ok(try!(SqliteConnection::open_with_flags(filename, SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULL_MUTEX)))
}

fn block_output_path(base_path: &Path, hash: &str) -> IoResult<Path> {
    let path = base_path.join(hash[0..2]);
    
    try!(mkdir_recursive(&path, std::io::FilePermission::all()));
    
    Ok(path.join(hash))
}

fn write_to_disk(path: &Path, bytes: &[u8]) -> IoResult<()> {
    let mut file = try!(File::create(path));
    
    try!(file.write(bytes));
    
    file.fsync()
}
