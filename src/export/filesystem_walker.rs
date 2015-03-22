use std::io;
use std::path::{PathBuf, Path};
use std::fs::{read_dir, PathExt};
use std::borrow::ToOwned;
use std::iter::IteratorExt;
use std::cmp::Ordering;
use std::mem;

use super::super::comm::spmc::bounded_fast as spmc;
use super::super::iter_reduce::{Reduce, IteratorReduce};

use super::super::database::Database;
use super::super::Directory;
use super::super::error::{BonzoResult, BonzoError};

pub struct FileInfo {
    pub path: PathBuf,
    pub modified: u64,
    pub filename: String,
    pub directory: Directory
}

pub type FileInfoMessage = BonzoResult<FileInfo>;

struct FilePathExporter<'sender> {
    database: Database,
    channel: &'sender mut spmc::Producer<'static, FileInfoMessage>
}

impl<'sender> FilePathExporter<'sender> {
    // Recursively walks the given directory, processing all files within.
    // Deletes references to deleted files which were previously found from the
    // database. Processes files in descending order of last mutation.
    fn export_directory(&self, path: &Path, directory: Directory) -> BonzoResult<()> {
        let content_iter = try!(newest_first_walker(path, false));
        let mut deleted_filenames = try!(self.database.get_directory_filenames(directory));
        
        for item in content_iter {
            let (content_path, last_modified) = try!(item);
            
            let filename = try!(
                content_path
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .ok_or(BonzoError::from_str("Could not convert filename to string"))
                    .map(String::from_str)
            );
            
            if content_path.is_dir() {
                let child_directory = try!(self.database.get_directory(directory, &filename));
            
                try!(self.export_directory(&content_path, child_directory));
            }
            else {
                // FIXME: don't String::from_str if filename is dir
                if directory != Directory::Root || filename != super::super::DATABASE_FILENAME {
                    deleted_filenames.remove(&filename);

                    try!(
                        self.channel.send_sync(Ok(FileInfo {
                            path: content_path,
                            modified: last_modified,
                            filename: filename,
                            directory: directory
                        })).map_err(|_| BonzoError::from_str("Failed sending file path"))
                    );
                }
            }
        }

        deleted_filenames
            .iter()
            .map(|filename| {
                self.database
                    .persist_null_alias(directory, filename.as_slice())
                    .map_err(|e| BonzoError::Database(e))
            })
            .reduce()
    }
}

// TODO: move this function and export_directory to own module
pub fn send_files(source_path: &Path, database: Database, mut channel: spmc::Producer<'static, FileInfoMessage>) {
    let result = {
        let exporter = FilePathExporter {
            database: database,
            channel: &mut channel
        };

        exporter.export_directory(source_path, Directory::Root)
    };

    if let Err(e) = result {
        let _ = channel.send_sync(Err(e));
    }
}

// Walks the filesystem in an order that is defined by sort map, returning extra
// information along with the paths. Is guaranteed to return directories before
// their children
pub struct FilesystemWalker<'a, T: 'static> {
    cur: Vec<(PathBuf, T)>,
    file_map: &'a Fn(&Path) -> io::Result<T>,
    sort_map: &'a Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering,
    recursive: bool
}

impl<'a, T> Iterator for FilesystemWalker<'a, T> {
    type Item = io::Result<(PathBuf, T)>;

    fn next(&mut self) -> Option<io::Result<(PathBuf, T)>> {
        match self.cur.pop() {
            Some((path, extra)) => {
                if self.recursive && path.is_dir() {
                    match self.read_dir_sorted(&path) {
                        Err(e) => Some(Err(e)),
                        Ok(..) => Some(Ok((path, extra)))
                    }
                }
                else {
                    Some(Ok((path, extra)))
                }
            },
            None => None
        }
    }
}

impl<'a, T> FilesystemWalker<'a, T> {
    pub fn new<F, S>(dir: &Path, file_map: &'a F, sort_map: &'a S, recursive: bool) -> io::Result<FilesystemWalker<'a, T>>
           where F: Fn(&Path) -> io::Result<T>,
                 S: Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering {
        let mut walker = FilesystemWalker {
            cur: Vec::new(),
            file_map: file_map,
            sort_map: sort_map,
            recursive: recursive
        };

        try!(walker.read_dir_sorted(dir));

        Ok(walker)
    }
    
    fn read_dir_sorted(&mut self, dir: &Path) -> io::Result<()> {    
        try!(
            read_dir(dir)
            .and_then(|list| list
                .map(|possible_entry| {
                    possible_entry.and_then(|entry| {
                        let path = entry.path();
                        
                        (*self.file_map)(&path)
                            .map(|extra| {
                                let pair = (path.to_owned(), extra);
                                self.cur.push(pair);
                            })
                    })
                })
                .reduce()
            )
        );

        let other_self: &FilesystemWalker<'a, T> = unsafe { mem::transmute(&mut *self) };

        self.cur.sort_by(|a, b| (*other_self.sort_map)(a, b) );

        Ok(())
    }
}

// The idea here is that we serve the most recently changed files first
// because those are likely to be the most relevant.
pub struct NewestFirst<'a> {
    walker: FilesystemWalker<'a, u64>,
    #[allow(dead_code)]
    file_map: Box<Fn(&Path) -> io::Result<u64>>,
    #[allow(dead_code)]
    sort_map: Box<Fn(&(PathBuf, u64), &(PathBuf, u64)) -> Ordering>
}

impl<'a> Iterator for NewestFirst<'a> {
    type Item = io::Result<(PathBuf, u64)>;
    
    fn next(&mut self) -> Option<io::Result<(PathBuf, u64)>> {
        self.walker.next()
    }
}

pub fn newest_first_walker(dir: &Path, recursive: bool) -> io::Result<NewestFirst<'static>> {
    fn newest_first(a: &(PathBuf, u64), b: &(PathBuf, u64)) -> Ordering {
        let &(_, time_a) = a;
        let &(_, time_b) = b;

        time_a.cmp(&time_b) 
    }        

    fn modified_date(path: &Path) -> io::Result<u64> {
        path.metadata()
            .map(|stats| {
                stats.modified()
            })
    }

    let file_map = Box::new(modified_date);
    let sort_map = Box::new(newest_first);
    
    let walker: io::Result<FilesystemWalker<u64>> = FilesystemWalker::<u64>::new(
        dir,
        unsafe { mem::copy_lifetime("silly", &*file_map) },
        unsafe { mem::copy_lifetime("wadda", &*sort_map) },
        recursive
    );

    Ok(NewestFirst {
        walker: try!(walker),
        file_map: file_map,
        sort_map: sort_map
    })
}

#[cfg(test)]
mod test {
    use std::thread::sleep;
    use std::time::Duration;
    use std::fs::create_dir_all;

    use super::super::super::tempdir::TempDir;
    use super::super::super::write_to_disk;

    #[test]
    fn read_dir() {
        let temp_dir = TempDir::new("readdir-test").unwrap();
        let root_path = temp_dir.path();
        let sub_dir = root_path.join("sub");

        create_dir_all(&sub_dir).unwrap();

        {
            let file_path = root_path.join("filezero");
            write_to_disk(&file_path, b"test123").unwrap();
        }

        sleep(Duration::milliseconds(50));

        {
            let file_path = sub_dir.join("firstfile");
            write_to_disk(&file_path, b"yolo").unwrap();
        }

        sleep(Duration::milliseconds(50));

        {
            let file_path = root_path.join("second");
            write_to_disk(&file_path, b"hello").unwrap();
        }

        sleep(Duration::milliseconds(50));

        {
            let file_path = root_path.join("third");
            write_to_disk(&file_path, b"waddaa").unwrap();
        }

        sleep(Duration::milliseconds(50));

        {
            let file_path = sub_dir.join("deadlast");
            write_to_disk(&file_path, b"plswork").unwrap();
        }

        let recursive_list = super::newest_first_walker(temp_dir.path(), true).unwrap();

        let all: Vec<String> = recursive_list
            .map(|x| {
                let (path, _) = x.unwrap();
                
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["sub", "deadlast", "third", "second", "firstfile", "filezero"], &all);

        let flat_list = super::newest_first_walker(temp_dir.path(), false).unwrap();

        let directory: Vec<String> = flat_list
            .map(|x| {
                let (path, _) = x.unwrap();
                
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["sub", "third", "second", "filezero"], &directory);
    }
}
