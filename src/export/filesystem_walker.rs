use std::io;
use std::path::{PathBuf, Path};
use std::fs::read_dir;
use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::mem;

use comm::spmc::bounded_fast as spmc;
use filetime::FileTime;

use ::itertools::Itertools;
use database::Database;
use Directory;
use error::{BonzoResult, BonzoError};

pub struct FileInfo {
    pub path: PathBuf,
    pub modified: u64,
    pub filename: String,
    pub directory: Directory,
}

pub type FileInfoMessage = BonzoResult<FileInfo>;

struct FilePathExporter<'sender> {
    database: Database,
    channel: &'sender mut spmc::Producer<'static, FileInfoMessage>,
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

            // We have to (?) do the transmute to bypass the borrow checker.
            // Otherwise we cannot send content_path, because the checker thinks
            // it still borrowed by file_name.
            let filename: &str = unsafe {
                mem::transmute(try!(content_path
                                        .file_name()
                                        .and_then(|os_str| os_str.to_str())
                                        .ok_or(BonzoError::from_str("Could not convert \
                                                                     filename to string"))))
            };

            if content_path.is_dir() {
                let child_directory = try!(self.database.get_directory(directory, filename));

                try!(self.export_directory(&content_path, child_directory));
                continue;
            }

            if directory != Directory::Root || filename != super::super::DATABASE_FILENAME {
                deleted_filenames.remove(filename);
                let owned_name = filename.to_string();

                try!(
                    self.channel.send_sync(Ok(FileInfo {
                        path: content_path,
                        modified: last_modified,
                        filename: owned_name,
                        directory: directory
                    }))
                    .map_err(|_| BonzoError::from_str("Failed sending file path"))
                );
            }
        }

        deleted_filenames.iter()
                         .map(|filename| {
                             self.database
                                 .persist_null_alias(directory, &filename)
                                 .map_err(|e| BonzoError::Database(e))
                         })
                         .fold_results((), |_, _| ())
    }
}

// TODO: move this function and export_directory to own module
pub fn send_files(source_path: &Path,
                  database: Database,
                  mut channel: spmc::Producer<'static, FileInfoMessage>) {
    let result = {
        let exporter = FilePathExporter { database: database, channel: &mut channel };

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
    root: PathBuf,
    cur: Vec<(PathBuf, T)>,
    file_map: &'a Fn(&Path) -> io::Result<T>,
    sort_map: &'a Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering,
    recursive: bool,
    symlinks: bool,
}

impl<'a, T> Iterator for FilesystemWalker<'a, T> {
    type Item = BonzoResult<(PathBuf, T)>;

    fn next(&mut self) -> Option<BonzoResult<(PathBuf, T)>> {
        self.cur.pop().map(|(path, extra)| {
            if self.recursive && path.is_dir() {
                try!(self.read_dir_sorted(&path));
            }

            Ok((path, extra))
        })
    }
}

// TODO: add tests for symlinks, non-symlinks case
impl<'a, T> FilesystemWalker<'a, T> {
    pub fn new<F, S>(dir: &Path,
                     file_map: &'a F,
                     sort_map: &'a S,
                     recursive: bool,
                     follow_symlinks: bool)
                     -> BonzoResult<FilesystemWalker<'a, T>>
        where F: Fn(&Path) -> io::Result<T>,
              S: Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering
    {
        let mut walker = FilesystemWalker {
            root: dir.to_owned(),
            cur: Vec::new(),
            file_map: file_map,
            sort_map: sort_map,
            recursive: recursive,
            symlinks: follow_symlinks,
        };

        try!(walker.read_dir_sorted(dir));

        Ok(walker)
    }

    // filter out recursive symlinks or all symlinks, depending on
    // settings
    fn is_accepted_path(&self, path: &Path) -> io::Result<bool> {
        path.symlink_metadata().map(|meta| {
            let is_symlink = meta.file_type().is_symlink();

            !is_symlink || self.symlinks && !path.starts_with(&self.root)
        })
    }

    fn read_dir_sorted(&mut self, dir: &Path) -> BonzoResult<()> {
        // add the paths and their associated values to the internal buffer
        for entry in try_io!(read_dir(dir), dir) {
            let path = try_io!(entry, dir).path();

            if !try_io!(self.is_accepted_path(&path), path) {
                continue;
            }

            let extra = try_io!((*self.file_map)(&path), path);
            let pair = (path.to_owned(), extra);
            self.cur.push(pair);
        }

        self.cur.sort_by(self.sort_map);

        Ok(())
    }
}

// Ick, just needed to get a &'static to newest_first and modified_date.
static SORT_MAP: &'static Fn(&(PathBuf, u64), &(PathBuf, u64)) -> Ordering = &newest_first;
static FILE_MAP: &'static Fn(&Path) -> io::Result<u64> = &modified_date;

fn newest_first(a: &(PathBuf, u64), b: &(PathBuf, u64)) -> Ordering {
    let &(_, time_a) = a;
    let &(_, time_b) = b;

    time_a.cmp(&time_b)
}

fn modified_date(path: &Path) -> io::Result<u64> {
    path.metadata()
        .map(|meta| FileTime::from_last_modification_time(&meta))
        .map(|filetime| {
            let millis = filetime.nanoseconds() as u64 / 1_000_000;
            1_000 * filetime.seconds_relative_to_1970() + millis
        })
}

pub fn newest_first_walker(dir: &Path,
                           recursive: bool)
                           -> BonzoResult<FilesystemWalker<'static, u64>> {
    FilesystemWalker::<u64>::new(dir, &FILE_MAP, &SORT_MAP, recursive, false)
}

#[cfg(test)]
mod test {
    use std::thread::sleep;
    use std::io::{self, Write};
    use std::path::Path;
    use std::fs::{File, create_dir_all};
    use std::time::Duration;

    use super::super::super::tempdir::TempDir;

    fn write_to_disk(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = try!(File::create(path));

        try!(file.write_all(bytes));
        file.sync_all()
    }

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

        sleep(Duration::from_millis(50));

        {
            let file_path = sub_dir.join("firstfile");
            write_to_disk(&file_path, b"yolo").unwrap();
        }

        sleep(Duration::from_millis(50));

        {
            let file_path = root_path.join("second");
            write_to_disk(&file_path, b"hello").unwrap();
        }

        sleep(Duration::from_millis(50));

        {
            let file_path = root_path.join("third");
            write_to_disk(&file_path, b"waddaa").unwrap();
        }

        sleep(Duration::from_millis(50));

        {
            let file_path = sub_dir.join("deadlast");
            write_to_disk(&file_path, b"plswork").unwrap();
        }

        let recursive_list = super::newest_first_walker(temp_dir.path(), true).unwrap();

        let all: Vec<String> = recursive_list.map(|x| {
                                                 let (path, _) = x.unwrap();

                                                 path.file_name()
                                                     .unwrap()
                                                     .to_string_lossy()
                                                     .into_owned()
                                             })
                                             .collect();

        assert_eq!(&["sub", "deadlast", "third", "second", "firstfile", "filezero"][..], &all[..]);

        let flat_list = super::newest_first_walker(temp_dir.path(), false).unwrap();

        let directory: Vec<String> = flat_list.map(|x| {
                                                  let (path, _) = x.unwrap();

                                                  path.file_name()
                                                      .unwrap()
                                                      .to_string_lossy()
                                                      .into_owned()
                                              })
                                              .collect();

        assert_eq!(&["sub", "third", "second", "filezero"][..], &directory[..]);
    }

    #[cfg_attr(target_os = "linux", test)]
    fn check_loops() {
        use std::os::unix;

        let temp_dir = TempDir::new("loop-test").ok().expect("make temp");
        let path = temp_dir.path();

        match unix::fs::symlink(path, &path.join("link")) {
            Err(e) => panic!("{:?}, {:?}", e, e.kind()),
            Ok(..) => {}
        }

        assert!(1 >= super::newest_first_walker(path, true).unwrap().count());
    }
}
