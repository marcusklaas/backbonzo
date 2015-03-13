use std::io;
use std::path::{PathBuf, Path};
use std::fs::{read_dir, PathExt};
use std::borrow::ToOwned;
use std::iter::IteratorExt;

use super::super::iter_reduce::{Reduce, IteratorReduce};

pub struct FilesystemWalker {
    cur: Vec<(u64, PathBuf)>
}

// The idea here is that we serve the most recently changed files first
// because those are likely to be the most relevant.
impl Iterator for FilesystemWalker {
    type Item = io::Result<(u64, PathBuf)>;

    fn next(&mut self) -> Option<io::Result<(u64, PathBuf)>> {
        loop {
            match self.cur.pop() {
                Some((modified, path)) => {
                    if path.is_dir() {
                        if let Err(e) = self.read_dir_sorted(&path) {
                            return Some(Err(e));
                        }
                    }
                    else {
                        return Some(Ok((modified, path)));
                    }
                },
                None => return None
            }
        }
    }
}

impl FilesystemWalker {
    pub fn new(dir: &Path) -> io::Result<FilesystemWalker> {
        let mut walker = FilesystemWalker {
            cur: Vec::new()
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
                        
                        path.metadata()
                            .map(|stats| {
                                let pair = (stats.modified(), path.to_owned());
                                self.cur.push(pair);
                            })
                    })
                })
                .reduce()
            )
        );

        self.cur.sort_by(|&(a, _), &(b, _)| a.cmp(&b));

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::old_io::Timer;
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

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = sub_dir.join("firstfile");
            write_to_disk(&file_path, b"yolo").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = root_path.join("second");
            write_to_disk(&file_path, b"hello").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = root_path.join("third");
            write_to_disk(&file_path, b"waddaa").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = sub_dir.join("deadlast");
            write_to_disk(&file_path, b"plswork").unwrap();
        }

        let list = super::FilesystemWalker::new(temp_dir.path()).unwrap();

        let filenames: Vec<String> = list
            .map(|x| {
                let (_, path) = x.unwrap();
                
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["deadlast", "third", "second", "firstfile", "filezero"], &filenames);
    }
}
