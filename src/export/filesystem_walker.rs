use std::io;
use std::path::{PathBuf, Path};
use std::fs::{read_dir, PathExt};
use std::borrow::ToOwned;
use std::iter::IteratorExt;
use std::vec::IntoIter;

pub struct FilesystemWalker {
    cur: Option<IntoIter<(u64, PathBuf)>>,
    stack: Vec<IntoIter<(u64, PathBuf)>>
}

// The idea here is that we serve the most recently changed files first
// because those are likely to be the most relevant. The current implementation
// does not guarantee this, though.
impl Iterator for FilesystemWalker {
    type Item = io::Result<(u64, PathBuf)>;

    fn next(&mut self) -> Option<io::Result<(u64, PathBuf)>> {
        loop {
            if let Some(ref mut cur) = self.cur {
                if let Some((modified, path)) = cur.next() {
                    if path.is_dir() {
                        match FilesystemWalker::read_dir_sorted(&path) {
                            Err(e) => return Some(Err(e)),
                            Ok(deeper) => self.stack.push(deeper)
                        }
                    }
                    
                    return Some(Ok((modified, path)));
                }
            }
            
            self.cur = None;
            
            match self.stack.pop() {
                next @ Some(..) => self.cur = next,
                None => return None,
            }
        }
    }
}

impl FilesystemWalker {
    pub fn new(dir: &Path) -> io::Result<FilesystemWalker> {
        let start = try!(FilesystemWalker::read_dir_sorted(dir));

        Ok(FilesystemWalker {
            cur: Some(start),
            stack: Vec::new()
        })
    }
    
    fn read_dir_sorted(dir: &Path) -> io::Result<IntoIter<(u64, PathBuf)>> {    
        let mut vec: Vec<(u64, PathBuf)> = try!(
            read_dir(dir)
            .and_then(|list| list
                .map(|possible_entry| {
                    possible_entry.and_then(|entry| {
                        let path = entry.path();
                        
                        path.metadata()
                            .map(move |stats| {
                                (stats.modified(), path.to_owned())
                            })
                    })
                })
                .collect()
            )
        );

        vec.sort_by(|&(a, _), &(b, _)| a.cmp(&b).reverse());

        Ok(vec.into_iter())
    }
}


#[cfg(test)]
mod test {
    use std::old_io::Timer;
    use std::time::Duration;

    use super::super::super::tempdir::TempDir;
    use super::super::super::write_to_disk;

    #[test]
    fn read_dir() {
        let temp_dir = TempDir::new("readdir-test").unwrap();

        {
            let file_path = temp_dir.path().join("firstfile");
            write_to_disk(&file_path, b"test123").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = temp_dir.path().join("second");
            write_to_disk(&file_path, b"hello").unwrap();
        }

        Timer::new().unwrap().sleep(Duration::milliseconds(50));

        {
            let file_path = temp_dir.path().join("third");
            write_to_disk(&file_path, b"waddaa").unwrap();
        }

        let list = super::FilesystemWalker::new(temp_dir.path()).unwrap();

        let filenames: Vec<String> = list
            .map(|x| {
                let (_, path) = x.unwrap();
                
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["third", "second", "firstfile"], &filenames);
    }
}
