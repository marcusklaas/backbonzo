use std::io;
use std::path::{PathBuf, Path};
use std::fs::{read_dir, PathExt};
use std::borrow::ToOwned;
use std::iter::IteratorExt;
use std::cmp::Ordering;
use std::mem;

use super::super::iter_reduce::{Reduce, IteratorReduce};

pub struct FilesystemWalker<'a, T: 'static> {
    cur: Vec<(PathBuf, T)>,
    file_map: &'a Fn(&Path) -> io::Result<T>,
    sort_map: &'a Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering
}

// The idea here is that we serve the most recently changed files first
// because those are likely to be the most relevant.
impl<'a, T> Iterator for FilesystemWalker<'a, T> {
    type Item = io::Result<(PathBuf, T)>;

    fn next(&mut self) -> Option<io::Result<(PathBuf, T)>> {
        loop {
            match self.cur.pop() {
                Some((path, extra)) => {
                    if path.is_dir() {
                        if let Err(e) = self.read_dir_sorted(&path) {
                            return Some(Err(e));
                        }
                    }
                    else {
                        return Some(Ok((path, extra)));
                    }
                },
                None => return None
            }
        }
    }
}

impl<'a, T> FilesystemWalker<'a, T> {
    pub fn new<F, S>(dir: &Path, file_map: &'a F, sort_map: &'a S) -> io::Result<FilesystemWalker<'a, T>>
           where F: Fn(&Path) -> io::Result<T>,
                 S: Fn(&(PathBuf, T), &(PathBuf, T)) -> Ordering {
        let mut walker = FilesystemWalker {
            cur: Vec::new(),
            file_map: file_map,
            sort_map: sort_map
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

        let other_self: &FilesystemWalker<'a, T>  = unsafe { mem::transmute(&mut *self) };

        self.cur.sort_by(|a, b| (*other_self.sort_map)(a, b) );

        Ok(())
    }
}

pub struct NewestFirst<'a> {
    walker: FilesystemWalker<'a, u64>,
    file_map: Box<Fn(&Path) -> io::Result<u64>>,
    sort_map: Box<Fn(&(PathBuf, u64), &(PathBuf, u64)) -> Ordering>
}

impl<'a> Iterator for NewestFirst<'a> {
    type Item = io::Result<(PathBuf, u64)>;
    
    fn next(&mut self) -> Option<io::Result<(PathBuf, u64)>> {
        self.walker.next()
    }
}

pub fn newest_first_walker(dir: &Path) -> io::Result<NewestFirst<'static>> {
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
        unsafe { mem::copy_lifetime("wadda", &*sort_map) }
    );

    Ok(NewestFirst {
        walker: try!(walker),
        file_map: file_map,
        sort_map: sort_map
    })
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

        let list = super::newest_first_walker(temp_dir.path()).unwrap();

        let filenames: Vec<String> = list
            .map(|x| {
                let (path, _) = x.unwrap();
                
                path.file_name().unwrap().to_string_lossy().into_owned()
            })
            .collect();

        assert_eq!(&["deadlast", "third", "second", "firstfile", "filezero"], &filenames);
    }
}
