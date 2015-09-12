use std::io::{self, Read};
use std::fs::File;
use std::path::Path;

// Semi-iterator which reads a file one block at a time. Is not a proper
// Iterator because we only keep one block in memory at a time.
pub struct Chunks<R> {
    file: R,
    buffer: Vec<u8>,
}

impl<R: Read> Chunks<R> {
    pub fn new(reader: R, chunk_size: usize) -> Chunks<R> {
        Chunks { file: reader, buffer: vec![0; chunk_size] }
    }

    pub fn next(&mut self) -> Option<io::Result<&[u8]>> {
        match self.file.read(&mut self.buffer[..]) {
            Ok(0) => None,
            Ok(bytes) => Some(Ok(&self.buffer[0..bytes])),
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait Chunk: Read + Sized {
    fn chunks(self, chunk_size: usize) -> Chunks<Self> {
        Chunks::new(self, chunk_size)
    }
}

impl<T: Read> Chunk for T {}

pub fn file_chunks(path: &Path, chunk_size: usize) -> io::Result<Chunks<File>> {
    File::open(&path).map(|file| file.chunks(chunk_size))
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::fs::File;

    use super::super::tempdir::TempDir;

    #[test]
    fn file_chunks() {
        let temp_dir = TempDir::new("chunks").unwrap();
        let file_path = temp_dir.path().join("test");

        let mut file = File::create(&file_path).unwrap();
        file.write_all(&[0, 1, 2, 3, 4]).unwrap();

        let mut chunks = super::file_chunks(&file_path, 2).unwrap();

        assert_eq!([0, 1], chunks.next().unwrap().unwrap());
        assert_eq!([2, 3], chunks.next().unwrap().unwrap());
        assert_eq!([4], chunks.next().unwrap().unwrap());
        assert!(chunks.next().is_none());
    }

    // TODO: add test for different read object
}
