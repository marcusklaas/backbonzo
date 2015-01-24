use std::io::IoResult;
use std::io::fs::File;
use std::mem::forget;

// Semi-iterator which reads a file one block at a time. Is not a proper
// Iterator because we only keep one block in memory at a time.
pub struct Chunks<'a> {
    file: File,
    buffer: Vec<u8>
}

impl<'a> Chunks<'a> {
    pub fn from_path(path: &Path, block_size: u32) -> IoResult<Chunks> {
        let machine_block_size = block_size as usize;
        let mut vec = Vec::with_capacity(machine_block_size);
        let pointer = vec.as_mut_ptr();
        
        Ok(Chunks {
            file: try!(File::open(path)),
            buffer: unsafe {
                forget(vec);
                
                Vec::from_raw_parts(pointer, machine_block_size, machine_block_size)
            }
        })
    }
    
    pub fn next(&'a mut self) -> Option<&'a [u8]> {
        self.file.read(self.buffer.as_mut_slice()).ok().map(move |bytes| {
            &self.buffer[0..bytes]
        })
    }
}


#[cfg(test)]
mod test {
    use std::io::TempDir;
    use std::io::fs::File;
    
    #[test]
    fn chunks() {
        let temp_dir = TempDir::new("chunks").unwrap();
        let file_path = temp_dir.path().join("test");
        let mut file = File::create(&file_path).unwrap();

        file.write(&[0, 1, 2, 3, 4]).unwrap();        

        let mut chunks = super::Chunks::from_path(&file_path, 2).unwrap();

        assert_eq!([0, 1], chunks.next().unwrap());
        assert_eq!([2, 3], chunks.next().unwrap());
        assert_eq!([4], chunks.next().unwrap());
        assert!(chunks.next().is_none());
    }

    #[test]
    fn non_existent_file() {
        let temp_dir = TempDir::new("bad-chunks").unwrap();
        let file_path = temp_dir.path().join("test");
        
        let chunks = super::Chunks::from_path(&file_path, 2);

        assert!(chunks.is_err());
    }
}
