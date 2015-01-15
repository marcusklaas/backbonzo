use std::fmt;
use std::ops::{Deref, DerefMut};
use std::time::duration::Duration;
use super::time;

struct Summary {
    bytes:  u64,
    blocks: u64,
    files:  u64,
    start:  u64
}

impl Summary {
    pub fn new() -> Summary {
        Summary {
            bytes:  0,
            blocks: 0,
            files:  0,
            start:  time::get_time().sec as u64
        }
    }

    pub fn add_block(&mut self, block: &[u8]) {
        self.blocks     += 1;
        self.bytes += block.len() as u64;
    }

    pub fn add_file(&mut self) {
        self.files += 1;
    }

    pub fn duration(&self) -> Duration {
        let now = time::get_time().sec as u64;
        let seconds_passed = (now - self.start) as i64; 

        Duration::seconds(seconds_passed)
    }
}

// The bytes field refers to the number of bytes restored (after decryption and
// decompression)
pub struct RestorationSummary(Summary);

impl RestorationSummary {
    pub fn new() -> RestorationSummary {
        RestorationSummary ( Summary::new() )
    }
}

impl Deref for RestorationSummary {
    type Target = Summary;

    fn deref<'a>(&'a self) -> &'a Summary {
        &self.0
    }
}

impl DerefMut for RestorationSummary {
    fn deref_mut<'a>(&'a mut self) -> &'a mut Summary {
        &mut self.0
    }
}

impl fmt::Show for RestorationSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let seconds_passed = self.duration().num_seconds();
        
        write!(
            f,
            "Restored {} bytes to {} files, from {} blocks in {} seconds",
            self.bytes,
            self.files,
            self.blocks,
            seconds_passed
        )
    }
}

// The bytes field refers to the number of bytes stored at the backup location
// after compression and encryption.
// Only newly written files and blocks will be included in this summary.
pub struct BackupSummary {
    summary: Summary,
    source_bytes: u64,
    pub timeout: bool
}

impl BackupSummary {
    pub fn new() -> BackupSummary {
        BackupSummary {
            summary: Summary::new(),
            source_bytes: 0,
            timeout: false
        }            
    }

    pub fn add_block(&mut self, block: &[u8], source_bytes: u64) {
        self.source_bytes += source_bytes;
        self.summary.add_block(block)
    }

    pub fn add_file(&mut self) {
        self.summary.add_file()
    }
}

impl fmt::Show for BackupSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let seconds_passed = self.summary.duration().num_seconds();
        let compression_ratio = (self.summary.bytes as f64) / (self.source_bytes as f64);
                
        write!(
            f,
            "Backed up {} files, into {} blocks containing {} bytes, in {} seconds. Compression ratio: {}. Timeout: {}",
            self.summary.files,
            self.summary.blocks,
            self.summary.bytes,
            seconds_passed,
            compression_ratio,
            self.timeout
        )
    }
}

#[cfg(test)]
mod test {
    extern crate regex;
    #[plugin] #[no_link]
    extern crate regex_macros;

    use super::super::time;
    use std::num::SignedInt;
    use std::iter::repeat;
    
    #[test]
    fn restoration() {
        let mut summary = super::RestorationSummary::new();
        let now = time::get_time().sec as i64;        

        let time_diff_seconds = (now - summary.start as i64).abs();
        assert!(time_diff_seconds < 10);

        let vec: Vec<u8> = repeat(5).take(1000).collect();

        summary.add_block(&vec[10..20]);
        summary.add_block(&vec[0..500]);
        summary.add_block(&vec[990..999]);

        summary.add_file();
        
        let representation = format!("{:?}", summary);

        assert!(is_prefix("Restored 519 bytes to 1 files, from 3 blocks in ", representation.as_slice()));
    }

    #[test]
    fn backup() {
        let mut summary = super::BackupSummary::new();

        let vec: Vec<u8> = repeat(5).take(1000).collect();

        summary.add_block(&vec[10..20], 100);

        summary.add_file();
        summary.add_file();

        let re = regex!(r"Backed up 2 files, into 1 blocks containing 10 bytes, in \d+ seconds. Compression ratio: 0.1. Timeout: false");

        let representation = format!("{:?}", summary);

        println!("{}", representation);
        
        assert!(re.is_match(representation.as_slice()));
    }

    fn is_prefix(prefix: &str, haystack: &str) -> bool {
        prefix.len() <= haystack.len() && prefix.chars().zip(haystack.chars()).all(|(a, b)| a == b) 
    }
}