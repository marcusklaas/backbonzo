extern crate number_prefix;

use self::number_prefix::{decimal_prefix, Standalone, Prefixed};

use std::fmt;
use std::time::Duration;
use super::time;

fn format_bytes(bytes: u64) -> String {
    match decimal_prefix(bytes as f64) {
        Standalone(bytes)   => format!("{} bytes", bytes),
        Prefixed(prefix, n) => format!("{:.0} {}B", n, prefix),
    }
}

#[derive(Debug)]
pub struct InitSummary;

impl fmt::Display for InitSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Initialized backbonzo index.")
    }
}

#[derive(Debug)]
pub struct CleanupSummary {
    pub bytes: u64,
    pub aliases: u64,
    pub blocks: u64,
}

impl fmt::Display for CleanupSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let byte_desc = format_bytes(self.bytes);

        write!(
            f,
            "Cleaned up {} old versions of files, removing {} blocks containing {}.",
            self.aliases,
            self.blocks,
            byte_desc
        )
    }
}

#[derive(Debug)]
pub struct Summary {
    pub bytes:  u64,
    pub blocks: u64,
    pub files:  u64,
    pub start:  u64,
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
        self.blocks += 1;
        self.bytes += block.len() as u64;
    }

    pub fn add_file(&mut self) {
        self.files += 1;
    }

    pub fn duration(&self) -> Duration {
        let now = time::get_time().sec as u64;
        let seconds_passed = now - self.start; 

        Duration::from_secs(seconds_passed)
    }
}

// The bytes field refers to the number of bytes restored (after decryption and
// decompression)
#[derive(Debug)]
pub struct RestorationSummary(Summary);

impl RestorationSummary {
    pub fn new() -> RestorationSummary {
        RestorationSummary(Summary::new())
    }

    pub fn add_block(&mut self, block: &[u8]) {
        self.0.add_block(block)
    }

    pub fn add_file(&mut self) {
        self.0.add_file()
    }
}

impl fmt::Display for RestorationSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let seconds_passed = self.0.duration().secs();
        let byte_desc = format_bytes(self.0.bytes);

        write!(
            f,
            "Restored {} to {} files, from {} blocks in {} seconds.",
            byte_desc,
            self.0.files,
            self.0.blocks,
            seconds_passed
        )
    }
}

// The bytes field refers to the number of bytes stored at the backup location
// after compression and encryption.
// Only newly written files and blocks will be included in this summary.
#[derive(Debug)]
pub struct BackupSummary {
    pub summary: Summary,
    pub cleanup: Option<CleanupSummary>,
    pub source_bytes: u64,
    pub timeout: bool
}

impl BackupSummary {
    pub fn new() -> BackupSummary {
        BackupSummary {
            summary: Summary::new(),
            cleanup: None,
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

    pub fn add_cleanup_summary(&mut self, summary: CleanupSummary) {
        self.cleanup = Some(summary);
    }
}

impl fmt::Display for BackupSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let seconds_passed = self.summary.duration().secs();
        let compression_ratio = (self.summary.bytes as f64) / (self.source_bytes as f64);
        let byte_desc = format_bytes(self.summary.bytes);

        try!(write!(
            f,
            "Backed up {} files, into {} blocks containing {}, in {} seconds.\n\
             Compression ratio: {}",
            self.summary.files,
            self.summary.blocks,
            byte_desc,
            seconds_passed,
            compression_ratio
        ));

        if let Some(ref cleanup_summary) = self.cleanup {
            try!(write!(f, "\n{}", cleanup_summary.to_string()))
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    extern crate regex;

    use super::super::time;
    use std::iter::repeat;

    #[test]
    fn restoration() {
        let mut summary = super::RestorationSummary::new();
        let now = time::get_time().sec;

        let time_diff_seconds = (now - summary.0.start as i64).abs();
        assert!(time_diff_seconds < 10);

        let vec: Vec<u8> = repeat(5).take(1000).collect();

        summary.add_block(&vec[10..20]);
        summary.add_block(&vec[0..500]);
        summary.add_block(&vec[990..999]);

        summary.add_file();

        assert!(summary.to_string().starts_with("Restored 519 bytes to 1 files, from 3 blocks in "));
    }

    #[test]
    fn backup() {
        let mut summary = super::BackupSummary::new();

        let vec: Vec<u8> = repeat(5).take(1000).collect();

        summary.add_block(&vec[10..20], 100);

        summary.add_file();
        summary.add_file();

        let representation = summary.to_string();

        let re = ::regex::Regex::new(r"Backed up 2 files, into 1 blocks containing 10 bytes, in \d+ seconds").unwrap();

        assert!(re.is_match(&representation));

        let re = ::regex::Regex::new(r"Compression ratio: 0\.1").unwrap();

        assert!(re.is_match(&representation));
    }
}
