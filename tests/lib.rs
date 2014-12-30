extern crate backbonzo;

use backbonzo::BonzoError;
use std::io::{IoError, IoResult, TempDir, BufReader};
use std::io::fs::{unlink, copy, File, mkdir_recursive};

#[test]
fn init() {
    let dir = TempDir::new("backbonzo-test").unwrap();
    let database_path = dir.path().join("index.db3");

    let result = backbonzo::init(&database_path, String::from_str("testpassword"));

    assert!(result.is_ok());

    let second_result = backbonzo::init(&database_path, String::from_str("testpassword"));

    let is_expected = match second_result {
        Err(BonzoError::Other(ref str)) if str.as_slice() == "Database file already exists" => true,
        _                                                                                   => false
    };

    assert!(is_expected);
}
