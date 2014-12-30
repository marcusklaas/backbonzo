extern crate backbonzo;
extern crate time;

use backbonzo::BonzoError;
use std::io::{IoError, IoResult, TempDir, BufReader};
use std::io::fs::{unlink, copy, File, mkdir_recursive};

#[test]
fn init() {
    let dir = TempDir::new("backbonzo-test").unwrap();
    let database_path = dir.path().join("index.db3");
    let password = String::from_str("testpassword");

    let result = backbonzo::init(database_path.clone(), password.clone());

    assert!(result.is_ok());

    let second_result = backbonzo::init(database_path.clone(), password.clone());

    let is_expected = match second_result {
        Err(BonzoError::Other(ref str)) => str.as_slice() == "Database file already exists",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_wrong_password() {
    let dir = TempDir::new("backbonzo-test").unwrap();
    let database_path = dir.path().join("index.db3");
    let source_path = dir.path().clone();
    let destination_path = source_path.clone();
    let deadline = time::now();

    let init_result = backbonzo::init(database_path.clone(), String::from_str("testpassword"));

    let backup_result = backbonzo::backup(
        database_path.clone(),
        source_path,
        destination_path,
        1000000,
        String::from_str("differentpassword"),
        deadline);

    let is_expected = match backup_result {
        Err(BonzoError::Other(ref str)) => str.as_slice() == "Password is not the same as in database",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_no_init() {
    let dir = TempDir::new("backbonzo-test").unwrap();
    let database_path = dir.path().join("index.db3");
    let source_path = dir.path().clone();
    let destination_path = source_path.clone();
    let deadline = time::now();

    let backup_result = backbonzo::backup(
        database_path.clone(),
        source_path,
        destination_path,
        1000000,
        String::from_str("differentpassword"),
        deadline);

    let is_expected = match backup_result {
        Err(BonzoError::Database(e)) => e.message.as_slice() == "unable to open database file",
        _                            => false
    };

    assert!(is_expected);
}
