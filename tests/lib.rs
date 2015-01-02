extern crate backbonzo;
extern crate time;

use backbonzo::BonzoError;
use std::io::TempDir;
use std::io::fs::{File, PathExtensions, mkdir_recursive};
use std::time::duration::Duration;
//use std::rand::{Rng, OsRng};

#[test]
fn init() {
    let dir = TempDir::new("backbonzo-test").unwrap();
    let database_path = dir.path().join("index.db3");
    let password = String::from_str("testpassword");

    let result = backbonzo::init(database_path.clone(), password.clone());

    assert!(result.is_ok());
    assert!(database_path.exists());

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

    assert!(backbonzo::init(database_path.clone(), String::from_str("testpassword")).is_ok());

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
        deadline
    );

    let is_expected = match backup_result {
        Err(BonzoError::Database(e)) => e.message.as_slice() == "unable to open database file",
        _                            => false
    };

    assert!(is_expected);
}

#[test]
fn backup_and_restore() {
    let source_temp = TempDir::new("backbonzo-source").unwrap();
    let destination_temp = TempDir::new("backbonzo-destination").unwrap();
    let database_path = source_temp.path().join("index.db3");
    let source_path = source_temp.path().clone();
    let destination_path = destination_temp.path().clone();
    let password = String::from_str("testpassword");
    let deadline = time::now() + Duration::minutes(1);

    assert!(mkdir_recursive(&source_path.join("test"), std::io::FilePermission::all()).is_ok());

    let filenames = ["test/welcome.txt", "welco.yolo", "smth_diffrent.jpg"];
    let bytes = "Hello, world!".as_bytes();

    for filename in filenames.iter() {
        let file_path = source_path.join(filename);
        let mut file = File::create(&file_path).unwrap();
        assert!(file.write(bytes).is_ok());
        assert!(file.fsync().is_ok());
    }

    assert!(backbonzo::init(database_path.clone(), password.clone()).is_ok());

    let backup_result = backbonzo::backup(
        database_path.clone(),
        source_path.clone(),
        destination_path.clone(),
        1000000,
        password.clone(),
        deadline
    );

    assert!(backup_result.is_ok());

    let timestamp = 1000 * time::get_time().sec as u64;
    let restore_temp = TempDir::new("backbonzo-restore").unwrap();
    let restore_path = restore_temp.path().clone();

    let restore_result = backbonzo::restore(
        restore_path.clone(),
        destination_path.clone(),
        password.clone(),
        timestamp,
        String::from_str("**/welco*")
    );

    assert!(restore_result.is_ok());

    let restored_file_path = restore_path.join("welco.yolo");

    assert!(restored_file_path.exists());

    let mut restored_file = File::open(&restored_file_path).unwrap();

    assert_eq!(bytes, restored_file.read_to_end().unwrap().as_slice());

    assert!(!restore_path.join("smth_diffrent.jpg").exists());
    assert!(restore_path.join("test/welcome.txt").exists());
}
