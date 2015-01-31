#![feature(collections)]
#![feature(io)]
#![feature(std_misc)]
#![feature(core)]
#![feature(path)]

extern crate backbonzo;
extern crate time;

use backbonzo::BonzoError;
use std::old_io::{FilePermission, FileAccess, FileMode, TempDir};
use std::old_io::fs::{File, PathExtensions, mkdir_recursive, rename, unlink};
use std::time::duration::Duration;
use time::get_time;

#[test]
fn init() {
    let source_dir = TempDir::new("init").unwrap();
    let backup_dir = TempDir::new("init-backup").unwrap();
    
    let password = "testpassword";

    let result = backbonzo::init(
        source_dir.path().clone(),
        backup_dir.path().clone(),
        password.clone()
    );

    assert!(result.is_ok());

    let second_result = backbonzo::init(
        source_dir.path().clone(),
        backup_dir.path().clone(),
        password.clone()
    );

    let is_expected = match second_result {
        Err(BonzoError::Other(ref str)) => str.as_slice() == "Database file already exists",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_wrong_password() {
    let dir = TempDir::new("wrong-password").unwrap();
    let source_path = dir.path().clone();
    let destination_path = source_path.clone();
    let deadline = time::now();

    assert!(
        backbonzo::init(
            source_path.clone(),
            destination_path.clone(),
            "testpassword"
        ).is_ok()
    );

    let backup_result = backbonzo::backup(
        source_path,
        1000000,
        "differentpassword",
        deadline);

    let is_expected = match backup_result {
        Err(BonzoError::Other(ref str)) => str.as_slice() == "Password is not the same as in database",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_no_init() {
    let dir = TempDir::new("no-init").unwrap();
    let source_path = dir.path().clone();
    let deadline = time::now();

    let backup_result = backbonzo::backup(
        source_path,
        1000000,
        "differentpassword",
        deadline
    );

    let is_expected = match backup_result {
        Err(BonzoError::Database(e)) => e.message.as_slice() == "unable to open database file",
        _                            => false
    };

    assert!(is_expected);
}

#[test]
// tests recursive behaviour, and filters for restore
fn backup_and_restore() {
    let source_temp = TempDir::new("source").unwrap();
    let destination_temp = TempDir::new("destination").unwrap();
    let source_path = source_temp.path().clone();
    let destination_path = destination_temp.path().clone();
    let password = "testpassword";
    let deadline = time::now() + Duration::minutes(1);

    assert!(mkdir_recursive(&source_path.join("test"), FilePermission::all()).is_ok());

    let filenames = ["test/welcome.txt", "welco.yolo", "smth_diffrent.jpg"];
    let bytes = "71d6e2f35502c03743f676449c503f487de29988".as_bytes();

    for filename in filenames.iter() {
        let file_path = source_path.join(filename);
        let mut file = File::create(&file_path).unwrap();
        assert!(file.write_all(bytes).is_ok());
        assert!(file.fsync().is_ok());
    }

    assert!(
        backbonzo::init(
            source_path.clone(),
            destination_path.clone(),
            password.clone()
        ).is_ok()
    );

    let backup_result = backbonzo::backup(
        source_path.clone(),
        1000000,
        password,
        deadline
    );

    assert!(backup_result.is_ok());

    let timestamp = epoch_milliseconds();
    let restore_temp = TempDir::new("restore").unwrap();
    let restore_path = restore_temp.path().clone();

    let restore_result = backbonzo::restore(
        restore_path.clone(),
        destination_path.clone(),
        password,
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

fn epoch_milliseconds() -> u64 {
    let stamp = get_time();
        
    stamp.nsec as u64 / 1000 / 1000 + stamp.sec as u64 * 1000
}

#[test]
fn renames() {
    let source_temp = TempDir::new("rename-source").unwrap();
    let destination_temp = TempDir::new("first-destination").unwrap();
    let source_path = source_temp.path().clone();
    let destination_path = destination_temp.path().clone();
    let password = "helloworld";
    let deadline = time::now() + Duration::minutes(10);

    assert!(
        backbonzo::init(
            source_path.clone(),
            destination_path.clone(),
            password.clone()
        ).is_ok()
    );

    let first_file_name = "first";
    let first_message   = "first message. ".as_bytes();

    let second_file_name = "second";
    let second_message   = "second".as_bytes();

    let mixed_message = "secondmessage. ".as_bytes();
    
    // create 1 file in source map
    let first_timestamp = {
        let file_path = source_path.join(first_file_name);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(first_message).unwrap();
        file.fsync().unwrap();

        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            password,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    // rename file, update modified date and backup again
    let second_timestamp = {
        let prev_path = source_path.join(first_file_name);
        let file_path = source_path.join(second_file_name);

        rename(&prev_path, &file_path).unwrap();

        let mut file = File::open_mode(&file_path, FileMode::Open, FileAccess::ReadWrite).unwrap();
        file.write_all(second_message).unwrap();
        file.fsync().unwrap();
        
        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            password,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    // rename file to first and update timestamp
    let third_timestamp = {
        let first_path = source_path.join(first_file_name);
        let second_path = source_path.join(second_file_name);

        rename(&second_path, &first_path).unwrap();
        
        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            password,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    // delete file
    {
        let first_path = source_path.join(first_file_name);

        unlink(&first_path).unwrap();

        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            password,
            deadline
        );

        assert!(backup_result.is_ok());
    }

    // restore to second state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().clone();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            password,
            second_timestamp + 1,
            String::from_str("**")
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(second_path.exists());
        assert!(! first_path.exists());

        let mut file = File::open_mode(&second_path, FileMode::Open, FileAccess::ReadWrite).unwrap();
        let contents = file.read_to_end().unwrap();

        assert_eq!(mixed_message, contents.as_slice());
    }

    // restore to third state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().clone();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            password,
            third_timestamp + 1,
            String::from_str("**")
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!( ! second_path.exists());
        assert!(first_path.exists());

        let mut file = File::open_mode(&first_path, FileMode::Open, FileAccess::ReadWrite).unwrap();
        let contents = file.read_to_end().unwrap();

        assert_eq!(mixed_message, contents.as_slice());
    }

    // restore to last state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().clone();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            password,
            epoch_milliseconds(),
            String::from_str("**")
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(! second_path.exists());
        assert!(! first_path.exists());
    }

    // restore to first state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().clone();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            password,
            first_timestamp + 1,
            String::from_str("**")
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(! second_path.exists());
        assert!(first_path.exists());

        let mut file = File::open_mode(&first_path, FileMode::Open, FileAccess::ReadWrite).unwrap();
        let contents = file.read_to_end().unwrap();

        assert_eq!(first_message, contents.as_slice());
    }

    // restore to initial state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().clone();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            password,
            5000,
            String::from_str("**")
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(! second_path.exists());
        assert!(! first_path.exists());
    }
}
