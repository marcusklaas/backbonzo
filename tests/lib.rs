#![feature(collections)]
#![feature(std_misc)]
#![feature(core)]
#![feature(path_ext)]

extern crate backbonzo;
extern crate time;
extern crate tempdir;

use backbonzo::BonzoError;
use std::io::{Read, Write, self};
use std::fs::{File, PathExt, create_dir_all, rename, remove_file, OpenOptions};
use std::time::duration::Duration;
use time::get_time;
use std::path::{PathBuf, AsPath};
use tempdir::TempDir;

fn open_read_write(path: &AsPath) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).append(false).open(path)
}

#[test]
fn init() {
    let source_dir = TempDir::new("init").unwrap();
    let backup_dir = TempDir::new("init-backup").unwrap();
    
    let password = "testpassword";

    let result = backbonzo::init(
        PathBuf::new(source_dir.path()),
        PathBuf::new(backup_dir.path()),
        password.clone()
    );

    assert!(result.is_ok());

    let second_result = backbonzo::init(
        PathBuf::new(source_dir.path()),
        PathBuf::new(backup_dir.path()),
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
    let source_path = PathBuf::new(dir.path());
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
    let source_path = PathBuf::new(dir.path());
    let deadline = time::now();

    let backup_result = backbonzo::backup(
        source_path,
        1000000,
        "differentpassword",
        deadline
    );

    assert_eq!(format!("{}", backup_result.unwrap_err()).as_slice(), "Database error: unable to open database file");
}

#[test]
// tests recursive behaviour, and filters for restore
fn backup_and_restore() {
    let source_temp = TempDir::new("source").unwrap();
    let destination_temp = TempDir::new("destination").unwrap();
    let source_path = PathBuf::new(source_temp.path());
    let destination_path = PathBuf::new(destination_temp.path());
    let password = "testpassword";
    let deadline = time::now() + Duration::minutes(1);

    assert!(create_dir_all(&source_path.join("test")).is_ok());

    let filenames = ["welcome.txt", "welco.yolo", "smth_diffrent.jpg"];
    let bytes = b"71d6e2f35502c03743f676449c503f487de29988";

    for filename in filenames.iter() {
        let file_path = source_path.join(filename);
        let mut file = File::create(&file_path).unwrap();
        assert!(file.write_all(bytes).is_ok());
        assert!(file.sync_all().is_ok());
    }

    {
        let subdir_path = source_path.join("test").join("welcomg!");
        let mut file = File::create(&subdir_path).unwrap();
        assert!(file.write_all(bytes).is_ok());
        assert!(file.sync_all().is_ok());
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
    let restore_path = PathBuf::new(restore_temp.path());

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
    let mut buffer = Vec::new();
    restored_file.read_to_end(&mut buffer).unwrap();
    
    assert_eq!(bytes, buffer.as_slice());

    assert!(!restore_path.join("smth_diffrent.jpg").exists());
    assert!(restore_path.join("welcome.txt").exists());
    assert!(restore_path.join("test").join("welcomg!").exists());
}

fn epoch_milliseconds() -> u64 {
    let stamp = get_time();
        
    stamp.nsec as u64 / 1000 / 1000 + stamp.sec as u64 * 1000
}

#[test]
fn renames() {
    let source_temp = TempDir::new("rename-source").unwrap();
    let destination_temp = TempDir::new("first-destination").unwrap();
    let source_path = PathBuf::new(source_temp.path());
    let destination_path = PathBuf::new(destination_temp.path());
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
    let first_message   = b"first message. ";

    let second_file_name = "second";
    let second_message   = b"second";

    let mixed_message = b"secondmessage. ";
    
    // create 1 file in source map
    let first_timestamp = {
        let file_path = source_path.join(first_file_name);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(first_message).unwrap();
        file.sync_all().unwrap();

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

        let mut file = open_read_write(&file_path).unwrap();
        file.write_all(second_message).unwrap();
        file.sync_all().unwrap();
        
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

        remove_file(&first_path).unwrap();

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
        let restore_path = PathBuf::new(restore_temp.path());

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

        let mut file = open_read_write(&second_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(mixed_message, contents.as_slice());
    }

    // restore to third state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = PathBuf::new(restore_temp.path());

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

        let mut file = open_read_write(&first_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(mixed_message, contents.as_slice());
    }

    // restore to last state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = PathBuf::new(restore_temp.path());

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
        let restore_path = PathBuf::new(restore_temp.path());

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

        let mut file = open_read_write(&first_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(first_message, contents.as_slice());
    }

    // restore to initial state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = PathBuf::new(restore_temp.path());

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
