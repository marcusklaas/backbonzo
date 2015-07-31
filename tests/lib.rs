#![feature(path_ext)]

extern crate backbonzo;
extern crate time;
extern crate tempdir;

use backbonzo::{AesEncrypter, BonzoError};
use std::io::{Read, Write, self};
use std::fs::{File, PathExt, create_dir_all, rename, remove_file, OpenOptions, read_dir};
use time::{Duration, get_time};
use tempdir::TempDir;
use std::convert::AsRef;
use std::borrow::ToOwned;
use std::path::Path;
use std::thread::sleep_ms;

// FIXME: loads of code duplication here. Clean it up!

fn open_read_write<P: AsRef<Path>>(path: &P) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).append(false).open(path)
}

// Regression test for the bug where backbonzo would err when it tried to remove
// a file during clean up which was already deleted earlier.
#[test]
fn cleanup_regression_test() {
    let source_temp = TempDir::new("cleanup-source").unwrap();
    let destination_temp = TempDir::new("cleanup-dest").unwrap();
    let source_path = source_temp.path().to_owned();
    let destination_path = destination_temp.path().to_owned();
    let crypto_scheme = AesEncrypter::new("testpassword");
    let deadline = time::now() + Duration::minutes(1);

    let init_result = backbonzo::init(
        &source_path,
        &destination_path,
        &crypto_scheme
    );

    assert!(init_result.is_ok());

    // write initial file
    let file_path = source_path.join("file1");
    {
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"first edition!").ok().expect("Failed writing to file.");
        assert!(file.sync_all().is_ok());
    }

    // run backup of file
    backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        0,
        deadline
    ).ok().expect("First backup failed");

    // save timestamp
    sleep_ms(3000);

    // delete file
    remove_file(&file_path).ok().expect("Couldn't remove file");
    assert!(file_path.exists() == false);

    // delete backup
    // FIXME: this makes too many assumptions on the structure of the backup
    let mut deletion_counter = 0;
    for p in read_dir(destination_path.clone()).unwrap() {
        let path = p.unwrap().path();

        if path.is_dir() {
            for q in read_dir(path).unwrap() {
                remove_file(q.unwrap().path()).unwrap();
                deletion_counter += 1;
            }
        }
    }

    assert!(deletion_counter >= 1);

    // rerun backup with very strict max_age parameter
    let summary = backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        1,
        deadline
    ).unwrap();

    let cleanup_summary = &summary.cleanup.unwrap();

    // Backup also makes a new null alias, which may or may not be deleted.
    assert!(cleanup_summary.aliases >= 1 && cleanup_summary.aliases <= 2);
    assert!(cleanup_summary.blocks >= 1);
    assert!(cleanup_summary.bytes == 0);
}

#[test]
fn cleanup() {
    let source_temp = TempDir::new("cleanup-source").unwrap();
    let destination_temp = TempDir::new("cleanup-dest").unwrap();
    let source_path = source_temp.path().to_owned();
    let destination_path = destination_temp.path().to_owned();
    let crypto_scheme = AesEncrypter::new("testpassword");
    let deadline = time::now() + Duration::minutes(1);

    let init_result = backbonzo::init(
        &source_path,
        &destination_path,
        &crypto_scheme
    );

    assert!(init_result.is_ok());

    // write initial file
    let file_path = source_path.join("file1");
    {
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"first edition!").ok().expect("Failed writing to file.");
        assert!(file.sync_all().is_ok());
    }

    // run backup of file
    backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        0,
        deadline
    ).ok().expect("First backup failed");

    // save timestamp
    let timestamp = epoch_milliseconds();
    sleep_ms(100);

    // delete file and re-run backup with forgiving max_age parameter
    remove_file(&file_path).ok().expect("Couldn't remove file");
    assert!(file_path.exists() == false);

    backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        60 * 1000,
        deadline
    ).ok().expect("Second backup failed");

    // run restore and check that our file is restored
    backbonzo::restore(
        source_path.clone(),
        destination_path.clone(),
        &crypto_scheme,
        timestamp,
        "**".to_owned()
    ).ok().expect("First restore failed");

    assert!(file_path.exists());

    // delete file again
    remove_file(&file_path).ok().expect("Couldn't remove file");
    assert!(file_path.exists() == false);

    // run backup with very strict max_age parameter
    backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        1,
        deadline
    ).ok().expect("Third backup failed");

    // again run restore and make sure that we cleaned up our file
    backbonzo::restore(
        source_path.clone(),
        destination_path.clone(),
        &crypto_scheme,
        timestamp,
        "**".to_owned()
    ).ok().expect("Second restore failed");

    assert!(file_path.exists() == false);
}

#[test]
fn init() {
    let source_dir = TempDir::new("init").unwrap();
    let backup_dir = TempDir::new("init-backup").unwrap();
    
    let crypto_scheme = AesEncrypter::new("testpassword");

    let result = backbonzo::init(
        &source_dir.path(),
        &backup_dir.path(),
        &crypto_scheme
    );

    assert!(result.is_ok());

    let second_result = backbonzo::init(
        &source_dir.path(),
        &backup_dir.path(),
        &crypto_scheme
    );

    let is_expected = match second_result {
        Err(BonzoError::Other(ref str)) => &str[..] == "Database file already exists",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_wrong_password() {
    let dir = TempDir::new("wrong-password").unwrap();
    let source_path = dir.path().to_owned();
    let destination_path = source_path.clone();
    let deadline = time::now();

    assert!(
        backbonzo::init(
            &source_path,
            &destination_path,
            &AesEncrypter::new("testpassword")
        ).is_ok()
    );

    let backup_result = backbonzo::backup(
        source_path,
        1000000,
        &AesEncrypter::new("differentpassword"),
        0,
        deadline
    );

    let is_expected = match backup_result {
        Err(BonzoError::Other(ref str)) => &str[..] == "Password is not the same as in database",
        _                               => false
    };

    assert!(is_expected);
}

#[test]
fn backup_no_init() {
    let dir = TempDir::new("no-init").unwrap();
    let source_path = dir.path().to_owned();
    let deadline = time::now();

    let backup_result = backbonzo::backup(
        source_path,
        1000000,
        &AesEncrypter::new("differentpassword"),
        0,
        deadline
    );

    assert_eq!(&format!("{}", backup_result.unwrap_err())[..], "Database error: unable to open database file");
}

#[test]
// tests recursive behaviour, and filters for restore
fn backup_and_restore() {
    let source_temp = TempDir::new("source").unwrap();
    let destination_temp = TempDir::new("destination").unwrap();
    let source_path = source_temp.path().to_owned();
    let destination_path = destination_temp.path().to_owned();
    let crypto_scheme = AesEncrypter::new("testpassword");
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
            &source_path,
            &destination_path,
            &crypto_scheme
        ).is_ok()
    );

    let backup_result = backbonzo::backup(
        source_path.clone(),
        1000000,
        &crypto_scheme,
        0,
        deadline
    );

    assert!(backup_result.is_ok());

    let timestamp = epoch_milliseconds();
    let restore_temp = TempDir::new("restore").unwrap();
    let restore_path = restore_temp.path().to_owned();

    let restore_result = backbonzo::restore(
        restore_path.clone(),
        destination_path.clone(),
        &crypto_scheme,
        timestamp,
        "**/welco*"
    );

    assert!(restore_result.is_ok());

    let restored_file_path = restore_path.join("welco.yolo");

    assert!(restored_file_path.exists());

    let mut restored_file = File::open(&restored_file_path).unwrap();
    let mut buffer = Vec::new();
    restored_file.read_to_end(&mut buffer).unwrap();
    
    assert_eq!(&bytes[..], &buffer[..]);

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
    let source_path = source_temp.path().to_owned();
    let destination_path = destination_temp.path().to_owned();
    let crypto_scheme = AesEncrypter::new("helloworld");
    let deadline = time::now() + Duration::minutes(10);
    let max_age_milliseconds = 60 * 60 * 1000;

    assert!(
        backbonzo::init(
            &source_path,
            &destination_path,
            &crypto_scheme
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
            &crypto_scheme,
            max_age_milliseconds,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    sleep_ms(100);

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
            &crypto_scheme,
            max_age_milliseconds,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    sleep_ms(100);

    // rename file to first and update timestamp
    let third_timestamp = {
        let first_path = source_path.join(first_file_name);
        let second_path = source_path.join(second_file_name);

        rename(&second_path, &first_path).unwrap();
        
        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            &crypto_scheme,
            max_age_milliseconds,
            deadline
        );

        assert!(backup_result.is_ok());

        epoch_milliseconds()
    };

    sleep_ms(100);

    // delete file
    {
        let first_path = source_path.join(first_file_name);

        remove_file(&first_path).unwrap();

        let backup_result = backbonzo::backup(
            source_path.clone(),
            1000000,
            &crypto_scheme,
            max_age_milliseconds,
            deadline
        );

        assert!(backup_result.is_ok());
    }

    // restore to second state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().to_owned();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            &crypto_scheme,
            second_timestamp + 1,
            "**"
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(second_path.exists());
        assert!(! first_path.exists());

        let mut file = open_read_write(&second_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(mixed_message, &contents[..]);
    }

    // restore to third state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().to_owned();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            &crypto_scheme,
            third_timestamp + 1,
            "**"
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!( ! second_path.exists());
        assert!(first_path.exists());

        let mut file = open_read_write(&first_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(&mixed_message[..], &contents[..]);
    }

    // restore to last state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().to_owned();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            &crypto_scheme,
            epoch_milliseconds(),
            "**"
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
        let restore_path = restore_temp.path().to_owned();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            &crypto_scheme,
            first_timestamp + 1,
            "**"
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(! second_path.exists());
        assert!(first_path.exists());

        let mut file = open_read_write(&first_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        assert_eq!(&first_message[..], &contents[..]);
    }

    // restore to initial state
    {
        let restore_temp = TempDir::new("rename-store").unwrap();
        let restore_path = restore_temp.path().to_owned();

        let restore_result = backbonzo::restore(
            restore_path.clone(),
            destination_path.clone(),
            &crypto_scheme,
            5000,
            "**"
        );

        assert!(restore_result.is_ok());

        let first_path = restore_path.join(first_file_name);
        let second_path = restore_path.join(second_file_name);

        assert!(! second_path.exists());
        assert!(! first_path.exists());
    }
}
