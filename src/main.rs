#![cfg(not(test))]
#![feature(libc)]

extern crate rustc_serialize;
extern crate backbonzo;
extern crate docopt;
extern crate time;
extern crate termios;
extern crate libc;

use docopt::Docopt;
use std::path::PathBuf;
use std::error::Error;
use time::Duration;
use std::fmt::Display;
use std::io::{Write, stderr, stdout, stdin};
use backbonzo::{init, backup, restore, epoch_milliseconds, BonzoResult, AesEncrypter};

static USAGE: &'static str = "
backbonzo

Usage:
  backbonzo init    -d <dest> [options]
  backbonzo backup            [options]
  backbonzo restore -d <dest> [options]
  backbonzo --help

Options:
  -s --source=<source>       Source directory [default: ./].
  -d --destination=<dest>    Backup directory.
  -b --blocksize=<bs>        Size of blocks in kilobytes [default: 1000].
  -t --timestamp=<mseconds>  State to restore to in milliseconds since epoch [default: 0].
  -T --timeout=<seconds>     Maximum execution time in seconds [default: 0].
  -f --filter=<exp>          Glob expression for paths to restore [default: **].
  -a --age=<days>            Number of days to retain old data [default: 183].
";

#[derive(RustcDecodable, Debug)]
struct Args {
    pub cmd_init: bool,
    pub cmd_backup: bool,
    pub cmd_restore: bool,
    pub flag_destination: String,
    pub flag_source: String,
    pub flag_blocksize: u32,
    pub flag_timestamp: u64,
    pub flag_timeout: u64,
    pub flag_filter: String,
    pub flag_age: u32
}

fn fetch_password() -> String {
    let optional_term = termios::Termios::from_fd(0).ok();

    if let Some(mut term) = optional_term {
        term.c_lflag &= !termios::ECHO;
        term.c_lflag |= termios::ECHONL;

        termios::tcsetattr(0, termios::TCSANOW, &term).unwrap();

        print!("Passphrase: ");
        stdout().flush().unwrap();
    }

    let mut password = String::new();
    stdin().read_line(&mut password).unwrap();

    if let Some(term) = optional_term {
        termios::tcsetattr(0, termios::TCSANOW, &term).unwrap();
    }

    password.pop().unwrap();

    password
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());
    let password = fetch_password();
    let crypto_scheme = AesEncrypter::new(&password);

    if args.cmd_init {
        let result = init(&args.flag_source, &args.flag_destination, &crypto_scheme);
        handle_result(result);
    }
    else if args.cmd_backup {
        let deadline = time::now() + match args.flag_timeout {
            0    => Duration::weeks(52),
            secs => Duration::seconds(secs as i64)
        };
        let max_alias_age_milliseconds = args.flag_age as u64 * 24 * 60 * 60 * 1000;
        let block_bytes = 1000 * (args.flag_blocksize as usize);

        let result = backup(PathBuf::from(args.flag_source), block_bytes, &crypto_scheme, max_alias_age_milliseconds, deadline);
        handle_result(result);
    }
    else if args.cmd_restore {
        let timestamp = match args.flag_timestamp {
            0 => epoch_milliseconds(),
            v => v
        };

        let result = restore(PathBuf::from(args.flag_source), PathBuf::from(args.flag_destination), &crypto_scheme, timestamp, args.flag_filter);
        handle_result(result);
    }
}

// Writes the result of the program to stdio in case of success, or stderr when
// it failed
fn handle_result<T: Display>(result: BonzoResult<T>) {
    match result {
        Ok(summary) => println!("{}", summary),
        Err(ref e)  => { let _ = writeln!(&mut stderr(), "{:?}", e); }
    }
}
