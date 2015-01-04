#![feature(phase)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate backbonzo;
extern crate serialize;
extern crate docopt;
extern crate time;
extern crate regex;
#[phase(plugin)] extern crate docopt_macros;

use docopt::Docopt;
use std::time::duration::Duration;
use backbonzo::{init, backup, restore, BonzoError, BonzoResult};

docopt!(Args deriving Show, "
backbonzo

Usage:
  backbonzo OPERATION [options] --key <key>
  backbonzo (-h | --help)

Operations:
  init, backup, restore
  
Options:
  -s --source=<source>       Source directory [default: ./].
  -d --destination=<dest>    Backup directory [default: /tmp/backbonzo/].
  -k --key=<key>             Encryption key.
  -b --blocksize=<bs>        Size of blocks in megabytes [default: 1].
  -t --timestamp=<mseconds>  State to restore to in milliseconds since epoch [default: 0].
  -T --timeout=<seconds>     Maximum execution time in seconds [default: 0].
  -f --filter=<exp>          Regular expression for paths [default: **].
", arg_OPERATION: Operation, flag_blocksize: u32, flag_key: String, flag_timestamp: u64, flag_timeout: u64, flag_filter: String);

static DATABASE_FILENAME: &'static str = "index.db3";

#[deriving(Show, RustcDecodable)]
enum Operation {
    Init,
    Backup,
    Restore
}

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let source_path = Path::new(args.flag_source);
    let backup_path = Path::new(args.flag_destination);
    let database_path = source_path.join(DATABASE_FILENAME);
    let block_bytes = 1000 * 1000 * args.flag_blocksize;
    let deadline = time::now() + match args.flag_timeout {
        0    => Duration::weeks(52),
        secs => Duration::seconds(secs as i64)
    };
    let timestamp = match args.flag_timestamp {
        0 => 1000 * time::get_time().sec as u64,
        v => v
    };

    let result = match args.arg_OPERATION {
        Operation::Init    => init(database_path, args.flag_key),
        Operation::Restore => restore(source_path, backup_path, args.flag_key, timestamp, args.flag_filter),
        Operation::Backup  => backup(database_path, source_path, backup_path, block_bytes, args.flag_key, deadline)
    };
    
    handle_result(result);
}

fn handle_result<T>(result: BonzoResult<T>) {
    let mut stderr = std::io::stderr();
    
    match result {
        Ok(..)                       => println!("Done!"),
        Err(BonzoError::Database(e)) => { let _ = writeln!(&mut stderr, "Database error: {}", e.message); },
        Err(BonzoError::Io(e))       => { let _ = writeln!(&mut stderr, "IO error: {}", e.desc); },
        Err(BonzoError::Crypto(..))  => { let _ = writeln!(&mut stderr, "Crypto error!"); },
        Err(BonzoError::Other(str))  => { let _ = writeln!(&mut stderr, "Error: {}", str) ; }
    }
}
