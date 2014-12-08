#![feature(phase)]

extern crate backbonzo;
extern crate serialize;
extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;

use docopt::Docopt;
use backbonzo::{init, update, restore, BonzoError, BonzoResult};

docopt!(Args deriving Show, "
backbonzo

Usage:
  backbonzo OPERATION [options]

Operations:
  init, backup, restore
  
Options:
  -h --help                 Show this screen.
  -d --destination=<dest>   Output directory (later mandatory).
  -k --key=<key>            Encryption key. 
  -b --blocksize=<bs>       Size of blocks in megabytes [default: 1].
", arg_OPERATION: Operation)

static DATABASE_FILENAME: &'static str = "index.db3";
static TEMP_INPUT_DIRECTORY: &'static str = ".";

#[deriving(Show, Decodable)]
enum Operation {
    Init,
    Backup,
    Restore
}

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    
    let path = Path::new(TEMP_INPUT_DIRECTORY);
    let mut database_path = path.clone();
    database_path.push(DATABASE_FILENAME);

    let result = match args.arg_OPERATION {
        Operation::Init    => init(&database_path),
        Operation::Restore => restore(&database_path),
        Operation::Backup  => update(&path, &database_path)
    };
    
    handle_result(result);
}

fn handle_result<T>(result: BonzoResult<T>) {
    match result {
        Ok(..)                       => println!("Done!"),
        Err(BonzoError::Database(e)) => println!("Database error: {}", e.message),
        Err(BonzoError::Io(e))       => println!("IO error: {}", e.desc),
        Err(BonzoError::Crypto(..))  => println!("Crypto error!"),
        Err(BonzoError::Other(str))  => println!("Error: {}", str)
    }
}
