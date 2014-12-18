#![feature(phase)]

extern crate backbonzo;
extern crate serialize;
extern crate docopt;
extern crate time;
#[phase(plugin)] extern crate docopt_macros;

use docopt::Docopt;
use backbonzo::{init, BackupManager, BonzoError, BonzoResult};

docopt!(Args deriving Show, "
backbonzo

Usage:
  backbonzo OPERATION [options] --key <key>
  backbonzo (-h | --help)

Operations:
  init, backup, restore
  
Options:
  -s --source=<source>      Source directory [default: ./].
  -d --destination=<dest>   Backup directory [default: /tmp/backbonzo/].
  -k --key=<key>            Encryption key. 
  -b --blocksize=<bs>       Size of blocks in megabytes [default: 1].
", arg_OPERATION: Operation, flag_blocksize: uint, flag_key: String)

static DATABASE_FILENAME: &'static str = "index.db3";

#[deriving(Show, Decodable)]
enum Operation {
    Init,
    Backup,
    Restore
}

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    
    let source_path = Path::new(args.flag_source);
    let output_path = Path::new(args.flag_destination);
    let block_bytes = 1000 * 1000 * args.flag_blocksize;
    
    let mut database_path = source_path.clone();
    database_path.push(DATABASE_FILENAME);

    let result = match args.arg_OPERATION {
        Operation::Init    => {
            init(&database_path, args.flag_key)
        },
        Operation::Restore => {
            let manager = BackupManager::new(database_path, source_path, output_path, block_bytes, Vec::from_elem(32, 0)).ok().unwrap();
            
            manager.restore(time::get_time().sec as u64)
        },
        Operation::Backup  => {
            let manager = BackupManager::new(database_path, source_path, output_path, block_bytes, Vec::from_elem(32, 0)).ok().unwrap();
            
            manager.update()
        }
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
