#![feature(phase)]

extern crate backbonzo;
extern crate serialize;
extern crate docopt;
extern crate time;
#[phase(plugin)] extern crate docopt_macros;

use docopt::Docopt;
use backbonzo::{BackupManager, BonzoError, BonzoResult};

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
static TEMP_OUTPUT_DIRECTORY: &'static str = "/tmp/backbonzo/";
static TEMP_RESTORE_DIRECTORY: &'static str = "/tmp/backbonzo-restore/";

#[deriving(Show, Decodable)]
enum Operation {
    Init,
    Backup,
    Restore
}

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    
    let input_path = Path::new(TEMP_INPUT_DIRECTORY);
    let output_path = Path::new(TEMP_OUTPUT_DIRECTORY);
    
    let mut database_path = input_path.clone();
    database_path.push(DATABASE_FILENAME);

    let result = match args.arg_OPERATION {
        Operation::Init    => {
            BackupManager::init(&database_path)
        },
        Operation::Restore => {
            let restore_path = Path::new(TEMP_RESTORE_DIRECTORY);
            let manager = BackupManager::new(database_path, restore_path, output_path, Vec::from_elem(32, 0)).ok().unwrap();
            
            manager.restore(time::get_time().sec as u64)
        },
        Operation::Backup  => {
            let manager = BackupManager::new(database_path, input_path, output_path, Vec::from_elem(32, 0)).ok().unwrap();
            
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
