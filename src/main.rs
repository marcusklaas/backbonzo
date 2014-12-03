extern crate backup;

fn main() {
    match backup::init() {
        Ok(..)   => println!("Done!"),
        Err(msg) => println!("Failed: {}.", msg)
    }
}
