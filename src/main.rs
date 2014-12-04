extern crate backbonzo;

fn main() {
    match backbonzo::init() {
        Ok(..)   => println!("Done!"),
        Err(msg) => println!("Failed: {}.", msg)
    }
}
