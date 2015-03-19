extern crate "crypto" as rust_crypto;

use self::rust_crypto::aes::{cbc_decryptor, cbc_encryptor, KeySize};
use self::rust_crypto::digest::Digest;
use self::rust_crypto::buffer::{RefReadBuffer, RefWriteBuffer, WriteBuffer, ReadBuffer, BufferResult};
use self::rust_crypto::blockmodes::PkcsPadding;
use self::rust_crypto::sha2::Sha256;
use self::rust_crypto::pbkdf2::pbkdf2;
use self::rust_crypto::hmac::Hmac;
use self::rust_crypto::symmetriccipher::SymmetricCipherError;

use super::file_chunks::file_chunks;
use std::path::Path;
use std::io;
use std::fmt;
use std::error::{FromError, Error};

pub struct CryptoError;

impl Error for CryptoError {
    fn description(&self) -> &str {
        "Symmetric cipher error. Bad key?"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl FromError<SymmetricCipherError> for CryptoError {
    fn from_error(error: SymmetricCipherError) -> CryptoError {
        CryptoError
    }
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

pub trait CryptoScheme {
    fn new(password: &str) -> Self;

    fn hash_password(&self) -> String;

    fn encrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError>;

    fn decrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError>;
}

pub trait HashScheme {
    fn hash_block(&self, block: &[u8]) -> String;

    fn hash_file(&self, path: &Path) -> io::Result<String>;
}

macro_rules! do_while_match (($b: block, $e: pat) => (while let $e = $b {}));

// Hashes a string using a strong cryptographic
pub fn hash_password(password: &str) -> String {
    let key = derive_key(password);

    hash_block(&*key)
}

// Turns a string into a 256 bit key that we can use for {en,de}cryption
pub fn derive_key(password: &str) -> Box<[u8; 32]> {
    let salt = [0; 16];
    let mut derived_key = Box::new([0u8; 32]);
    let mut mac = Hmac::new(Sha256::new(), password.as_bytes());
    
    pbkdf2(&mut mac, &salt, 100000, derived_key.as_mut_slice());

    derived_key
}

// Returns the SHA256 hash of a file in hex encoding
pub fn hash_file(path: &Path) -> io::Result<String> {
    let mut chunks = try!(file_chunks(path, 1024));
    let mut hasher = Sha256::new();
    
    while let Some(slice) = chunks.next() {
        let unwrapped_slice = try!(slice);
        
        hasher.input(unwrapped_slice);
    }
    
    Ok(hasher.result_str())
}

// Returns the SHA256 hash of a slice of bytes in hex encoding
pub fn hash_block(block: &[u8]) -> String {
    let mut hasher = Sha256::new();
    
    hasher.input(block);
    hasher.result_str()
}

// FIXME: we should still refactor this so it shares less code with decrypt_block
pub fn encrypt_block(block: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {
    let iv: [u8; 16] = [0; 16];
    let mut encryptor = cbc_encryptor(KeySize::KeySize256, key.as_slice(), iv.as_slice(), PkcsPadding);
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0; 4096];
    let mut read_buffer = RefReadBuffer::new(block);
    let mut write_buffer = RefWriteBuffer::new(&mut buffer);

    do_while_match!({
        let result = try!(encryptor.encrypt(&mut read_buffer, &mut write_buffer, true));
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
        result
    }, BufferResult::BufferOverflow);

    Ok(final_result)
} 

// Decrypts a given block of AES256-CBC data using a 32 byte key and 16 byte
// initialization vector. Returns error on incorrect passwords 
pub fn decrypt_block(block: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {    
    let iv: [u8; 16] = [0; 16];
    let mut decryptor = cbc_decryptor(KeySize::KeySize256, key.as_slice(), iv.as_slice(), PkcsPadding);
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0; 4096];
    let mut read_buffer = RefReadBuffer::new(block);
    let mut write_buffer = RefWriteBuffer::new(&mut buffer);

    do_while_match!({
        let result = try!(decryptor.decrypt(&mut read_buffer, &mut write_buffer, true));
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
        result
    }, BufferResult::BufferOverflow);

    Ok(final_result)
}

#[cfg(test)]
mod test {
    use super::super::rand::{Rng, OsRng};
    use super::super::tempdir::TempDir;
    
    use std::fs::File;
    use std::io::Write;
    
    #[test]
    fn aes_encryption_decryption() {
        let mut data: [u8; 100000] = [0; 100000];
        let mut key: [u8; 32] = [0; 32];
        let mut rng = OsRng::new().ok().unwrap();

        rng.fill_bytes(&mut data);
        rng.fill_bytes(&mut key);
    
        let index = rng.gen::<u32>() % 100000;
        let slice = &data[0..index as usize];
        let encrypted_data = super::encrypt_block(slice, &key).ok().unwrap();
        let decrypted_data = super::decrypt_block(encrypted_data.as_slice(), &key).ok().unwrap();

        assert!(slice == decrypted_data.as_slice());
    }

    #[test]
    fn decryption_bad_key() {
        let message = "hello, world!";
        let key = [0u8; 32];
        let bad_key = [1u8; 32];

        let encrypted_data = super::encrypt_block(message.as_bytes(), &key).ok().unwrap();
        
        let bad_decrypt = super::decrypt_block(encrypted_data.as_slice(), &bad_key);
        let good_decrypt = super::decrypt_block(encrypted_data.as_slice(), &key);

        assert!(bad_decrypt.is_err());
        assert!(good_decrypt.is_ok());
    }

    #[test]
    fn key_derivation() {
        let key = super::derive_key("test");
        let key_two = super::derive_key("testk");

        assert!(key.as_slice() != key_two.as_slice());
    }

    #[test]
    fn hash_file() {
        let temp_dir = TempDir::new("hash-test").unwrap();
        let file_path = temp_dir.path().join("test");
        let mut file = File::create(&file_path).unwrap();

        let expected_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let hash = super::hash_file(&file_path).unwrap();

        assert_eq!(expected_hash, hash.as_slice());

        let _ = file.write_all("test".as_bytes()).unwrap();
        let _ = file.sync_all().unwrap();

        let new_expected_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        let new_hash = super::hash_file(&file_path).unwrap();

        assert_eq!(new_expected_hash, new_hash.as_slice());

        let non_existant_path = temp_dir.path().join("no-exist");

        assert!(super::hash_file(&non_existant_path).is_err());
    }

    #[test]
    fn hash_block() {
        let expected_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        let hash = super::hash_block("test".as_bytes());

        assert_eq!(expected_hash, hash.as_slice());
    }
}
