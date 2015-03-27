extern crate crypto as rust_crypto;

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

macro_rules! do_while_match (($b: block, $e: pat) => (while let $e = $b {}));

#[derive(Debug)]
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
    fn from_error(_: SymmetricCipherError) -> CryptoError {
        CryptoError
    }
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

pub trait CryptoScheme: Send + Sync + Copy {
    fn hash_password(&self) -> String;

    fn encrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError>;

    fn decrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError>;
}

#[derive(Copy)]
pub struct AesEncrypter {
    key: [u8; 32]
}

impl AesEncrypter {
    pub fn new(password: &str) -> AesEncrypter {
        let mut scheme = AesEncrypter {
            key: [0; 32]
        };

        let salt = [0; 16];
        let mut mac = Hmac::new(Sha256::new(), password.as_bytes());

        pbkdf2(&mut mac, &salt, 100000, &mut scheme.key);

        scheme
    }
}

unsafe impl Send for AesEncrypter {}
unsafe impl Sync for AesEncrypter {}

impl CryptoScheme for AesEncrypter {
    fn hash_password(&self) -> String {
        let mut hasher = Sha256::new();
    
        hasher.input(&self.key);
        hasher.result_str()
    }

    fn encrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let iv: [u8; 16] = [0; 16];
        let mut encryptor = cbc_encryptor(KeySize::KeySize256, &self.key, &iv, PkcsPadding);
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

    fn decrypt_block(&self, block: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let iv: [u8; 16] = [0; 16];
        let mut decryptor = cbc_decryptor(KeySize::KeySize256, &self.key, &iv, PkcsPadding);
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
}

pub trait HashScheme {
    fn hash_block(&self, block: &[u8]) -> String;

    fn hash_file(&self, path: &Path) -> io::Result<String>;
}

pub struct Sha256Hasher;

impl HashScheme for Sha256Hasher {
    fn hash_file(&self, path: &Path) -> io::Result<String> {
        let mut chunks = try!(file_chunks(path, 1024));
        let mut hasher = Sha256::new();
        
        while let Some(slice) = chunks.next() {
            let unwrapped_slice = try!(slice);
            
            hasher.input(unwrapped_slice);
        }
        
        Ok(hasher.result_str())
    }

    fn hash_block(&self, block: &[u8]) -> String {
        let mut hasher = Sha256::new();
        
        hasher.input(block);
        hasher.result_str()
    }
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

#[cfg(test)]
mod test {
    use super::super::rand::{Rng, OsRng};
    use super::super::tempdir::TempDir;
    use super::{CryptoScheme, AesEncrypter};
    
    use std::fs::File;
    use std::io::Write;
    
    #[test]
    fn aes_encryption_decryption() {
        let mut data: [u8; 100000] = [0; 100000];
        let mut key: [u8; 500] = [0; 500];
        let mut rng = OsRng::new().ok().unwrap();

        rng.fill_bytes(&mut data);
        rng.fill_bytes(&mut key);

        let scheme = AesEncrypter::new(&String::from_utf8_lossy(&key));
        let index = rng.gen::<u32>() % 100000;
        let slice = &data[0..index as usize];
        let encrypted_data = scheme.encrypt_block(slice).ok().unwrap();
        let decrypted_data = scheme.decrypt_block(&encrypted_data).ok().unwrap();

        assert!(slice == &decrypted_data[..]);
    }

    #[test]
    fn decryption_bad_key() {
        let message = b"hello, world!";
        let scheme = AesEncrypter::new("test");
        let bad_scheme = AesEncrypter::new("hallo");

        let encrypted_data = scheme.encrypt_block(message).ok().unwrap();
        
        let bad_decrypt = bad_scheme.decrypt_block(&encrypted_data);
        let good_decrypt = scheme.decrypt_block(&encrypted_data);

        assert!(bad_decrypt.is_err());
        assert!(good_decrypt.is_ok());
    }

    #[test]
    fn key_derivation() {
        let key = AesEncrypter::new("test").hash_password();
        let key_two = AesEncrypter::new("testk").hash_password();

        assert!(key != key_two);
    }

    #[test]
    fn hash_file() {
        let temp_dir = TempDir::new("hash-test").unwrap();
        let file_path = temp_dir.path().join("test");
        let mut file = File::create(&file_path).unwrap();

        let expected_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let hash = super::hash_file(&file_path).unwrap();

        assert_eq!(expected_hash, &hash[..]);

        let _ = file.write_all("test".as_bytes()).unwrap();
        let _ = file.sync_all().unwrap();

        let new_expected_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        let new_hash = super::hash_file(&file_path).unwrap();

        assert_eq!(new_expected_hash, &new_hash[..]);

        let non_existant_path = temp_dir.path().join("no-exist");

        assert!(super::hash_file(&non_existant_path).is_err());
    }

    #[test]
    fn hash_block() {
        let expected_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        let hash = super::hash_block("test".as_bytes());

        assert_eq!(expected_hash, &hash[..]);
    }
}
