use super::rust_crypto::aes::{cbc_decryptor, cbc_encryptor, KeySize};
use super::rust_crypto::digest::Digest;
use super::rust_crypto::buffer::{RefReadBuffer, RefWriteBuffer, WriteBuffer, ReadBuffer, BufferResult};
use super::rust_crypto::blockmodes::PkcsPadding;
use super::rust_crypto::sha2::Sha256;
use super::rust_crypto::scrypt::{scrypt_simple, scrypt_check, ScryptParams};
use super::rust_crypto::pbkdf2::pbkdf2;
use super::rust_crypto::hmac::Hmac;
use super::rust_crypto::symmetriccipher::SymmetricCipherError;

use super::export::Blocks;
use std::io::IoResult;
use std::iter::repeat;

macro_rules! do_while_match (($b: block, $e: pat) => (while let $e = $b {}));

// Hashes a string using a strong cryptographic, including parameters
// and salt in the result
pub fn hash_password(password: &str) -> IoResult<String> {
    let params = ScryptParams::new(12, 6, 1);

    scrypt_simple(password, &params)
}

// Checks if a hash generated with hash_password matches a password
pub fn check_password(password: &str, hash: &str) -> bool {
    match scrypt_check(password, hash) {
        Err(..)  => false,
        Ok(bool) => bool
    }
}

// Turns a string into a 256 bit key that we can use for {en,de}cryption. It is
// important that we use an algorithm that is not similar to the one to hash
// the password for storage. One could otherwise use the stored hash to gain
// information on the key used for {en,de}cryption.
pub fn derive_key(password: &str) -> Vec<u8> {
    let salt = [0; 16];
    let mut derived_key = repeat(0).take(32).collect::<Vec<u8>>();
    let mut mac = Hmac::new(Sha256::new(), password.as_bytes());
    
    pbkdf2(&mut mac, &salt, 100000, derived_key.as_mut_slice());

    derived_key
}

// Returns the SHA256 hash of a file in hex encoding
pub fn hash_file(path: &Path) -> IoResult<String> {    
    let mut hasher = Sha256::new();
    let mut blocks = try!(Blocks::from_path(path, 1024));
    
    while let Some(slice) = blocks.next() {
        hasher.input(slice);
    }
    
    Ok(hasher.result_str())
}

// Returns the SHA256 hash of a slice of bytes in hex encoding
pub fn hash_block(block: &[u8]) -> String {
    let mut hasher = Sha256::new();
    
    hasher.input(block);
    hasher.result_str()
}

// FIXME: maybe we can take a Box<[u8; 32]> and Box<[u8; 16]> to enforce proper length of key/ iv
// and we should still refactor this so it shares less code with decrypt_block
pub fn encrypt_block(block: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    if key.len() != 32 || iv.len() != 16 {
        return Err(SymmetricCipherError::InvalidLength);
    }
    
    let mut encryptor = cbc_encryptor(KeySize::KeySize256, key, iv, PkcsPadding);
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
pub fn decrypt_block(block: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    if key.len() != 32 || iv.len() != 16 {
        return Err(SymmetricCipherError::InvalidLength); // FIXME: is this correct error? not clear which one is wrong length.
    }
    
    let mut decryptor = cbc_decryptor(KeySize::KeySize256, key, iv, PkcsPadding);
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
    use std::rand::{Rng, OsRng};
    use std::io::TempDir;
    use std::io::fs::File;
    
    #[test]
    fn aes_encryption_decryption() {
        let mut data: [u8; 100000] = [0; 100000];
        let mut key: [u8; 32] = [0; 32];
        let mut iv: [u8; 16] = [0; 16];
        let mut rng = OsRng::new().ok().unwrap();
        
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut iv);
        rng.fill_bytes(&mut data);

        let encrypted_data = super::encrypt_block(&data, &key, &iv).ok().unwrap();
        let decrypted_data = super::decrypt_block(encrypted_data.as_slice(), &key, &iv).ok().unwrap();

        assert!(data.as_slice() == decrypted_data.as_slice());
    }

    #[test]
    fn decryption_bad_key() {
        let message = "hello, world!";
        let key = [0u8; 32];
        let bad_key = [1u8; 32];
        let iv = [0u8; 16];
        let bad_iv = [3u8; 16];

        let encrypted_data = super::encrypt_block(message.as_bytes(), &key, &iv).ok().unwrap();
        
        let bad_decrypt = super::decrypt_block(encrypted_data.as_slice(), &bad_key, &iv);
        let good_decrypt = super::decrypt_block(encrypted_data.as_slice(), &key, &iv);

        assert!(bad_decrypt.is_err());
        assert!(good_decrypt.is_ok());

        let bad_iv_decrypt = super::decrypt_block(encrypted_data.as_slice(), &key, &bad_iv);

        assert!(bad_iv_decrypt.ok().unwrap().as_slice() != message.as_bytes());
    }

    #[test]
    fn encryption_bad_iv() {
        let message = "hello, world!";
        let key = [1u8; 32];
        let iv = [0u8; 32];
        
        let bad_encrypt = super::encrypt_block(message.as_bytes(), &key, &iv);

        assert!(bad_encrypt.is_err());
    }

    #[test]
    fn key_derivation() {
        let key: Vec<u8> = super::derive_key("test");

        assert_eq!(32u, key.len());

        let key_two = super::derive_key("testk");

        assert_eq!(32u, key.len());

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

        let _ = file.write("test".as_bytes()).unwrap();
        let _ = file.fsync().unwrap();

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

    #[test]
    fn hash_password() {
        let pass_a = "password123";
        let pass_b = "different_pass";

        let first_hash = super::hash_password(pass_a).unwrap();
        let second_hash = super::hash_password(pass_a).unwrap();

        assert!(first_hash.as_slice() != second_hash.as_slice()); // it is extremely unlikely for the salts to be equal

        let different_hash = super::hash_password(pass_b).unwrap();

        assert!(super::check_password(pass_a, first_hash.as_slice()));
        assert!(super::check_password(pass_a, second_hash.as_slice()));

        assert!(super::check_password(pass_b, different_hash.as_slice()));
        
        assert!( ! super::check_password(pass_b, first_hash.as_slice()));
        assert!( ! super::check_password(pass_b, second_hash.as_slice()));
        
        assert!( ! super::check_password(pass_a, different_hash.as_slice()));
    }
}
