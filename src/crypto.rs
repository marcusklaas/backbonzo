use super::rust_crypto::aes::{cbc_decryptor, cbc_encryptor, KeySize};
use super::rust_crypto::digest::Digest;
use super::rust_crypto::buffer::{RefReadBuffer, RefWriteBuffer, WriteBuffer, ReadBuffer, BufferResult};
use super::rust_crypto::blockmodes::PkcsPadding;
use super::rust_crypto::sha2::Sha256;
use super::rust_crypto::symmetriccipher::SymmetricCipherError;
use super::rust_crypto::scrypt::{scrypt_simple, scrypt_check, ScryptParams};
use super::rust_crypto::pbkdf2::pbkdf2;
use super::rust_crypto::hmac::Hmac;

use super::Blocks;
use std::io::IoResult;

pub fn hash_password(password: &str) -> IoResult<String> {
    let params = ScryptParams::new(12, 6, 1);

    scrypt_simple(password, &params)
}

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
    let salt = [0, ..16];
    let mut derived_key = Vec::from_elem(32, 0);
    let mut mac = Hmac::new(Sha256::new(), password.as_bytes());
    
    pbkdf2(&mut mac, &salt, 100000, derived_key.as_mut_slice());

    derived_key
}

pub fn hash_file(path: &Path) -> IoResult<String> {
    let mut hasher = Sha256::new();
    let mut blocks = try!(Blocks::from_path(path, 1024));
    
    while let Some(slice) = blocks.next() {
        hasher.input(slice);
    }
    
    Ok(hasher.result_str())
}

pub fn hash_block(block: &[u8]) -> String {
    let mut hasher = Sha256::new();
    
    hasher.input(block);
    
    hasher.result_str()
}

pub fn encrypt_block(block: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    let mut encryptor = cbc_encryptor(KeySize::KeySize256, key, iv, PkcsPadding);
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0, ..4096];
    let mut read_buffer = RefReadBuffer::new(block);
    let mut write_buffer = RefWriteBuffer::new(&mut buffer);

    while let BufferResult::BufferOverflow = try!(encryptor.encrypt(&mut read_buffer, &mut write_buffer, true)) {
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
    }

    final_result.push_all(write_buffer.take_read_buffer().take_remaining());

    Ok(final_result)
} 

pub fn decrypt_block(block: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    let mut decryptor = cbc_decryptor(KeySize::KeySize256, key, iv, PkcsPadding);
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0, ..4096];
    let mut read_buffer = RefReadBuffer::new(block);
    let mut write_buffer = RefWriteBuffer::new(&mut buffer);

    while let BufferResult::BufferOverflow = try!(decryptor.decrypt(&mut read_buffer, &mut write_buffer, true)) {
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
    }

    final_result.push_all(write_buffer.take_read_buffer().take_remaining());

    Ok(final_result)
}

#[cfg(test)]
mod test {
    use std::rand::{Rng, OsRng};
    
    #[test]
    fn aes_encryption_decryption() {
        let message = "Hello World!";
        let mut key: [u8, ..32] = [0, ..32];
        let mut iv: [u8, ..16] = [0, ..16];
        let mut rng = OsRng::new().ok().unwrap();
        
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut iv);

        let encrypted_data = super::encrypt_block(message.as_bytes(), &key, &iv).ok().unwrap();
        let decrypted_data = super::decrypt_block(encrypted_data.as_slice(), &key, &iv).ok().unwrap();

        assert!(message.as_bytes() == decrypted_data.as_slice());
    }
}
