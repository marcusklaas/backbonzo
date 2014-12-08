use super::rust_crypto::{aes, buffer, symmetriccipher};
use super::rust_crypto::digest::Digest;
use super::rust_crypto::buffer::{WriteBuffer, ReadBuffer};
use super::rust_crypto::blockmodes::PkcsPadding;
use super::rust_crypto::sha2::Sha256;
use super::rust_crypto::symmetriccipher::SymmetricCipherError;

use super::Blocks;
use std::io::IoResult;

static TEST_KEY: &'static str = "testkey123";

pub fn hash_file(path: &Path) -> IoResult<String> {
    let mut hasher = Sha256::new();
    let mut blocks = try!(Blocks::from_path(path, 1024));
    
    loop {
        match blocks.next() {
            Some(slice) => hasher.input(slice),
            None        => break
        }
    }
    
    Ok(hasher.result_str())
}

pub fn hash_block(block: &[u8]) -> String {
    let mut hasher = Sha256::new();
    
    hasher.input(block);
    
    hasher.result_str()
}

pub fn encrypt_block(block: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    let mut encryptor: Box<symmetriccipher::Encryptor> = aes::cbc_encryptor(
        aes::KeySize::KeySize256,
        TEST_KEY.as_bytes(),
        &[0, ..16],
        PkcsPadding
    );
    
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0, ..4096];
    let mut read_buffer = buffer::RefReadBuffer::new(block);
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);
    
    while !read_buffer.is_empty() {
        try!(encryptor.encrypt(&mut read_buffer, &mut write_buffer, true));        
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
    }
    
    Ok(final_result)
}

// FIXME: lots of duplicate code with function above -- can we refactor?
pub fn decrypt_block(block: &[u8]) -> Result<Vec<u8>, SymmetricCipherError> {
    let mut decryptor: Box<symmetriccipher::Decryptor> = aes::cbc_decryptor(
        aes::KeySize::KeySize256,
        TEST_KEY.as_bytes(),
        &[0, ..16],
        PkcsPadding
    );
    
    let mut final_result = Vec::<u8>::new();
    let mut buffer = [0, ..4096];
    let mut read_buffer = buffer::RefReadBuffer::new(block);
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);
    
    while !read_buffer.is_empty() {
        try!(decryptor.decrypt(&mut read_buffer, &mut write_buffer, true));        
        final_result.push_all(write_buffer.take_read_buffer().take_remaining());
    }
    
    Ok(final_result)
}

#[cfg(test)]
mod test {
    #[test]
    fn aes_encryption_decryption() {
        let block = [13u8, ..(1024*52)];

        let encrypted_bytes: Vec<u8> = super::encrypt_block(&block).ok().unwrap();

        let decrypted_bytes = super::decrypt_block(encrypted_bytes.as_slice()).ok().unwrap();
        
        assert_eq!(block.len(), decrypted_bytes.len());

        for (x, y) in block.iter().zip(decrypted_bytes.as_slice().iter()) {
            assert_eq!(*x, *y);
        }
    }
}
