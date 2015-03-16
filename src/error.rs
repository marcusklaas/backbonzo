use std::error::{Error, FromError};
use std::io;
use std::fmt;

use super::rust_crypto::symmetriccipher::SymmetricCipherError;
use super::database::SqliteError;

pub enum BonzoError {
    Database(SqliteError),
    Io(io::Error),
    Crypto(SymmetricCipherError),
    Other(String)
}

impl BonzoError {
    pub fn from_str(slice: &str) -> BonzoError {
        BonzoError::Other(slice.to_string())
    }
}

impl FromError<io::Error> for BonzoError {
    fn from_error(error: io::Error) -> BonzoError {
        BonzoError::Io(error)
    }
}

impl FromError<SymmetricCipherError> for BonzoError {
    fn from_error(error: SymmetricCipherError) -> BonzoError {
        BonzoError::Crypto(error)
    }
}

impl FromError<SqliteError> for BonzoError {
    fn from_error(error: SqliteError) -> BonzoError {
        BonzoError::Database(error)
    }
}

impl fmt::Debug for BonzoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BonzoError::Database(ref e) => write!(f, "Database error: {}", e.message),
            BonzoError::Io(ref e)       => write!(f, "IO error ({:?}): {}, {}", e.kind(), <io::Error as Error>::description(e), e.to_string()),
            BonzoError::Crypto(..)      => write!(f, "Crypto error!"),
            BonzoError::Other(ref str)  => write!(f, "Error: {}", str)
        }
    }
}

impl fmt::Display for BonzoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

pub type BonzoResult<T> = Result<T, BonzoError>;
