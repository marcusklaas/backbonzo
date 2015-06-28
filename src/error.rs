use std::error::Error;
use std::convert::From;
use std::io;
use std::fmt;
use std::path::PathBuf;

use super::crypto::CryptoError;
use super::database::DatabaseError;

pub enum BonzoError {
    Database(DatabaseError),
    Io(io::Error, Option<PathBuf>),
    Crypto(CryptoError),
    Other(String)
}

impl BonzoError {
    pub fn from_str(slice: &str) -> BonzoError {
        BonzoError::Other(slice.to_string())
    }
}

// TODO: implement!
impl Error for BonzoError {
    fn description(&self) -> &str {
        match self {
            &BonzoError::Database(ref e) => e.description(),
            _                            => ""
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &BonzoError::Database(ref e) => Some(e),
            _                            => None
        }
    }
}

impl From<io::Error> for BonzoError {
    fn from(error: io::Error) -> BonzoError {
        BonzoError::Io(error, None)
    }
}

impl From<CryptoError> for BonzoError {
    fn from(error: CryptoError) -> BonzoError {
        BonzoError::Crypto(error)
    }
}

impl From<DatabaseError> for BonzoError {
    fn from(error: DatabaseError) -> BonzoError {
        BonzoError::Database(error)
    }
}

impl fmt::Debug for BonzoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BonzoError::Database(ref e)     => write!(f, "Database error: {}", e),
            BonzoError::Io(ref e, ref path) => write!(f,
                                                      "IO error ({:?}) ({:?}): {}, {}",
                                                      path,
                                                      e.kind(),
                                                      <io::Error as Error>::description(e),
                                                      e.to_string()),
            BonzoError::Crypto(ref e)       => write!(f, "Crypto error: {}", e),
            BonzoError::Other(ref str)      => write!(f, "Error: {}", str)
        }
    }
}

impl fmt::Display for BonzoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

pub type BonzoResult<T> = Result<T, BonzoError>;

macro_rules! try_io {
    ($expr:expr, $f:expr) => (match $expr {
        Ok(val) => val,
        Err(err) => {
            return Err(BonzoError::Io(err, Some(::std::convert::From::from($f))))
        }
    })
}
