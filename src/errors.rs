use std::{fmt, io};

/// The type alias for the error type used in the bredis crate.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Represents the possible errors that can occur in the database.
#[derive(Debug)]
pub enum DatabaseError {
    /// Failed to initialize the database.
    InitialFailed(String),
    /// Invalid value type for a key in the database.
    InvalidValueType(String),
    /// Value not found in the database.
    ValueNotFound(String),
    /// Internal error occurred in the database.
    InternalError(String),
}

// Implement the Display trait for the DatabaseError enum.
impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InitialFailed(err) => write!(f, "Failed to initialize database: {err}"),
            Self::InvalidValueType(type_) => {
                write!(f, "Invalid value type: {type_}")
            }
            Self::ValueNotFound(key) => write!(f, "Value not found for key: {key}"),
            Self::InternalError(err) => write!(f, "Internal error: {err}"),
        }
    }
}

// Implement the Error trait for the DatabaseError enum.
impl std::error::Error for DatabaseError {}

// Implement the From trait for converting a rocksdb::Error to a DatabaseError.
impl From<rocksdb::Error> for DatabaseError {
    fn from(err: rocksdb::Error) -> Self {
        Self::InternalError(err.to_string())
    }
}

// Implement the From trait for converting an io::Error to a DatabaseError.
impl From<io::Error> for DatabaseError {
    fn from(err: io::Error) -> Self {
        Self::InternalError(err.to_string())
    }
}

// Implement the From trait for converting a String to a DatabaseError.
impl From<String> for DatabaseError {
    fn from(err: String) -> Self {
        Self::InternalError(err)
    }
}
