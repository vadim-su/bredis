use serde::{Deserialize, Serialize};

use crate::errors::DatabaseError;

#[allow(clippy::module_name_repetitions)]
/// A struct to represent a value in the database
/// This struct is used to store the value type and the time-to-live (TTL) for the value
/// The value is stored as a byte array
/// The struct can be serialized and deserialized to/from a binary representation
///
/// # Example
/// ```
/// let storage_value = StorageValue {
///   value_type: ValueType::String,
///   ttl: 1000,
///   value: b"my_value".to_vec(),
/// };
/// let binary = storage_value.to_binary();
/// let storage_value = StorageValue::from_binary(&binary);
/// ```
///
/// # Fields
/// * `value_type` - The type of the value
/// * `ttl` - The time-to-live (TTL) for the value
/// * `value` - The value as a byte array
#[derive(Clone, Serialize, Deserialize)]
pub struct StorageValue {
    pub value_type: ValueType,
    pub ttl: i64,
    pub value: Vec<u8>,
}

impl StorageValue {
    /// Create a new `StorageValue` instance
    /// # Returns
    /// The `StorageValue` instance
    pub fn to_binary(&self) -> Vec<u8> {
        return bincode::serialize(&self).unwrap();
    }

    /// Create a new `StorageValue` instance from a binary representation
    /// # Arguments
    /// * `data` - The binary representation of the `StorageValue`
    /// # Returns
    /// The `StorageValue` instance
    pub fn from_binary(data: &[u8]) -> Self {
        return bincode::deserialize(data).unwrap();
    }

    /// Get the value as a Integer
    ///
    /// # Returns
    /// Result containing the integer value or an error
    ///
    /// # Example
    /// ```
    /// let storage_value = StorageValue {
    ///  value_type: ValueType::Integer,
    ///  ttl: 1000,
    ///  value: b"123".to_vec(),
    /// };
    /// let value = storage_value.get_integer_value().unwrap();
    /// ```
    pub fn get_integer_value(&self) -> Result<i64, DatabaseError> {
        if self.value_type != ValueType::Integer {
            return Err(DatabaseError::InvalidValueType(
                "Value is not an integer".to_string(),
            ));
        }

        let string_value = String::from_utf8(self.value.clone());
        if string_value.is_err() {
            return Err(DatabaseError::InternalError(
                "Failed to parse integer value".to_string(),
            ));
        }

        let value = string_value.unwrap().parse();
        match value {
            Ok(value) => return Ok(value),
            Err(err) => {
                return Err(DatabaseError::InternalError(format!(
                    "Failed to parse integer value: {err}"
                )));
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
/// Value types supported by the database
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ValueType {
    String,
    Integer,
}

impl From<ValueType> for String {
    fn from(value: ValueType) -> Self {
        return match value {
            ValueType::String => Self::from("String"),
            ValueType::Integer => Self::from("Integer"),
        };
    }
}
