use std::fs;
use std::sync::Arc;

use rocksdb::{Options, DB, DEFAULT_COLUMN_FAMILY_NAME};
use serde::{Deserialize, Serialize};

/// The byte value to search for the end of a prefix
const PREFIX_SEARCH_ENDING: u8 = 0xFF;

/// A struct to represent a Database
/// This struct is used to interact with a RocksDB database (currently)
///
/// In the future, this struct can be extended to support multiple storage backends.
///
/// # Example
/// ```
/// let db = Database::open("/dev/shm/my_storage").unwrap();
/// db.set(b"my_key", b"my_value");
/// let value = db.get(b"my_key").unwrap();
/// match value {
///   Some(value) => {
///    println!("Value: {}", String::from_utf8(value).unwrap());
///   }
///   None => {
///     println!("Value not found");
///   }
/// }
/// ```
///
/// # Fields
/// * `path` - The path to the database
/// * `store` - The RocksDB instance
pub struct Database {
    path: String,
    store: Arc<DB>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        return Database {
            path: self.path.clone(),
            store: self.store.clone(),
        };
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

impl Database {
    /// Open a new RocksDB database at the specified path
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A Result containing the Database instance or a RocksDB error
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// ```
    pub fn open(path: &str) -> Result<Self, rocksdb::Error> {
        Self::prepare_store_location(path);

        let mut options = Options::default();
        options.create_if_missing(true);
        let store = DB::open_cf(&options, path, vec![DEFAULT_COLUMN_FAMILY_NAME])?;
        return Ok(Self {
            path: path.to_string(),
            store: Arc::new(store),
        });
    }

    /// Close the database and remove the storage directory
    pub fn close(&self) {
        fs::remove_dir_all(&self.path).unwrap_or_default();
    }

    /// Get the value for a key from the database
    ///
    /// # Arguments
    /// * `key` - The key to get the value for
    ///
    /// # Returns
    /// An Option containing the value or None if the key is not found
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// let value = db.get(b"my_key").unwrap();
    /// match value {
    ///    Some(value) => {
    ///       println!("Value: {}", String::from_utf8(value).unwrap());
    ///   }
    ///  None => {
    ///     println!("Value not found");
    /// }
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<StorageValue>, rocksdb::Error> {
        let raw_value = self.store.get(key);
        match raw_value {
            Ok(value) => match value {
                Some(value) => {
                    let mut storage_value = StorageValue::from_binary(value.as_slice());

                    if storage_value.ttl > -1 {
                        storage_value.ttl = storage_value.ttl - chrono::Utc::now().timestamp();
                        if self.delete_on_ttl(&storage_value)? {
                            return Ok(None);
                        }
                    }

                    return Ok(Some(storage_value));
                }
                None => return Ok(None),
            },
            Err(err) => return Err(err),
        }
    }

    /// Get all keys in the database
    ///
    /// # Arguments
    /// * `prefix` - The prefix to filter keys by
    ///
    /// # Returns
    /// A Result containing a vector of keys or a RocksDB error
    pub fn get_all_keys(&self, prefix: &[u8]) -> Result<Vec<String>, rocksdb::Error> {
        let mut keys = Vec::new();
        let iter = self.store.prefix_iterator(prefix);
        for result in iter {
            match result {
                Ok((key, raw_value)) => {
                    let mut storage_value = StorageValue::from_binary(&raw_value);
                    if storage_value.ttl > -1 {
                        storage_value.ttl = storage_value.ttl - chrono::Utc::now().timestamp();
                        if self.delete_on_ttl(&storage_value)? {
                            continue;
                        }
                    }

                    // FIXME: This is not correct, we need to return error
                    let parsed_key = String::from_utf8(key.to_vec()).unwrap();
                    keys.push(parsed_key);
                } // Push reference to the key
                Err(err) => return Err(err),
            }
        }
        return Ok(keys);
    }

    /// Set the value for a key in the database
    ///
    /// # Arguments
    /// * `key` - The key to set the value for
    /// * `value` - The value to set
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.set(b"my_key", b"my_value");
    /// ```
    pub fn set(&self, key: &[u8], value: &StorageValue) -> Result<(), rocksdb::Error> {
        let mut value = value.clone();
        if value.ttl < 0 {
            value.ttl = -1;
        } else {
            value.ttl = chrono::Utc::now().timestamp() + value.ttl;
        }

        return self.store.put(key, value.to_binary());
    }

    /// Delete a key-value pair from the database
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.delete(b"my_key");
    /// ```
    pub fn delete(&self, key: &[u8]) -> Result<(), rocksdb::Error> {
        return self.store.delete(key);
    }

    /// Delete all keys starting with a prefix
    ///
    /// # Arguments
    /// * `prefix` - The prefix to filter keys by
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.delete_prefix(b"my_prefix");
    /// ```
    pub fn delete_prefix(&self, prefix: &[u8]) -> Result<(), rocksdb::Error> {
        let mut end_prefix = prefix.to_vec();
        end_prefix.push(PREFIX_SEARCH_ENDING);
        let cf = self.store.cf_handle(DEFAULT_COLUMN_FAMILY_NAME);
        let cf = cf.unwrap();

        return self
            .store
            .delete_range_cf(&cf, prefix, end_prefix.as_slice());
    }

    fn prepare_store_location(path: &str) {
        fs::remove_dir_all(path).unwrap_or_default();
        fs::create_dir_all(path).unwrap();
    }

    fn delete_on_ttl(&self, key: &StorageValue) -> Result<bool, rocksdb::Error> {
        if key.ttl <= 0 {
            self.delete(key.value.as_slice())?;
            return Ok(true);
        }
        return Ok(false);
    }
}

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
    /// Create a new StorageValue instance
    /// # Returns
    /// The StorageValue instance
    pub fn to_binary(&self) -> Vec<u8> {
        return bincode::serialize(&self).unwrap();
    }

    /// Create a new StorageValue instance from a binary representation
    /// # Arguments
    /// * `data` - The binary representation of the StorageValue
    /// # Returns
    /// The StorageValue instance
    pub fn from_binary(data: &[u8]) -> Self {
        return bincode::deserialize(data).unwrap();
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ValueType {
    String,
    Integer,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_keys() {
        let db = get_test_db();

        let keys = db.get_all_keys(b"prefix_").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&String::from("prefix_key1")));
        assert!(keys.contains(&String::from("prefix_key2")));
    }

    #[test]
    fn test_set() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: -1,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let raw_value = db.store.get(b"my_key").unwrap();
        let storage_value = StorageValue::from_binary(raw_value.unwrap().as_slice());
        assert_eq!(
            storage_value.value_type,
            ValueType::String,
            "Value type is incorrect"
        );
        assert_eq!(storage_value.value, b"my_value", "Value is incorrect");
        assert_eq!(storage_value.ttl, -1, "TTL is incorrect");
    }

    #[test]
    fn test_delete() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: -1,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();
        db.delete(b"my_key").unwrap();

        let value = db.store.get(b"my_key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_delete_prefix() {
        let db = get_test_db();
        db.delete_prefix(b"prefix_").unwrap();

        let keys = db.get_all_keys(b"").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&String::from("key1")));
        assert!(keys.contains(&String::from("key2")));
    }

    #[test]
    fn test_ttl() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: 1,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let value = db.get(b"my_key").unwrap().unwrap();
        assert_eq!(
            value.value_type,
            ValueType::String,
            "Value type is incorrect"
        );
        assert_eq!(value.value, b"my_value", "Value is incorrect");
        assert_eq!(value.ttl, 1, "TTL is incorrect");

        std::thread::sleep(std::time::Duration::from_secs(2));
        let value = db.get(b"my_key").unwrap();
        assert!(value.is_none());
    }

    fn get_test_db() -> Database {
        let db_path = format!("/dev/shm/test_db_{}", rand::random::<i32>());
        let db = Database::open(db_path.as_str()).unwrap();

        let value = &mut StorageValue {
            value_type: ValueType::String,
            ttl: -1,
            value: "value1".as_bytes().to_vec(),
        };
        db.set(b"key1", value).unwrap();

        value.value = "value2".as_bytes().to_vec();
        db.set(b"key2", value).unwrap();

        value.value = "value3".as_bytes().to_vec();
        db.set(b"prefix_key1", value).unwrap();

        value.value = "value4".as_bytes().to_vec();
        db.set(b"prefix_key2", value).unwrap();
        return db;
    }
}
