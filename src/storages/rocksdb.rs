use std::fs;
use std::sync::Arc;

use rocksdb::{OptimisticTransactionDB, Options, Transaction, DB, DEFAULT_COLUMN_FAMILY_NAME};

use crate::errors::DatabaseError;
use crate::storages::storage::Storage;

use super::value::{StorageValue, ValueType};

/// The byte value to search for the end of a prefix
const PREFIX_SEARCH_ENDING: u8 = 0xFF;

/// A struct to represent a Database
/// This struct is used to interact with a `RocksDB` database (currently)
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
/// * `store` - The `RocksDB` instance
pub struct Rocksdb {
    path: String,
    store: Arc<OptimisticTransactionDB>,
}

impl Clone for Rocksdb {
    fn clone(&self) -> Self {
        return Self {
            path: self.path.clone(),
            store: self.store.clone(),
        };
    }
}

impl Drop for Rocksdb {
    fn drop(&mut self) {
        self.close();
    }
}

impl Rocksdb {
    /// Open a new `RocksDB` database at the specified path
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A Result containing the Database instance or a `RocksDB` error
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// ```
    pub fn open(path: &str) -> Result<Self, DatabaseError> {
        Self::prepare_store_location(path)?;

        let mut options = Options::default();
        options.create_if_missing(true);
        let store =
            OptimisticTransactionDB::open_cf(&options, path, vec![DEFAULT_COLUMN_FAMILY_NAME])?;
        return Ok(Self {
            path: path.to_string(),
            store: Arc::new(store),
        });
    }

    /// Prepare the storage location by removing the directory and creating a new one
    ///
    /// # Arguments
    /// * `path` - The path to the storage location
    ///
    /// # Returns
    /// A Result containing `()` or a `DatabaseError`
    ///
    /// # Example
    /// ```
    /// let result = Database::prepare_store_location("/dev/shm/my_storage");
    /// result.unwrap();
    /// ```
    fn prepare_store_location(path: &str) -> Result<(), DatabaseError> {
        fs::remove_dir_all(path).unwrap_or_default();

        match fs::create_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(err) => return Err(DatabaseError::InitialFailed(err.to_string())),
        }
    }
}

impl Storage for Rocksdb {
    /// Close the database and remove the storage directory
    fn close(&self) {
        DB::destroy(&Options::default(), &self.path).unwrap_or_default();
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
    /// let value = db.get(b"my_key").uimpl fmt::Display for StorageValue {
    ///       println!("Value: {}", String::from_utf8(value).unwrap());
    ///   }
    ///  None => {
    ///     println!("Value not found");
    /// }
    /// ```
    fn get(&self, key: &[u8]) -> Result<Option<StorageValue>, DatabaseError> {
        let txn = self.store.transaction();
        let raw_value = txn.get(key);
        match raw_value {
            Ok(value) => match value {
                Some(value) => {
                    let mut storage_value = StorageValue::from_binary(value.as_slice());
                    if storage_value.ttl > -1 {
                        storage_value.ttl -= chrono::Utc::now().timestamp();
                        if self.delete_on_ttl(&txn, &storage_value)? {
                            return Ok(None);
                        }
                    }

                    return Ok(Some(storage_value));
                }
                None => return Ok(None),
            },
            Err(err) => return Err(err.into()),
        }
    }

    /// Get all keys in the database
    ///
    /// # Arguments
    /// * `prefix` - The prefix to filter keys by
    ///
    /// # Returns
    /// A Result containing a vector of keys or a `RocksDB` error
    fn get_all_keys(&self, prefix: &[u8]) -> Result<Vec<String>, DatabaseError> {
        let mut keys = Vec::new();
        let txn = self.store.transaction();
        let iter = txn.prefix_iterator(prefix);
        for result in iter {
            match result {
                Ok((key, raw_value)) => {
                    let mut storage_value = StorageValue::from_binary(&raw_value);
                    if storage_value.ttl > -1 {
                        storage_value.ttl -= chrono::Utc::now().timestamp();
                        if self.delete_on_ttl(&txn, &storage_value)? {
                            continue;
                        }
                    }

                    // FIXME: This is not correct, we need to return error
                    let parsed_key = String::from_utf8(key.to_vec()).unwrap();
                    keys.push(parsed_key);
                } // Push reference to the key
                Err(err) => return Err(err.into()),
            }
        }
        return Ok(keys);
    }

    /// Get the time-to-live (TTL) for a key
    ///
    /// # Arguments
    /// * `key` - The key to get the TTL for
    ///
    /// # Returns
    /// A Result containing the TTL or a `RocksDB` error
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// let ttl = db.get_ttl(b"my_key").unwrap();
    /// ```
    ///
    /// # Errors
    /// If the key is not found, a `DatabaseError::ValueNotFound` error is returned
    /// If there is an error getting the value, a `DatabaseError` is returned
    fn get_ttl(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        let txn = self.store.transaction();
        let raw_value = txn.get(key);
        match raw_value {
            Ok(value) => match value {
                Some(value) => {
                    let storage_value = StorageValue::from_binary(value.as_slice());
                    if storage_value.ttl <= 0 {
                        return Ok(storage_value.ttl);
                    }

                    let ttl = storage_value.ttl - chrono::Utc::now().timestamp();
                    if ttl > 0 {
                        return Ok(ttl);
                    }

                    self.delete(key)?;
                    return Err(DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ));
                }
                None => {
                    return Err(DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ))
                }
            },
            Err(err) => return Err(err.into()),
        }
    }

    /// Update the time-to-live (TTL) for a key
    /// If the TTL is set to a negative value, the key will not expire
    ///
    /// # Arguments
    /// * `key` - The key to update the TTL for
    /// * `ttl` - The new TTL value
    ///
    /// # Returns
    /// A Result containing `()` or a `DatabaseError`
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.update_ttl(b"my_key", 1000);
    /// ```
    fn update_ttl(&self, key: &[u8], ttl: i64) -> Result<(), DatabaseError> {
        let txn = self.store.transaction();
        let raw_value = txn.get(key)?;
        if let Some(value) = raw_value {
            let mut storage_value = StorageValue::from_binary(value.as_slice());
            if ttl < 0 {
                storage_value.ttl = -1;
            } else {
                storage_value.ttl = ttl + chrono::Utc::now().timestamp();
            };
            txn.put(key, storage_value.to_binary())?;
            txn.commit()?;
            Ok(())
        } else {
            Err(DatabaseError::ValueNotFound(
                String::from_utf8_lossy(key).to_string(),
            ))
        }
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
    fn set(&self, key: &[u8], value: &StorageValue) -> Result<(), DatabaseError> {
        let mut value = value.clone();
        if value.ttl < 0 {
            value.ttl = -1;
        } else {
            value.ttl += chrono::Utc::now().timestamp();
        }

        match self.store.put(key, value.to_binary()) {
            Ok(()) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }

    /// Increment the value for a key in the database
    /// If the key does not exist, it will be created with the default value
    ///
    /// # Arguments
    /// * `key` - The key to increment
    /// * `value` - The value to increment by
    /// * `default_value` - The default value to use if the key does not exist
    ///
    /// # Returns
    /// A Result containing the new value or a `DatabaseError`
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.increment(b"my_key", 1, None);
    /// ```
    fn increment(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError> {
        let txn = self.store.transaction();
        let raw_value = txn.get(key);

        if raw_value.is_err() {
            return Err(DatabaseError::InternalError(format!(
                "Failed to get value: {err}",
                err = raw_value.unwrap_err()
            )));
        }

        let mut storage_value: StorageValue;

        match raw_value.unwrap() {
            Some(raw_value) => {
                storage_value = StorageValue::from_binary(raw_value.as_slice());

                let current_value = storage_value.get_integer_value()?;
                let new_value = current_value + value;
                storage_value.value = new_value.to_string().as_bytes().to_vec();
            }
            None => match default_value {
                Some(default_value) => {
                    storage_value = StorageValue {
                        value_type: ValueType::Integer,
                        ttl: -1,
                        value: (default_value + value).to_string().as_bytes().to_vec(),
                    };
                }
                None => {
                    return Err(DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ));
                }
            },
        }

        txn.put(key, storage_value.to_binary())?;
        txn.commit()?;
        return Ok(storage_value);
    }

    /// Decrement the value for a key in the database
    /// If the key does not exist, it will be created with the default value
    ///
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `value` - The value to decrement by
    /// * `default_value` - The default value to use if the key does not exist
    ///
    /// # Returns
    /// A Result containing the new value or a `DatabaseError`
    ///
    /// # Example
    /// ```
    /// let db = Database::open("/dev/shm/my_storage").unwrap();
    /// db.decrement(b"my_key", 1, None);
    /// ```
    fn decrement(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError> {
        let txn = self.store.transaction();
        let raw_value = txn.get(key);

        if raw_value.is_err() {
            return Err(DatabaseError::InternalError(format!(
                "Failed to get value: {err}",
                err = raw_value.unwrap_err()
            )));
        }

        let mut storage_value: StorageValue;

        match raw_value.unwrap() {
            Some(raw_value) => {
                storage_value = StorageValue::from_binary(raw_value.as_slice());

                let current_value = storage_value.get_integer_value()?;
                let new_value = current_value - value;
                storage_value.value = new_value.to_string().as_bytes().to_vec();
            }
            None => match default_value {
                Some(default_value) => {
                    storage_value = StorageValue {
                        value_type: ValueType::Integer,
                        ttl: -1,
                        value: (default_value - value).to_string().as_bytes().to_vec(),
                    };
                }
                None => {
                    return Err(DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ));
                }
            },
        }

        txn.put(key, storage_value.to_binary())?;
        txn.commit()?;
        return Ok(storage_value);
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
    fn delete(&self, key: &[u8]) -> Result<(), DatabaseError> {
        match self.store.delete(key) {
            Ok(()) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
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
    fn delete_prefix(&self, prefix: &[u8]) -> Result<(), DatabaseError> {
        let mut end_prefix = prefix.to_vec();
        end_prefix.push(PREFIX_SEARCH_ENDING);
        let cf = self.store.cf_handle(DEFAULT_COLUMN_FAMILY_NAME);
        let cf = cf.unwrap();

        let del_result = self
            .store
            .delete_range_cf(&cf, prefix, end_prefix.as_slice());

        match del_result {
            Ok(()) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }

    /// Delete a key-value pair from the database if the TTL has expired
    /// # Arguments
    /// * `txn` - The transaction to use
    /// * `key` - The key to delete
    /// # Returns
    /// A Result containing a boolean indicating if the key was deleted or a `RocksDB` error
    fn delete_on_ttl(
        &self,
        txn: &Transaction<OptimisticTransactionDB>,
        key: &StorageValue,
    ) -> Result<bool, DatabaseError> {
        if key.ttl <= 0 {
            txn.delete(key.value.as_slice())?;
            return Ok(true);
        }
        return Ok(false);
    }
}

#[cfg(test)]
mod tests {
    use crate::storages::value::{StorageValue, ValueType};

    use super::*;

    #[test]
    fn test_get_all_keys() {
        let db = get_test_db();

        let keys = db.get_all_keys(b"prefix_").unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&String::from("prefix_key1")));
        assert!(keys.contains(&String::from("prefix_key2")));
    }

    #[test]
    fn test_get_ttl() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: 1000,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let ttl = db.get_ttl(b"my_key").unwrap();
        assert_eq!(ttl, 1000, "TTL is incorrect");

        let ttl = db.get_ttl(b"non_existent_key");
        assert!(ttl.is_err(), "Expected error for non-existent key");
    }

    #[test]
    fn test_get_ttl_no_ttl() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: -1,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let ttl = db.get_ttl(b"my_key").unwrap();
        assert_eq!(ttl, -1, "TTL is incorrect");
    }

    #[test]
    fn test_get_ttl_expired() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: 1,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));
        let ttl = db.get_ttl(b"my_key");
        assert!(ttl.is_err(), "Expected error for expired key");
    }

    #[test]
    fn test_update_ttl() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: 1000,
            value: b"my_value".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let ttl = db.get_ttl(b"my_key").unwrap();
        assert_eq!(ttl, 1000, "TTL is incorrect");

        db.update_ttl(b"my_key", 2000).unwrap();
        let ttl = db.get_ttl(b"my_key").unwrap();
        assert_eq!(ttl, 2000, "TTL is incorrect");

        db.update_ttl(b"my_key", -1).unwrap();
        let ttl = db.get_ttl(b"my_key").unwrap();
        assert_eq!(ttl, -1, "TTL is incorrect");
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
        assert_eq!(keys.len(), 3);
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

    #[test]
    fn test_integer_value() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::Integer,
            ttl: -1,
            value: b"123".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let value = db.get(b"my_key").unwrap().unwrap();
        assert_eq!(
            value.value_type,
            ValueType::Integer,
            "Value type is incorrect"
        );
        assert_eq!(value.value, b"123", "Value is incorrect");
        assert_eq!(value.ttl, -1, "TTL is incorrect");
    }

    #[test]
    fn test_get_integer_value() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::Integer,
            ttl: -1,
            value: b"123".to_vec(),
        };
        db.set(b"my_key", value).unwrap();

        let value = db.get(b"my_key").unwrap().unwrap();
        let integer_value = value.get_integer_value().unwrap();
        assert_eq!(integer_value, 123);
    }

    #[test]
    fn test_increment() {
        let db = get_test_db();

        let value = db.increment(b"value_num", 1, None).unwrap();
        assert_eq!(value.value, b"2", "Value is incorrect");

        let value = db.increment(b"value_num", 2, None).unwrap();
        assert_eq!(value.value, b"4", "Value is incorrect");
    }

    #[test]
    fn test_default_increment() {
        let db = get_test_db();

        let value = db.increment(b"value_num", 1, Some(10)).unwrap();
        assert_eq!(value.value, b"2", "Value is incorrect");

        let value = db.increment(b"value_num", 2, Some(10)).unwrap();
        assert_eq!(value.value, b"4", "Value is incorrect");
    }

    #[test]
    fn test_default_exist_increment() {
        let db = get_test_db();

        let value = db.increment(b"value_num", 1, Some(10)).unwrap();
        assert_eq!(value.value, b"2", "Value is incorrect");

        let value = db.increment(b"value_num", 2, Some(10)).unwrap();
        assert_eq!(value.value, b"4", "Value is incorrect");
    }

    #[test]
    fn test_decrement() {
        let db = get_test_db();

        let value = db.decrement(b"value_num", 1, None).unwrap();
        assert_eq!(value.value, b"0", "Value is incorrect");

        let value = db.decrement(b"value_num", 2, None).unwrap();
        assert_eq!(value.value, b"-2", "Value is incorrect");
    }

    #[test]
    fn test_default_decrement() {
        let db = get_test_db();

        let value = db.decrement(b"new_value_num", 1, Some(10)).unwrap();
        assert_eq!(value.value, b"9", "Value is incorrect");

        let value = db.decrement(b"new_value_num", 2, Some(10)).unwrap();
        assert_eq!(value.value, b"7", "Value is incorrect");
    }

    #[test]
    fn test_default_exist_decrement() {
        let db = get_test_db();

        let value = db.decrement(b"value_num", 1, Some(10)).unwrap();
        assert_eq!(value.value, b"0", "Value is incorrect");

        let value = db.decrement(b"value_num", 2, Some(10)).unwrap();
        assert_eq!(value.value, b"-2", "Value is incorrect");
    }

    #[test]
    fn test_string_value() {
        let db = get_test_db();

        let value = &StorageValue {
            value_type: ValueType::String,
            ttl: -1,
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
        assert_eq!(value.ttl, -1, "TTL is incorrect");
    }

    fn get_test_db() -> Rocksdb {
        let db_path = format!("/dev/shm/test_db_{}", rand::random::<i32>());
        let db = Rocksdb::open(db_path.as_str()).unwrap();

        let value = &mut StorageValue {
            value_type: ValueType::String,
            ttl: -1,
            value: b"value1".to_vec(),
        };
        db.set(b"key1", value).unwrap();

        value.value = b"value2".to_vec();
        db.set(b"key2", value).unwrap();

        value.value = b"value3".to_vec();
        db.set(b"prefix_key1", value).unwrap();

        value.value = b"value4".to_vec();
        db.set(b"prefix_key2", value).unwrap();

        let value = &StorageValue {
            value_type: ValueType::Integer,
            ttl: -1,
            value: b"1".to_vec(),
        };
        db.set(b"value_num", value).unwrap();

        return db;
    }
}
