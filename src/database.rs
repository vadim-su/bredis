use std::fs;
use std::sync::Arc;

use rocksdb::{Options, DB, DEFAULT_COLUMN_FAMILY_NAME};

const PREFIX_ENDING: u8 = 0xFF;

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
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        return self.store.get(key);
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
                Ok((key, _)) => {
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
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), rocksdb::Error> {
        return self.store.put(key, value);
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
        end_prefix.push(PREFIX_ENDING);
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
        db.set(b"my_key", b"my_value").unwrap();

        let value = db.store.get(b"my_key").unwrap();
        assert_eq!(value.unwrap(), b"my_value");
    }

    #[test]
    fn test_delete() {
        let db = get_test_db();
        db.set(b"my_key", b"my_value").unwrap();
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

    fn get_test_db() -> Database {
        let db_path = format!("/dev/shm/test_db_{}", rand::random::<i32>());
        let db = Database::open(db_path.as_str()).unwrap();
        db.set(b"key1", b"value1").unwrap();
        db.set(b"key2", b"value2").unwrap();
        db.set(b"prefix_key1", b"value3").unwrap();
        db.set(b"prefix_key2", b"value4").unwrap();
        return db;
    }
}
