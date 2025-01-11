use async_trait::async_trait;

use crate::errors::DatabaseError;

use super::value::StorageValue;

#[async_trait]
pub trait Storage: Sync + Send {
    /// Close the database and remove the storage directory
    async fn close(&self);

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
    async fn get(&self, key: &[u8]) -> Result<Option<StorageValue>, DatabaseError>;

    /// Get all keys in the database
    ///
    /// # Arguments
    /// * `prefix` - The prefix to filter keys by
    ///
    /// # Returns
    /// A Result containing a vector of keys or a `RocksDB` error
    async fn get_all_keys(&self, prefix: &[u8]) -> Result<Vec<String>, DatabaseError>;

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
    async fn get_ttl(&self, key: &[u8]) -> Result<i64, DatabaseError>;

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
    async fn update_ttl(&self, key: &[u8], ttl: i64) -> Result<(), DatabaseError>;

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
    async fn set(&self, key: &[u8], value: &StorageValue) -> Result<(), DatabaseError>;

    async fn increment(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError>;

    async fn decrement(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError>;

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
    async fn delete(&self, key: &[u8]) -> Result<(), DatabaseError>;

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
    async fn delete_prefix(&self, prefix: &[u8]) -> Result<(), DatabaseError>;
}
