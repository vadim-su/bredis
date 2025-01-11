use async_trait::async_trait;
use surrealkv::{Options, Store};

use crate::errors;

use super::{storage::Storage, value::StorageValue};

const PREFIX_SEARCH_ENDING: u8 = 0xFF;

pub struct SurrealKV {
    store: Store,
}

impl SurrealKV {
    pub fn open() -> Self {
        let options = Options {
            disk_persistence: false,
            ..Default::default()
        };

        let store = Store::new(options).expect("Failed to create store");
        Self { store }
    }
}

#[async_trait]
impl Storage for SurrealKV {
    async fn close(&self) {
        self.store.close().await.unwrap();
    }

    async fn get(&self, key: &[u8]) -> Result<Option<StorageValue>, errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let raw_value = txn.get(key);
        let mut value = match raw_value {
            Ok(Some(value)) => super::value::StorageValue::from_binary(&value),
            Ok(None) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        // TTL doesn't set, return the value
        if value.ttl < 0 {
            return Ok(Some(value));
        }

        // TTL is set, check if the value is expired
        value.ttl -= chrono::Utc::now().timestamp();
        if value.ttl <= 0 {
            txn.delete(key).unwrap();
            return Ok(None);
        }

        txn.commit().await.unwrap();
        return Ok(Some(value));
    }

    async fn get_all_keys(&self, prefix: &[u8]) -> Result<Vec<String>, errors::DatabaseError> {
        let mut end_prefix = prefix.to_vec();
        end_prefix.push(PREFIX_SEARCH_ENDING);
        let keys_range = prefix..end_prefix.as_slice();

        let mut txn = self.store.begin().unwrap();
        let key_val_res = txn.scan(keys_range, None)?;

        let mut keys: Vec<String> = vec![];
        for (key, raw_value, _) in key_val_res {
            let value = super::value::StorageValue::from_binary(&raw_value);

            if value.ttl > -1 {
                let ttl = value.ttl - chrono::Utc::now().timestamp();
                if ttl <= 0 {
                    txn.delete(&key).unwrap();
                    continue;
                }
            }
            keys.push(String::from_utf8_lossy(&key).to_string());
        }

        txn.commit().await.unwrap();
        return Ok(keys);
    }

    async fn get_ttl(&self, key: &[u8]) -> Result<i64, errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let raw_value = txn.get(key)?;
        let value = match raw_value {
            Some(value) => super::value::StorageValue::from_binary(&value),
            None => {
                return Err(errors::DatabaseError::ValueNotFound(
                    String::from_utf8_lossy(key).to_string(),
                ))
            }
        };

        if value.ttl < 0 {
            return Ok(-1);
        }

        let ttl = value.ttl - chrono::Utc::now().timestamp();
        if ttl <= 0 {
            txn.delete(key)?;
            return Err(errors::DatabaseError::ValueNotFound(
                String::from_utf8_lossy(key).to_string(),
            ));
        }

        txn.commit().await.unwrap();
        return Ok(ttl);
    }

    async fn update_ttl(&self, key: &[u8], ttl: i64) -> Result<(), errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let raw_value = txn.get(key)?;
        let mut value = match raw_value {
            Some(value) => super::value::StorageValue::from_binary(&value),
            None => {
                return Err(errors::DatabaseError::ValueNotFound(
                    String::from_utf8_lossy(key).to_string(),
                ))
            }
        };

        if ttl < 0 {
            value.ttl = -1;
        } else {
            value.ttl = ttl + chrono::Utc::now().timestamp();
        }

        txn.set(key, &value.to_binary())?;

        txn.commit().await.unwrap();
        return Ok(());
    }

    async fn set(&self, key: &[u8], value: &StorageValue) -> Result<(), errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let mut value = value.clone();

        if value.ttl >= 0 {
            value.ttl += chrono::Utc::now().timestamp();
        } else {
            value.ttl = -1;
        }

        txn.set(key, &value.to_binary())?;
        txn.commit().await.unwrap();

        return Ok(());
    }

    async fn increment(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let raw_value = txn.get(key)?;

        let storage_value = match raw_value {
            Some(raw_value) => {
                let mut storage_value = StorageValue::from_binary(&raw_value);
                let current_value = storage_value.get_integer_value()?;
                let new_value = current_value + value;
                storage_value.value = new_value.to_string().as_bytes().to_vec();
                storage_value
            }
            None => match default_value {
                Some(default_value) => StorageValue {
                    value_type: super::value::ValueType::Integer,
                    ttl: -1,
                    value: (default_value + value).to_string().as_bytes().to_vec(),
                },
                None => {
                    return Err(errors::DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ));
                }
            },
        };

        txn.set(key, &storage_value.to_binary())?;

        txn.commit().await.unwrap();
        Ok(storage_value)
    }

    async fn decrement(
        &self,
        key: &[u8],
        value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        let raw_value = txn.get(key)?;

        let storage_value = match raw_value {
            Some(raw_value) => {
                let mut storage_value = StorageValue::from_binary(&raw_value);
                let current_value = storage_value.get_integer_value()?;
                let new_value = current_value - value;
                storage_value.value = new_value.to_string().as_bytes().to_vec();
                storage_value
            }
            None => match default_value {
                Some(default_value) => StorageValue {
                    value_type: super::value::ValueType::Integer,
                    ttl: -1,
                    value: (default_value - value).to_string().as_bytes().to_vec(),
                },
                None => {
                    return Err(errors::DatabaseError::ValueNotFound(
                        String::from_utf8_lossy(key).to_string(),
                    ));
                }
            },
        };

        txn.set(key, &storage_value.to_binary())?;

        txn.commit().await.unwrap();
        Ok(storage_value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), errors::DatabaseError> {
        let mut txn = self.store.begin().unwrap();
        txn.delete(key)?;

        txn.commit().await.unwrap();
        return Ok(());
    }

    async fn delete_prefix(&self, prefix: &[u8]) -> Result<(), errors::DatabaseError> {
        let mut end_prefix = prefix.to_vec();
        end_prefix.push(PREFIX_SEARCH_ENDING);
        let keys_range = prefix..end_prefix.as_slice();

        let mut txn = self.store.begin().unwrap();
        let key_val_res = txn.scan(keys_range, None)?;

        for (key, _, _) in key_val_res {
            txn.delete(&key)?;
        }

        txn.commit().await.unwrap();
        return Ok(());
    }
}

impl From<surrealkv::Error> for errors::DatabaseError {
    fn from(err: surrealkv::Error) -> Self {
        Self::InternalError(err.to_string())
    }
}
