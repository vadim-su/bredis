use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;

use crate::errors::DatabaseError;

use super::{
    storage::Storage,
    value::{StorageValue, ValueType},
};

#[derive(Clone)]
pub struct Bredis {
    store: Arc<RwLock<HashMap<String, StorageValue>>>,
}

impl Bredis {
    #[allow(dead_code)]
    pub fn open() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Storage for Bredis {
    async fn get(&self, key: &[u8]) -> Result<Option<StorageValue>, DatabaseError> {
        let key_str = String::from_utf8(key.to_vec()).unwrap();
        let mut store = self.store.write().unwrap();
        if let Some(value) = store.get_mut(&key_str) {
            if value.ttl < 0 {
                return Ok(Some(value.clone()));
            }

            value.ttl -= chrono::Utc::now().timestamp();
            if value.ttl < 0 {
                // Value is expired, remove it
                store.remove(&key_str);
                drop(store);
                return Ok(None);
            }
            return Ok(Some(value.clone()));
        }
        Ok(None)
    }

    async fn set(&self, key: &[u8], value: &StorageValue) -> Result<(), DatabaseError> {
        let mut value = value.clone();
        if value.ttl < 0 {
            value.ttl = -1;
        } else {
            value.ttl += chrono::Utc::now().timestamp();
        }
        self.store
            .write()
            .unwrap()
            .insert(String::from_utf8(key.to_vec()).unwrap(), value);
        Ok(())
    }

    async fn get_all_keys(&self, prefix: &[u8]) -> Result<Vec<String>, DatabaseError> {
        let keys: Vec<String> = self
            .store
            .read()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(&String::from_utf8(prefix.to_vec()).unwrap()))
            .cloned()
            .collect();
        Ok(keys)
    }

    async fn get_ttl(&self, key: &[u8]) -> Result<i64, DatabaseError> {
        let mut store = self.store.write().unwrap();
        match store.get(&String::from_utf8(key.to_vec()).unwrap()) {
            Some(value) => {
                if value.ttl < 0 {
                    return Ok(-1);
                }

                let ttl = value.ttl - chrono::Utc::now().timestamp();
                if ttl > 0 {
                    return Ok(ttl);
                }

                store.remove(&String::from_utf8(key.to_vec()).unwrap());

                return Err(DatabaseError::ValueNotFound(
                    String::from_utf8(key.to_vec()).unwrap(),
                ));
            }
            None => Err(DatabaseError::ValueNotFound(
                String::from_utf8(key.to_vec()).unwrap(),
            )),
        }
    }

    async fn update_ttl(&self, key: &[u8], ttl: i64) -> Result<(), DatabaseError> {
        let mut store = self.store.write().unwrap();
        match store.get_mut(&String::from_utf8(key.to_vec()).unwrap()) {
            Some(value) => {
                if ttl < 0 {
                    value.ttl = -1;
                } else {
                    value.ttl = chrono::Utc::now().timestamp() + ttl;
                }
                Ok(())
            }
            None => Err(DatabaseError::ValueNotFound(
                String::from_utf8(key.to_vec()).unwrap(),
            )),
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn increment(
        &self,
        key: &[u8],
        increment_value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError> {
        let mut store = self.store.write().unwrap();
        let key = String::from_utf8(key.to_vec()).unwrap();
        let value = store.entry(key).or_insert_with(|| StorageValue {
            value_type: ValueType::Integer,
            ttl: -1,
            value: default_value.unwrap_or(0).to_string().into_bytes(),
        });
        if value.value_type != ValueType::Integer {
            return Err(DatabaseError::InvalidValueType(
                "Value is not an integer".to_string(),
            ));
        }
        let string_value = String::from_utf8(value.value.clone());
        if string_value.is_err() {
            return Err(DatabaseError::InternalError(
                "Failed to parse integer value".to_string(),
            ));
        }
        let current_value = string_value.unwrap().parse::<i64>().unwrap();
        let new_value = current_value + increment_value;
        value.value = new_value.to_string().into_bytes();
        Ok(value.clone())
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn decrement(
        &self,
        key: &[u8],
        decrement_value: i64,
        default_value: Option<i64>,
    ) -> Result<StorageValue, DatabaseError> {
        let mut store = self.store.write().unwrap();
        let key = String::from_utf8(key.to_vec()).unwrap();
        let value = store.entry(key).or_insert_with(|| StorageValue {
            value_type: ValueType::Integer,
            ttl: -1,
            value: default_value.unwrap_or(0).to_string().into_bytes(),
        });
        if value.value_type != ValueType::Integer {
            return Err(DatabaseError::InvalidValueType(
                "Value is not an integer".to_string(),
            ));
        }
        let string_value = String::from_utf8(value.value.clone());
        if string_value.is_err() {
            return Err(DatabaseError::InternalError(
                "Failed to parse integer value".to_string(),
            ));
        }
        let current_value = string_value.unwrap().parse::<i64>().unwrap();
        let new_value = current_value - decrement_value;
        value.value = new_value.to_string().into_bytes();
        Ok(value.clone())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), DatabaseError> {
        self.store
            .write()
            .unwrap()
            .remove(&String::from_utf8(key.to_vec()).unwrap());
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &[u8]) -> Result<(), DatabaseError> {
        let mut store = self.store.write().unwrap();

        // Remove all keys that start with the prefix
        store.retain(|key, _| !key.starts_with(&String::from_utf8(prefix.to_vec()).unwrap()));

        drop(store);
        Ok(())
    }

    async fn close(&self) {}
}
