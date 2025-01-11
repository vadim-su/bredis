use crate::storages::value::{StorageValue, ValueType};
use rstest::*;
use rstest_reuse::{self, *};

use super::{bredis::Bredis, rocksdb::Rocksdb, storage::Storage, surrealkv::SurrealKV};

#[template]
#[rstest]
#[case::rocksdb(async { rocksdb().await })]
#[case::bredis(async { bredis().await })]
#[case::surrealkv(async { surrealkv().await })]
#[tokio::test]
async fn test_cases(
    #[future]
    #[case]
    _db: Box<impl Storage>,
) {
}

#[apply(test_cases)]
async fn test_get_all_keys(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance
    let keys = db.get_all_keys(b"prefix_").await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&String::from("prefix_key1")));
    assert!(keys.contains(&String::from("prefix_key2")));
}

#[apply(test_cases)]
async fn test_get_ttl(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance
    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: 1000,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let ttl = db.get_ttl(b"my_key").await.unwrap();
    assert_eq!(ttl, 1000, "TTL is incorrect");

    let ttl = db.get_ttl(b"non_existent_key").await;
    assert!(ttl.is_err(), "Expected error for non-existent key");
}

#[apply(test_cases)]
async fn test_get_ttl_no_ttl(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance
    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let ttl = db.get_ttl(b"my_key").await.unwrap();
    assert_eq!(ttl, -1, "TTL is incorrect");
}

#[apply(test_cases)]
async fn test_get_ttl_expired(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: 1,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2));
    let ttl = db.get_ttl(b"my_key").await;
    assert!(ttl.is_err(), "Expected error for expired key");
}

#[apply(test_cases)]
async fn test_update_ttl(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: 1000,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let ttl = db.get_ttl(b"my_key").await.unwrap();
    assert_eq!(ttl, 1000, "TTL is incorrect");

    db.update_ttl(b"my_key", 2000).await.unwrap();
    let ttl = db.get_ttl(b"my_key").await.unwrap();
    assert_eq!(ttl, 2000, "TTL is incorrect");

    db.update_ttl(b"my_key", -1).await.unwrap();
    let ttl = db.get_ttl(b"my_key").await.unwrap();
    assert_eq!(ttl, -1, "TTL is incorrect");
}

#[apply(test_cases)]
async fn test_set(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let storage_value = db.get(b"my_key").await.unwrap().unwrap();
    assert_eq!(
        storage_value.value_type,
        ValueType::String,
        "Value type is incorrect"
    );
    assert_eq!(storage_value.value, b"my_value", "Value is incorrect");
    assert_eq!(storage_value.ttl, -1, "TTL is incorrect");
}

#[apply(test_cases)]
async fn test_delete(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();
    db.delete(b"my_key").await.unwrap();

    let value = db.get(b"my_key").await.unwrap();
    assert!(value.is_none());
}

#[apply(test_cases)]
async fn test_delete_prefix(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    db.delete_prefix(b"prefix_").await.unwrap();

    let keys = db.get_all_keys(b"").await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&String::from("key1")));
    assert!(keys.contains(&String::from("key2")));
}

#[apply(test_cases)]
async fn test_ttl(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let ttl = 1;
    let value = &StorageValue {
        value_type: ValueType::String,
        ttl,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let value = db.get(b"my_key").await.unwrap().unwrap();
    assert_eq!(
        value.value_type,
        ValueType::String,
        "Value type is incorrect"
    );
    assert_eq!(value.value, b"my_value", "Value is incorrect");
    assert_eq!(value.ttl, ttl, "TTL is incorrect");

    std::thread::sleep(std::time::Duration::from_secs(2));
    let value = db.get(b"my_key").await.unwrap();
    assert!(value.is_none());
}

#[apply(test_cases)]
async fn test_integer_value(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"123".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let value = db.get(b"my_key").await.unwrap().unwrap();
    assert_eq!(
        value.value_type,
        ValueType::Integer,
        "Value type is incorrect"
    );
    assert_eq!(value.value, b"123", "Value is incorrect");
    assert_eq!(value.ttl, -1, "TTL is incorrect");
}

#[apply(test_cases)]
async fn test_get_integer_value(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"123".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let value = db.get(b"my_key").await.unwrap().unwrap();
    let integer_value = value.get_integer_value().unwrap();
    assert_eq!(integer_value, 123);
}

#[apply(test_cases)]
async fn test_increment(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.increment(b"value_num", 1, None).await.unwrap();
    assert_eq!(value.value, b"2", "Value is incorrect");

    let value = db.increment(b"value_num", 2, None).await.unwrap();
    assert_eq!(value.value, b"4", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_default_increment(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.increment(b"value_num", 1, Some(10)).await.unwrap();
    assert_eq!(value.value, b"2", "Value is incorrect");

    let value = db.increment(b"value_num", 2, Some(10)).await.unwrap();
    assert_eq!(value.value, b"4", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_default_exist_increment(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.increment(b"value_num", 1, Some(10)).await.unwrap();
    assert_eq!(value.value, b"2", "Value is incorrect");

    let value = db.increment(b"value_num", 2, Some(10)).await.unwrap();
    assert_eq!(value.value, b"4", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_decrement(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.decrement(b"value_num", 1, None).await.unwrap();
    assert_eq!(value.value, b"0", "Value is incorrect");

    let value = db.decrement(b"value_num", 2, None).await.unwrap();
    assert_eq!(value.value, b"-2", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_default_decrement(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.decrement(b"new_value_num", 1, Some(10)).await.unwrap();
    assert_eq!(value.value, b"9", "Value is incorrect");

    let value = db.decrement(b"new_value_num", 2, Some(10)).await.unwrap();
    assert_eq!(value.value, b"7", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_default_exist_decrement(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = db.decrement(b"value_num", 1, Some(10)).await.unwrap();
    assert_eq!(value.value, b"0", "Value is incorrect");

    let value = db.decrement(b"value_num", 2, Some(10)).await.unwrap();
    assert_eq!(value.value, b"-2", "Value is incorrect");
}

#[apply(test_cases)]
async fn test_string_value(
    #[future]
    #[case]
    db: Box<impl Storage>,
) {
    let db = db.await; // Await the future to get the actual storage instance

    let value = &StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"my_value".to_vec(),
    };
    db.set(b"my_key", value).await.unwrap();

    let value = db.get(b"my_key").await.unwrap().unwrap();
    assert_eq!(
        value.value_type,
        ValueType::String,
        "Value type is incorrect"
    );
    assert_eq!(value.value, b"my_value", "Value is incorrect");
    assert_eq!(value.ttl, -1, "TTL is incorrect");
}

#[fixture]
async fn rocksdb() -> Box<impl Storage> {
    let db_path = format!("/dev/shm/test_db_{}", rand::random::<i32>());
    let db = Rocksdb::open(db_path.as_str()).unwrap();

    let value = &mut StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"value1".to_vec(),
    };
    db.set(b"key1", value).await.unwrap();

    value.value = b"value2".to_vec();
    db.set(b"key2", value).await.unwrap();

    value.value = b"value3".to_vec();
    db.set(b"prefix_key1", value).await.unwrap();

    value.value = b"value4".to_vec();
    db.set(b"prefix_key2", value).await.unwrap();

    let value = &StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"1".to_vec(),
    };
    db.set(b"value_num", value).await.unwrap();

    return Box::new(db);
}

#[fixture]
async fn bredis() -> Box<impl Storage> {
    let db = Bredis::open();
    let value = &mut StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"value1".to_vec(),
    };
    db.set(b"key1", value).await.unwrap();

    value.value = b"value2".to_vec();
    db.set(b"key2", value).await.unwrap();

    value.value = b"value3".to_vec();
    db.set(b"prefix_key1", value).await.unwrap();

    value.value = b"value4".to_vec();
    db.set(b"prefix_key2", value).await.unwrap();

    let value = &StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"1".to_vec(),
    };
    db.set(b"value_num", value).await.unwrap();

    return Box::new(db);
}

#[fixture]
async fn surrealkv() -> Box<impl Storage> {
    let db = SurrealKV::open();
    let value = &mut StorageValue {
        value_type: ValueType::String,
        ttl: -1,
        value: b"value1".to_vec(),
    };
    db.set(b"key1", value).await.unwrap();

    value.value = b"value2".to_vec();
    db.set(b"key2", value).await.unwrap();

    value.value = b"value3".to_vec();
    db.set(b"prefix_key1", value).await.unwrap();

    value.value = b"value4".to_vec();
    db.set(b"prefix_key2", value).await.unwrap();

    let value = &StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"1".to_vec(),
    };
    db.set(b"value_num", value).await.unwrap();

    return Box::new(db);
}
