use std::sync::Arc;

use actix_web::{test, App};

use super::service::DatabaseQueries;
use crate::server::models;
use crate::storages::rocksdb::Rocksdb;
use crate::storages::storage::Storage;
use crate::storages::value::{StorageValue, ValueType};

#[actix_web::test]
async fn test_get_value() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::default().uri("/keys/key1").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );
}

#[actix_web::test]
async fn test_get_all_keys() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::default()
        .uri("/keys?prefix=prefix_")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetAllKeysResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetAllKeysResponse { keys }) => {
            assert_eq!(keys.len(), 3);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_set_key() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );
}

#[actix_web::test]
async fn test_delete_key() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));

    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::delete().uri("/keys/key1").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db.get(b"key1").unwrap().is_none());
}

#[actix_web::test]
async fn test_delete_keys() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::delete()
        .uri("/keys")
        .set_json(models::DeleteKeysRequest {
            prefix: "prefix_".to_string(),
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db.get(b"prefix_key1").unwrap().is_none());
    assert!(db.get(b"prefix_key2").unwrap().is_none());
    assert!(db.get(b"key1").unwrap().is_some());
}

#[actix_web::test]
async fn test_ttl() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: 2,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db.get(b"key3").unwrap().is_some());

    std::thread::sleep(std::time::Duration::from_secs(2));

    assert!(db.get(b"key3").unwrap().is_none());
}

#[actix_web::test]
async fn test_integer_value() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::Int(123),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key3").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetResponse { value }) => {
            let value = value.unwrap();
            match value {
                models::IntOrString::Int(i) => assert_eq!(i, 123),
                models::IntOrString::String(_) => panic!("Unexpected value: {value:?}"),
            }
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_string_value() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key3").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetResponse { value }) => {
            let value = value.unwrap();
            match value {
                models::IntOrString::String(s) => assert_eq!(s, "value3"),
                models::IntOrString::Int(_) => panic!("Unexpected value: {value:?}"),
            }
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_increment() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: None,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 2);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_default_increment() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 2);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_default_exist_increment() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/new_value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 11);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_decrement() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: None,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 0);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_default_decrement() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/new_value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 9);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_default_exist_decrement() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::IncrementResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::IncrementResponse { value }) => {
            assert_eq!(value, 0);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_get_ttl() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::get().uri("/keys/key1/ttl").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetTtlResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetTtlResponse { ttl }) => {
            assert_eq!(ttl, -1);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_get_ttl_nonexistent_key() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::get()
        .uri("/keys/nonexistent_key/ttl")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetTtlResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetTtlResponse { ttl }) => {
            assert_eq!(ttl, -1);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_set_key_with_ttl() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key_with_ttl".to_string(),
            value: models::IntOrString::String("value_with_ttl".to_string()),
            ttl: 5,
        })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get()
        .uri("/keys/key_with_ttl/ttl")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetTtlResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetTtlResponse { ttl }) => {
            assert!((0..=5).contains(&ttl));
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[actix_web::test]
async fn test_set_ttl() {
    let db = get_test_db();
    let query_service = DatabaseQueries::new(Arc::new(db.clone()));
    let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
    let req = test::TestRequest::post()
        .uri("/keys/key1/ttl")
        .set_json(models::SetTtlRequest { ttl: 5 })
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key1/ttl").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetTtlResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetTtlResponse { ttl }) => {
            assert!((0..=5).contains(&ttl));
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

fn get_test_db() -> impl Storage + Clone {
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

    let value = &mut StorageValue {
        value_type: ValueType::Integer,
        ttl: -1,
        value: b"1".to_vec(),
    };
    db.set(b"value_num", value).unwrap();

    return db;
}
