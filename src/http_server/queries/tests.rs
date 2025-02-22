use std::sync::Arc;

use actix_web::{test, App};
use apistos::app::OpenApiWrapper;
use apistos::spec::Spec;
use rstest::*;
use rstest_reuse::{apply, template};

use crate::http_server::models;
use crate::storages::bredis::Bredis;
use crate::storages::rocksdb::Rocksdb;
use crate::storages::storage::Storage;
use crate::storages::surrealkv::SurrealKV;
use crate::storages::value::{StorageValue, ValueType};

#[template]
#[rstest]
#[case::rocksdb(async { rocksdb().await })]
#[case::bredis(async { bredis().await })]
#[case::surrealkv(async { surrealkv().await })]
#[actix_web::test]
async fn test_cases(
    #[future]
    #[case]
    _db: Box<dyn Storage>,
) {
}

#[apply(test_cases)]
async fn test_get_value(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::default().uri("/keys/key1").to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );
}

#[apply(test_cases)]
async fn test_get_all_keys(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::default()
        .uri("/keys?prefix=prefix_")
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let body: models::ApiResponse<models::GetAllKeysResponse> = test::read_body_json(resp).await;

    match body {
        models::ApiResponse::Success(models::GetAllKeysResponse { keys }) => {
            assert_eq!(keys.len(), 2);
        }
        models::ApiResponse::ErrorResponse(_) => panic!("Unexpected response: {body:?}"),
    }
}

#[apply(test_cases)]
async fn test_set_key(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );
}

#[apply(test_cases)]
async fn test_delete_key(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db_arc = Arc::new(db.await);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::delete().uri("/keys/key1").to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db_arc.get(b"key1").await.unwrap().is_none());
}

#[apply(test_cases)]
async fn test_delete_keys(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db_arc = Arc::new(db.await);

    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::delete()
        .uri("/keys")
        .set_json(models::DeleteKeysRequest {
            prefix: "prefix_".to_string(),
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db_arc.get(b"prefix_key1").await.unwrap().is_none());
    assert!(db_arc.get(b"prefix_key2").await.unwrap().is_none());
    assert!(db_arc.get(b"key1").await.unwrap().is_some());
}

#[apply(test_cases)]
async fn test_ttl(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db_arc = Arc::new(db.await);

    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: 2,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    assert!(db_arc.get(b"key3").await.unwrap().is_some());

    std::thread::sleep(std::time::Duration::from_secs(2));

    assert!(db_arc.get(b"key3").await.unwrap().is_none());
}

#[apply(test_cases)]
async fn test_integer_value(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::Int(123),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key3").to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_string_value(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key3".to_string(),
            value: models::IntOrString::String("value3".to_string()),
            ttl: -1,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key3").to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_increment(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: None,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_default_increment(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_default_exist_increment(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/new_value_num/inc")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_decrement(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: None,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_default_decrement(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/new_value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_default_exist_decrement(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/value_num/dec")
        .set_json(models::IncrementRequest {
            value: 1,
            default: Some(10),
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_get_ttl(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::get().uri("/keys/key1/ttl").to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_get_ttl_nonexistent_key(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::get()
        .uri("/keys/nonexistent_key/ttl")
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_set_key_with_ttl(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_json(models::SetRequest {
            key: "key_with_ttl".to_string(),
            value: models::IntOrString::String("value_with_ttl".to_string()),
            ttl: 5,
        })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get()
        .uri("/keys/key_with_ttl/ttl")
        .to_request();
    let resp = test::call_service(&service, req).await;
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

#[apply(test_cases)]
async fn test_set_ttl(
    #[future]
    #[case]
    db: Box<dyn Storage>,
) {
    let db = db.await;
    let db_arc = Arc::new(db);
    let app = App::new()
        .document(Spec::default())
        .configure(|cfg| super::service::configure(db_arc.clone(), cfg))
        .build("docs");
    let service = test::init_service(app).await;
    let req = test::TestRequest::post()
        .uri("/keys/key1/ttl")
        .set_json(models::SetTtlRequest { ttl: 5 })
        .to_request();
    let resp = test::call_service(&service, req).await;
    assert!(
        resp.status().is_success(),
        "{:?}: {:?}",
        resp,
        resp.response().body()
    );

    let req = test::TestRequest::get().uri("/keys/key1/ttl").to_request();
    let resp = test::call_service(&service, req).await;
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

#[fixture]
async fn rocksdb() -> Box<dyn Storage> {
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
async fn bredis() -> Box<dyn Storage> {
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
async fn surrealkv() -> Box<dyn Storage> {
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
