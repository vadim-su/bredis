use std::sync::Arc;

use actix_web::web;

use crate::database::{Database, StorageValue, ValueType};
use crate::server::models;

use super::models::GetResponse;

pub struct Service {
    db: Arc<Database>,
}

impl Service {
    #[must_use]
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn config(&self, cfg: &mut web::ServiceConfig) {
        let scoped_services = web::scope("/keys")
            .service(
                web::resource("")
                    .route(web::get().to(Self::get_all_keys))
                    .route(web::post().to(Self::set_key))
                    .route(web::delete().to(Self::delete_keys)),
            )
            .service(
                web::resource("/{key_name}")
                    .route(web::get().to(Self::get_by_key))
                    .route(web::delete().to(Self::delete_key)),
            )
            .service(web::resource("/{key_name}/inc").route(web::post().to(Self::increment)))
            .service(web::resource("/{key_name}/dec").route(web::post().to(Self::decrement)));

        cfg.app_data(web::Data::new(self.db.clone()))
            .service(scoped_services);
    }

    pub async fn get_by_key(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<GetResponse>> {
        let possible_value = db.get(key.as_bytes());
        return match possible_value {
            Ok(Some(sotre_value)) => match sotre_value.value_type {
                ValueType::Integer => {
                    web::Json(models::ApiResponse::Success(models::GetResponse {
                        value: Some(models::IntOrString::Int(i64::from_be_bytes(
                            sotre_value.value.as_slice().try_into().unwrap(),
                        ))),
                    }))
                }
                ValueType::String => web::Json(models::ApiResponse::Success(models::GetResponse {
                    value: Some(models::IntOrString::String(
                        String::from_utf8(sotre_value.value).unwrap(),
                    )),
                })),
            },
            Ok(None) => web::Json(models::ApiResponse::Success(models::GetResponse {
                value: None,
            })),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn get_all_keys(
        db: web::Data<Arc<Database>>,
        web::Query(models::GetAllKeysQuery { prefix }): web::Query<models::GetAllKeysQuery>,
    ) -> web::Json<models::ApiResponse<models::GetAllKeysResponse>> {
        let keys = db.get_all_keys(prefix.as_bytes());
        return match keys {
            Ok(keys) => web::Json(models::ApiResponse::Success(models::GetAllKeysResponse {
                keys,
            })),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn set_key(
        db: web::Data<Arc<Database>>,
        request: web::Json<models::SetRequest>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let store_value = match &request.value {
            models::IntOrString::Int(i) => StorageValue {
                value_type: ValueType::Integer,
                ttl: request.ttl,
                value: i.to_be_bytes().to_vec(),
            },
            models::IntOrString::String(s) => StorageValue {
                value_type: ValueType::String,
                ttl: request.ttl,
                value: s.as_bytes().to_vec(),
            },
        };

        let result = db.set(request.key.as_bytes(), &store_value);
        return match result {
            Ok(()) => web::Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            )),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn delete_key(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let result = db.delete(key.as_bytes());
        return match result {
            Ok(()) => web::Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            )),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn delete_keys(
        db: web::Data<Arc<Database>>,
        request: web::Json<models::DeleteKeysRequest>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        match db.delete_prefix(request.prefix.as_bytes()) {
            Ok(()) => {
                return web::Json(models::ApiResponse::Success(
                    models::OperationSuccessResponse { success: true },
                ))
            }
            Err(err) => {
                return web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                    error: format!("{err}",),
                }))
            }
        }
    }

    pub async fn increment(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
        request: web::Json<models::IncrementRequest>,
    ) -> web::Json<models::ApiResponse<models::IncrementResponse>> {
        let store_value_result = db.increment(key.as_bytes(), request.value, request.default);
        if store_value_result.is_err() {
            return web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}", err = store_value_result.err().unwrap()),
            }));
        }

        return match store_value_result.unwrap().get_integer_value() {
            Ok(value) => web::Json(models::ApiResponse::Success(models::IncrementResponse {
                value,
            })),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn decrement(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
        request: web::Json<models::IncrementRequest>,
    ) -> web::Json<models::ApiResponse<models::IncrementResponse>> {
        let store_value_result = db.decrement(key.as_bytes(), request.value, request.default);
        if store_value_result.is_err() {
            return web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}", err = store_value_result.err().unwrap()),
            }));
        }

        return match store_value_result.unwrap().get_integer_value() {
            Ok(value) => web::Json(models::ApiResponse::Success(models::IncrementResponse {
                value,
            })),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{test, App};

    use crate::database::{StorageValue, ValueType};

    use super::*;

    #[actix_web::test]
    async fn test_get_value() {
        let db = get_test_db();
        let query_service = Service::new(Arc::new(db));
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
        let query_service = Service::new(Arc::new(db));
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

        let body: models::ApiResponse<models::GetAllKeysResponse> =
            test::read_body_json(resp).await;

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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));

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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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
        let query_service = Service::new(Arc::new(db.clone()));
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

    fn get_test_db() -> Database {
        let db_path = format!("/dev/shm/test_db_{}", rand::random::<i32>());
        let db = Database::open(db_path.as_str()).unwrap();

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
}
