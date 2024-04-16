use std::sync::Arc;

use actix_web::web;

use crate::database::Database;
use crate::server::models;

use super::models::GetResponse;

pub struct QueryService {
    db: Arc<Database>,
}

impl QueryService {
    pub fn new(db: Arc<Database>) -> Self {
        QueryService { db }
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
            );

        cfg.app_data(web::Data::new(self.db.clone()))
            .service(scoped_services);
    }

    pub async fn get_by_key(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<GetResponse>> {
        let possible_value = db.get(key.as_bytes());
        return match possible_value {
            Ok(Some(value)) => {
                let raw_value = String::from_utf8(value);
                match raw_value {
                    Ok(value) => web::Json(models::ApiResponse::Success(models::GetResponse {
                        value: Some(value),
                    })),
                    Err(e) => {
                        web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                            error: format!("{}", e),
                        }))
                    }
                }
            }
            Ok(None) => web::Json(models::ApiResponse::Success(models::GetResponse {
                value: None,
            })),
            Err(e) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{}", e),
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
            Err(e) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{}", e),
            })),
        };
    }

    pub async fn set_key(
        db: web::Data<Arc<Database>>,
        request: web::Json<models::SetRequest>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let result = db.set(request.key.as_bytes(), request.value.as_bytes());
        return match result {
            Ok(_) => web::Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            )),
            Err(e) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{}", e),
            })),
        };
    }

    pub async fn delete_key(
        db: web::Data<Arc<Database>>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let result = db.delete(key.as_bytes());
        return match result {
            Ok(_) => web::Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            )),
            Err(e) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{}", e),
            })),
        };
    }

    pub async fn delete_keys(
        db: web::Data<Arc<Database>>,
        request: web::Json<models::DeleteKeysRequest>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        match db.delete_prefix(request.prefix.as_bytes()) {
            Ok(_) => {
                return web::Json(models::ApiResponse::Success(
                    models::OperationSuccessResponse { success: true },
                ))
            }
            Err(e) => {
                return web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                    error: format!("{}", e),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{test, App};

    use super::*;

    #[actix_web::test]
    async fn test_get_value() {
        let db = get_test_db();
        let query_service = QueryService::new(Arc::new(db));
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
        let query_service = QueryService::new(Arc::new(db));
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
                assert_eq!(keys.len(), 2);
            }
            _ => panic!("Unexpected response: {:?}", body),
        }
    }

    #[actix_web::test]
    async fn test_set_key() {
        let db = get_test_db();
        let query_service = QueryService::new(Arc::new(db.clone()));
        let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
        let req = test::TestRequest::post()
            .uri("/keys")
            .set_json(&models::SetRequest {
                key: "key3".to_string(),
                value: "value3".to_string(),
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
        let query_service = QueryService::new(Arc::new(db.clone()));

        let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
        let req = test::TestRequest::delete().uri("/keys/key1").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(
            resp.status().is_success(),
            "{:?}: {:?}",
            resp,
            resp.response().body()
        );

        assert_eq!(db.get(b"key1").unwrap(), None);
    }

    #[actix_web::test]
    async fn test_delete_keys() {
        let db = get_test_db();
        let query_service = QueryService::new(Arc::new(db.clone()));
        let app = test::init_service(App::new().configure(|cfg| query_service.config(cfg))).await;
        let req = test::TestRequest::delete()
            .uri("/keys")
            .set_json(&models::DeleteKeysRequest {
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

        assert_eq!(db.get(b"prefix_key1").unwrap(), None);
        assert_eq!(db.get(b"prefix_key2").unwrap(), None);
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
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
