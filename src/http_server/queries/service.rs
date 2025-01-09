use std::sync::Arc;

use actix_web::web;

use crate::{
    http_server::models,
    storages::{
        storage::Storage,
        value::{StorageValue, ValueType},
    },
};

/// A type alias for the storage type
type StorageType = Arc<dyn Storage + Send + Sync>;

pub struct DatabaseQueries {
    db: StorageType,
}

impl DatabaseQueries {
    #[must_use]
    pub const fn new(db: StorageType) -> Self {
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
            .service(web::resource("/{key_name}/dec").route(web::post().to(Self::decrement)))
            .service(
                web::resource("/{key_name}/ttl")
                    .route(web::get().to(Self::get_ttl))
                    .route(web::post().to(Self::set_ttl)),
            );

        cfg.app_data(web::Data::new(self.db.clone()))
            .service(scoped_services);
    }

    pub async fn get_by_key(
        db: web::Data<StorageType>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<models::GetResponse>> {
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
        db: web::Data<StorageType>,
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
        db: web::Data<StorageType>,
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
        db: web::Data<StorageType>,
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
        db: web::Data<StorageType>,
        request: Option<web::Json<models::DeleteKeysRequest>>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let prefix = match request {
            None => String::new(),
            Some(request) => request.prefix.clone(),
        };

        match db.delete_prefix(prefix.as_bytes()) {
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

    pub async fn get_ttl(
        db: web::Data<StorageType>,
        key: web::Path<String>,
    ) -> web::Json<models::ApiResponse<models::GetTtlResponse>> {
        let ttl = db.get_ttl(key.as_bytes());
        return match ttl {
            Ok(ttl) => web::Json(models::ApiResponse::Success(models::GetTtlResponse { ttl })),
            Err(crate::errors::DatabaseError::ValueNotFound(_)) => {
                web::Json(models::ApiResponse::Success(models::GetTtlResponse {
                    ttl: -1,
                }))
            }
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn set_ttl(
        db: web::Data<StorageType>,
        key: web::Path<String>,
        request: web::Json<models::SetTtlRequest>,
    ) -> web::Json<models::ApiResponse<models::OperationSuccessResponse>> {
        let result = db.update_ttl(key.as_bytes(), request.ttl);
        return match result {
            Ok(()) => web::Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            )),
            Err(err) => web::Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}"),
            })),
        };
    }

    pub async fn increment(
        db: web::Data<StorageType>,
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
        db: web::Data<StorageType>,
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
