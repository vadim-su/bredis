use std::sync::Arc;

use actix_web::web::{Data, Json, Path, Query};
use apistos::api_operation;
use apistos::web::{self, ServiceConfig};

use crate::{
    http_server::models,
    storages::{
        storage::Storage,
        value::{StorageValue, ValueType},
    },
};

/// A type alias for the storage type
pub type StorageType = Arc<Box<dyn Storage>>;

pub fn configure(db: StorageType, cfg: &mut ServiceConfig) {
    let scoped_services = web::scope("/keys")
        .service(
            web::resource("")
                .route(web::get().to(get_all_keys))
                .route(web::post().to(set_key))
                .route(web::delete().to(delete_keys)),
        )
        .service(
            web::resource("/{key_name}")
                .route(web::get().to(get_by_key))
                .route(web::delete().to(delete_key)),
        )
        .service(web::resource("/{key_name}/inc").route(web::post().to(increment)))
        .service(web::resource("/{key_name}/dec").route(web::post().to(decrement)))
        .service(
            web::resource("/{key_name}/ttl")
                .route(web::get().to(get_ttl))
                .route(web::post().to(set_ttl)),
        );

    cfg.app_data(Data::new(db)).service(scoped_services);
}

#[api_operation(summary = "Get key by provided key")]
pub async fn get_by_key(
    db: Data<StorageType>,
    key: Path<String>,
) -> Json<models::ApiResponse<models::GetResponse>> {
    let possible_value = db.get(key.as_bytes()).await;
    return match possible_value {
        Ok(Some(store_value)) => match store_value.value_type {
            ValueType::Integer => Json(models::ApiResponse::Success(models::GetResponse {
                value: Some(models::IntOrString::Int(i64::from_be_bytes(
                    store_value.value.as_slice().try_into().unwrap(),
                ))),
            })),
            ValueType::String => Json(models::ApiResponse::Success(models::GetResponse {
                value: Some(models::IntOrString::String(
                    String::from_utf8(store_value.value).unwrap(),
                )),
            })),
        },
        Ok(None) => Json(models::ApiResponse::Success(models::GetResponse {
            value: None,
        })),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Get all keys")]
pub async fn get_all_keys(
    db: Data<StorageType>,
    Query(models::GetAllKeysQuery { prefix }): Query<models::GetAllKeysQuery>,
) -> Json<models::ApiResponse<models::GetAllKeysResponse>> {
    let keys = db.get_all_keys(prefix.as_bytes()).await;
    return match keys {
        Ok(keys) => Json(models::ApiResponse::Success(models::GetAllKeysResponse {
            keys,
        })),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Set a key's value")]
pub async fn set_key(
    db: Data<StorageType>,
    request: Json<models::SetRequest>,
) -> Json<models::ApiResponse<models::OperationSuccessResponse>> {
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

    let result = db.set(request.key.as_bytes(), &store_value).await;
    return match result {
        Ok(()) => Json(models::ApiResponse::Success(
            models::OperationSuccessResponse { success: true },
        )),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Delete a specific key")]
pub async fn delete_key(
    db: Data<StorageType>,
    key: Path<String>,
) -> Json<models::ApiResponse<models::OperationSuccessResponse>> {
    let result = db.delete(key.as_bytes()).await;
    return match result {
        Ok(()) => Json(models::ApiResponse::Success(
            models::OperationSuccessResponse { success: true },
        )),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Delete keys with a provided prefix")]
pub async fn delete_keys(
    db: Data<StorageType>,
    request: Option<Json<models::DeleteKeysRequest>>,
) -> Json<models::ApiResponse<models::OperationSuccessResponse>> {
    let prefix = match request {
        None => String::new(),
        Some(request) => request.prefix.clone(),
    };

    match db.delete_prefix(prefix.as_bytes()).await {
        Ok(()) => {
            return Json(models::ApiResponse::Success(
                models::OperationSuccessResponse { success: true },
            ))
        }
        Err(err) => {
            return Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
                error: format!("{err}",),
            }))
        }
    }
}

#[api_operation(summary = "Get time-to-live for a key")]
pub async fn get_ttl(
    db: Data<StorageType>,
    key: Path<String>,
) -> Json<models::ApiResponse<models::GetTtlResponse>> {
    let ttl = db.get_ttl(key.as_bytes()).await;
    return match ttl {
        Ok(ttl) => Json(models::ApiResponse::Success(models::GetTtlResponse { ttl })),
        Err(crate::errors::DatabaseError::ValueNotFound(_)) => {
            Json(models::ApiResponse::Success(models::GetTtlResponse {
                ttl: -1,
            }))
        }
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Set time-to-live for a key")]
pub async fn set_ttl(
    db: Data<StorageType>,
    key: Path<String>,
    request: Json<models::SetTtlRequest>,
) -> Json<models::ApiResponse<models::OperationSuccessResponse>> {
    let result = db.update_ttl(key.as_bytes(), request.ttl).await;
    return match result {
        Ok(()) => Json(models::ApiResponse::Success(
            models::OperationSuccessResponse { success: true },
        )),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Increment a key's integer value")]
pub async fn increment(
    db: Data<StorageType>,
    key: Path<String>,
    request: Json<models::IncrementRequest>,
) -> Json<models::ApiResponse<models::IncrementResponse>> {
    let store_value_result = db
        .increment(key.as_bytes(), request.value, request.default)
        .await;
    if store_value_result.is_err() {
        return Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}", err = store_value_result.err().unwrap()),
        }));
    }

    return match store_value_result.unwrap().get_integer_value() {
        Ok(value) => Json(models::ApiResponse::Success(models::IncrementResponse {
            value,
        })),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}

#[api_operation(summary = "Decrement a key's integer value")]
pub async fn decrement(
    db: Data<StorageType>,
    key: Path<String>,
    request: Json<models::IncrementRequest>,
) -> Json<models::ApiResponse<models::IncrementResponse>> {
    let store_value_result = db
        .decrement(key.as_bytes(), request.value, request.default)
        .await;
    if store_value_result.is_err() {
        return Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}", err = store_value_result.err().unwrap()),
        }));
    }

    return match store_value_result.unwrap().get_integer_value() {
        Ok(value) => Json(models::ApiResponse::Success(models::IncrementResponse {
            value,
        })),
        Err(err) => Json(models::ApiResponse::ErrorResponse(models::ErrorResponse {
            error: format!("{err}"),
        })),
    };
}
