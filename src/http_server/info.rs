use actix_web::{
    web::{Data, Json},
    Responder,
};
use apistos::{
    api_operation,
    web::{self, resource},
};

use crate::info::Info;

use super::models;

/// Configures the `InfoService` with the given `ServiceConfig`.
///
/// # Arguments
///
/// * `cfg` - The `ServiceConfig` to configure.
pub fn configure(cfg: &mut apistos::web::ServiceConfig) {
    cfg.service(resource("/info").route(web::get().to(get)));
}

/// Retrieves the server information.
///
/// # Returns
///
/// A JSON response containing the server information.
#[api_operation(summary = "Get information about the server")]
pub async fn get(info: Data<Info>) -> impl Responder {
    return Json(models::InfoResponse {
        version: info.version.clone(),
        rustc: info.rustc.clone(),
        backend: info.backend.clone(),
        build_date: info.build_date.clone(),
    });
}
