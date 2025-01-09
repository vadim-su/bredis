use std::sync::Arc;

use actix_web::{web, Responder};

use super::models;

pub struct Service {
    info: crate::info::Info,
}

/// Represents the Info service.
///
/// This service provides information about the server.
impl Service {
    /// Creates a new instance of the `InfoService`.
    ///
    /// # Returns
    ///
    /// A new instance of the `InfoService`.
    #[must_use]
    pub fn new() -> Self {
        return Self {
            info: crate::info::Info::default(),
        };
    }

    /// Configures the `InfoService` with the given `ServiceConfig`.
    ///
    /// # Arguments
    ///
    /// * `cfg` - The `ServiceConfig` to configure.
    pub fn config(self, cfg: &mut web::ServiceConfig) {
        let self_clone = Arc::new(self);
        cfg.service(web::resource("/info").route(web::get().to(move || {
            let self_clone = self_clone.clone();
            async move { self_clone.get().await }
        })));
    }

    /// Retrieves the server information.
    ///
    /// # Returns
    ///
    /// A JSON response containing the server information.
    pub async fn get(&self) -> impl Responder {
        web::Json(models::InfoResponse {
            version: self.info.version.clone(),
            rustc: self.info.rustc.clone(),
        })
    }
}
