use actix_web::web;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi()]
struct ApiDoc;
pub struct Service;

impl Service {
    /// Creates a new instance of the `InfoService`.
    ///
    /// # Returns
    ///
    /// A new instance of the `InfoService`.
    #[must_use]
    pub const fn new() -> Self {
        return Self;
    }

    /// Configures the `InfoService` with the given `ServiceConfig`.
    ///
    /// # Arguments
    ///
    /// * `cfg` - The `ServiceConfig` to configure.
    #[allow(clippy::unused_self)]
    pub fn config(self, cfg: &mut web::ServiceConfig) {
        let openapi = ApiDoc::openapi();
        cfg.service(
            SwaggerUi::new("/swagger-ui/{_:.*}").url("/docs/openapi.json", openapi.clone()),
        )
        .service(Redoc::with_url("/docs", openapi));
    }
}
