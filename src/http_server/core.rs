/// Core server logic.
///
/// I have implemented the core server logic in this module, because to keep mod.rs clean.
use std::sync::Arc;

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceRequest, ServiceResponse};
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use crate::errors::Error;
use crate::http_server::{docs, info, queries};
use crate::storages::storage::Storage;

#[derive(Clone)]
pub struct Server {
    db: Arc<Box<dyn Storage>>,
}

impl Server {
    pub const fn new(db: Arc<Box<dyn Storage>>) -> Self {
        Self { db }
    }

    #[allow(clippy::future_not_send)]
    pub async fn serve(self, addr: String) -> Result<(), Error> {
        log::info!("Starting server on: {addr}");
        HttpServer::new(move || self.clone().make_app())
            .bind(addr)?
            .run()
            .await?;

        Ok(())
    }

    fn config(self, cfg: &mut web::ServiceConfig) {
        cfg.configure(move |cfg| info::Service::new().config(cfg));
        cfg.configure(move |cfg| {
            let query_service = queries::service::DatabaseQueries::new(self.db);
            query_service.config(cfg);
        });
        cfg.configure(move |cfg| docs::Service::new().config(cfg));
    }

    fn make_app(
        self,
    ) -> App<
        impl ServiceFactory<
            ServiceRequest,
            Response = ServiceResponse<impl MessageBody>,
            Config = (),
            InitError = (),
            Error = actix_web::error::Error,
        >,
    > {
        return App::new()
            .configure(|cfg: &mut web::ServiceConfig| self.config(cfg))
            .wrap(Logger::default());
    }
}
