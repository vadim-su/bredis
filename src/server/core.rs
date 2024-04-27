/// Core server logic.
///
/// I
use std::sync::Arc;

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceRequest, ServiceResponse};
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use crate::database::Database;
use crate::server::{docs, info, query};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Server {
    db: Arc<Database>,
}

impl Server {
    pub fn new(db: Database) -> Self {
        Self { db: Arc::new(db) }
    }

    #[allow(clippy::future_not_send)]
    pub async fn serve(self, addr: String) -> Result<(), Error> {
        HttpServer::new(move || self.clone().make_app())
            .bind(addr)?
            .run()
            .await?;

        Ok(())
    }

    fn config(self, cfg: &mut web::ServiceConfig) {
        cfg.configure(move |cfg| info::Service::new().config(cfg));
        cfg.configure(move |cfg| {
            let query_service = query::Service::new(self.db);
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
