use std::net::IpAddr;
/// Core server logic.
///
/// I have implemented the core server logic in this module, because to keep mod.rs clean.
use std::sync::Arc;

use actix_web::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceRequest, ServiceResponse};
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use crate::errors::Error;
use crate::http_server::{info, queries};
use crate::storages::storage::Storage;

use apistos::app::{BuildConfig, OpenApiWrapper};
use apistos::spec::Spec;
use apistos::ScalarConfig;

#[derive(Clone)]
pub struct Server {
    db: Arc<Box<dyn Storage>>,
}

impl Server {
    pub const fn new(db: Arc<Box<dyn Storage>>) -> Self {
        Self { db }
    }

    #[allow(clippy::future_not_send)]
    pub async fn serve(self, addr: IpAddr, port: u16, backend_name: String) -> Result<(), Error> {
        log::info!("Starting server on: {addr}:{port}");
        HttpServer::new(move || self.clone().make_app(backend_name.clone()))
            .bind((addr, port))?
            .run()
            .await?;

        Ok(())
    }

    fn make_app(
        self,
        backend_name: String,
    ) -> App<
        impl ServiceFactory<
            ServiceRequest,
            Response = ServiceResponse<impl MessageBody>,
            Config = (),
            InitError = (),
            Error = actix_web::error::Error,
        >,
    > {
        let info = crate::info::Info {
            backend: backend_name,
            ..Default::default()
        };

        let spec = Spec {
            info: apistos_models::info::Info {
                title: "Bredis API".to_string(),
                version: info.version.clone(),
                ..Default::default()
            },
            ..Default::default()
        };

        return App::new()
            .document(spec)
            .app_data(web::Data::new(info))
            .configure(info::configure)
            .configure(move |cfg| {
                queries::service::configure(self.db, cfg);
            })
            .wrap(Logger::default())
            .build_with(
                "/openapi.json",
                BuildConfig::default().with(ScalarConfig::new(&"/docs")),
            );
    }
}
