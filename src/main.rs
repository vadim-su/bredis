#![warn(clippy::pedantic)]
// #![warn(clippy::nursery)]
#![warn(clippy::cargo)]
#![deny(clippy::as_conversions)]
#![allow(clippy::needless_return)]
#![allow(clippy::multiple_crate_versions)]
#[allow(clippy::future_not_send)]
mod cli;
mod errors;
mod http_server;
pub(crate) mod info;
mod storages;

use log::{debug, error};
use rand::random;
use std::{fmt::Display, sync::Arc};
use storages::storage::Storage;

const DEFAULT_PORT: &str = "4123";

enum Backend {
    Rocksdb,
    Bredis,
    SurrealKV,
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Rocksdb => write!(f, "Rocksdb"),
            Backend::Bredis => write!(f, "Bredis"),
            Backend::SurrealKV => write!(f, "SurrealKV"),
        }
    }
}

/// The main entry point of the program.
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));

    let matches = cli::make_cli().get_matches();

    if let Some(cmd_args) = matches.subcommand_matches("run") {
        let bind: &String = cmd_args.get_one("bind").unwrap();
        let backend: &String = cmd_args.get_one("backend").unwrap();
        let backend = match backend.as_str() {
            "rocksdb" => Backend::Rocksdb,
            "bredis" => Backend::Bredis,
            "surrealkv" => Backend::SurrealKV,
            _ => {
                error!("Invalid backend: {backend}");
                return;
            }
        };
        run(bind, backend).await;
    }
}

#[allow(clippy::future_not_send)]
async fn run(bind: &str, backend: Backend) {
    let db: Arc<Box<dyn Storage>> = match backend {
        Backend::Rocksdb => {
            let db_path = format!("/dev/shm/bredis_{}", random::<i32>());

            debug!("Using database path: {db_path}");

            let db_result = storages::rocksdb::Rocksdb::open(db_path.as_str());
            if let Err(err) = db_result {
                error!("Error opening database: {err}");
                return;
            }
            let db = db_result.unwrap();
            Arc::new(Box::new(db))
        }
        Backend::Bredis => {
            let db = storages::bredis::Bredis::open();
            Arc::new(Box::new(db))
        }
        Backend::SurrealKV => {
            let db = storages::surrealkv::SurrealKV::open();
            Arc::new(Box::new(db))
        }
    };

    let server = http_server::Server::new(db);

    let (ip_str, port_str) = bind.split_once(':').unwrap_or((bind, DEFAULT_PORT));

    if let Err(err) = server
        .serve(
            ip_str.parse().unwrap(),
            port_str.parse().unwrap(),
            backend.to_string(),
        )
        .await
    {
        error!("Error serving: {err}");
    }
}
