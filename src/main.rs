#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
#![deny(clippy::as_conversions)]
#![allow(clippy::needless_return)]
#![allow(clippy::multiple_crate_versions)]
mod cli;
mod errors;
pub(crate) mod info;
mod http_server;
mod storages;

use std::sync::Arc;

use log::{debug, error};
use rand::random;

/// The main entry point of the program.
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));

    let matches = cli::make_cli().get_matches();

    if let Some(cmd_args) = matches.subcommand_matches("run") {
        let bind: &String = cmd_args.get_one("bind").unwrap();
        run(bind).await;
    }
}

#[allow(clippy::future_not_send)]
async fn run(bind: &str) {
    let db_path = format!("/dev/shm/bredis_{}", random::<i32>());

    debug!("Using database path: {db_path}");

    let db_result = storages::rocksdb::Rocksdb::open(db_path.as_str());
    if let Err(err) = db_result {
        error!("Error opening database: {err}");
        return;
    }
    let db = db_result.unwrap();
    let server = http_server::Server::new(Arc::new(db));

    if let Err(err) = server.serve(bind.to_owned()).await {
        error!("Error serving: {err}");
    }
}
