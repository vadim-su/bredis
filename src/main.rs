#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
#![deny(clippy::as_conversions)]
#![allow(clippy::needless_return)]
#![allow(clippy::multiple_crate_versions)]
use log::{debug, error};
use rand::random;

mod cli;
mod database;
mod errors;
pub(crate) mod info;
mod server;

/// The main entry point of the program.
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let matches = cli::make_cli().get_matches();

    if let Some(cmd_args) = matches.subcommand_matches("run") {
        let db_path = format!("/dev/shm/bredis_{}", random::<i32>());

        debug!("Using database path: {db_path}");

        let db_result = database::Database::open(db_path.as_str());
        if let Err(err) = db_result {
            error!("Error opening database: {err}");
            return;
        }
        let bind: &String = cmd_args.get_one("bind").unwrap();
        let db = db_result.unwrap();
        let server = server::Server::new(db);

        if let Err(err) = server.serve(bind.to_owned()).await {
            error!("Error serving: {err}");
        }
    }
}
