use rand::random;

mod cli;
mod database;
pub(crate) mod info;
mod server;

/// The main entry point of the program.
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let matches = cli::make_cli().get_matches();

    if let Some(cmd_args) = matches.subcommand_matches("run") {
        let db_path = format!("/dev/shm/bredis{}", random::<i32>());

        let db_result = database::Database::open(db_path.as_str());
        if let Err(e) = db_result {
            eprintln!("Error opening database: {e}");
            return;
        }
        let bind: &String = cmd_args.get_one("bind").unwrap();
        let db = db_result.unwrap();
        let server = server::Server::new(db);
        if let Err(e) = server.serve(bind.to_owned()).await {
            eprintln!("Error serving: {e}");
        }
    }
}
