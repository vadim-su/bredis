use clap::{crate_authors, crate_name, Arg, Command};

use crate::info::Info;

pub fn make_cli() -> Command {
    let info = Info::default();

    return Command::new(crate_name!())
        .about("Bredis is a Redis-like database with similar functions and an HTTP API.")
        .version(format!("{} (rustc: {})", info.version, info.rustc))
        .author(crate_authors!(",\n"))
        .subcommand_required(true)
        .subcommand(
            Command::new("run").about("Run the Bredis server").arg(
                Arg::new("bind")
                    .short('b')
                    .long("bind")
                    .value_name("BIND")
                    .help("Address to bind to")
                    .default_value("[::1]:4123"),
            ),
        );
}
