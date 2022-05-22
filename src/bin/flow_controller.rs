use conf::{get_config, init_logger};

use chrono::Utc;
use log::info;
use clap::{ArgMatches, Arg, Command};
use std::collections::BTreeMap;

fn usage() {
    println!("Usage: cargo run --bin flow_controller -- --config <config>");
}

fn parse_args() -> clap::ArgMatches {
    let cli_args = Command::new("boot")
        .args(&[
            Arg::new("conf")
                .long("config")
                .short('c')
                .takes_value(true)
                .required(true),
            Arg::new("help")
                .long("help")
                .short('h'),
        ])
        .get_matches();

    cli_args
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: ArgMatches = parse_args();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid");
    let config: BTreeMap<String, String> = get_config(config_name);

    let dt = Utc::now().to_rfc3339();
    let log_dir = String::from(config.get("log_dir").expect("ERR: log_dir is invalid"));
    let log_path = format!("{}/{}_flow_controller.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("flow_controller|starting");

    loop {
        

    }
}
