use conf::{get_config, init_logger};

use chrono::Utc;
use log::info;
use clap::{ArgMatches, Arg, Command};
use std::collections::BTreeMap;
use std::time::SystemTime;

use diesel::prelude::*;
use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    expression::dsl::now,
    ExpressionMethods,
};

use base_diesel::{
    get_conn,
    schema::{
        job::{
            dsl::job,
            id as job_id,
            job_name,
            flow_id,
            start_dt,
            status as job_status,
        },
    },
};

fn usage() {
    println!("Usage: cargo run --bin job_controller -- --config <config>");
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
    let log_path = format!("{}/{}_job_controller.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("job_controller|starting");
    info!("job_controller|conf={}", config_name);

    let conn = match get_conn(
        config.get("pg_db").expect("ERR: conf [pg_db] is invalid"),
        config.get("pg_user").expect("ERR: conf [pg_user] is invalid"),
        config.get("pg_secret").expect("ERR: conf [pg_secret] is invalid"),
        config.get("pg_host").expect("ERR: conf [pg_host] is invalid"),
        config.get("pg_port").expect("ERR: conf [pg_post] is invalid"),
    ) {
        Ok(connection) => {
            info!("main|conn established");
            connection
        },
        Err(err) => {
            panic!("main|ERR: failed to connect to db|err={}", err);
        },
    };

    let jobs: Vec<(i32, String, i32, String, SystemTime)> = job
        .filter(job_status.eq("N"))
        .select((job_id, job_name, flow_id, job_status, start_dt))
        .load(&conn)
        .unwrap_or(vec![]);

    //println!("{:?}", jobs);
    // for each job, get flow steps
    // insert job steps
    // schedule cron job for first sequence_id
    // update flow status and flow_step status for first sequence
    // wait

    /*
    loop {
        

    }*/
    Ok(())
}
