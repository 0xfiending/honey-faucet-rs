use conf::{get_config, init_logger};

use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    ExpressionMethods,
};

use base_diesel::{
    get_conn,
    models::{
        //Flow,
        Job,
        JobForm,
    },
    schema::{
        job,
        flow::{
            id as flow_id,
            //topic_id as ft_id,
            flow_name,
            is_active,
            frequency,
            dsl::flow,
        },
    },
};

use std::{
    thread,
    time::{
        Duration,
        SystemTime,
    },
    collections::BTreeMap,
    str::FromStr,
};

use serde_json::json;
use cron::Schedule;
use chrono::Utc;
use log::info;
use clap::{ArgMatches, Arg, Command};

#[allow(dead_code)]
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
    info!("flow_controller|conf={}", config_name);

    let conn = match get_conn(
        config.get("pg_db").expect("ERR: conf [pg_db] is invalid"),
        config.get("pg_user").expect("ERR: conf [pg_user] is invalid"),
        config.get("pg_secret").expect("ERR: conf [pg_secret] is invalid"),
        config.get("pg_host").expect("ERR: conf [pg_host] is invalid"),
        config.get("pg_port").expect("ERR: conf [pg_port] is invalid"),
    ) {
        Ok(connection) => {
            info!("main|conn established");
            connection
        },
        Err(err) => {
            panic!("main|ERR: failed to connect to db|err={}", err);
        },
    };

    loop {
        let flows: Vec<(i32, String, String)> = flow
            .filter(is_active.eq(true))
            .select((flow_id, flow_name, frequency))
            .load(&conn)
            .unwrap_or(vec![]);

        if flows.len() > 0 {
            for _flow in flows {
                let (_flow_id, _flow_name, _frequency) = _flow;

                // process schedule
                let schedule = match Schedule::from_str(&_frequency) {
                    Ok(s) => s,
                    Err(err) => {
                        info!("main|schedule cannot be parsed|frequency={}|err={}", _frequency, err); 
                        continue
                    },
                };

                // get any scheduled times for the time
                let cur_date = Utc::today();
                let tasks = schedule
                    .upcoming(Utc)
                    .filter(|dt| dt.date() == cur_date);

                let _tmp_id = 1; // static, change later
                let mut n = 0;
                let now_dt = SystemTime::now();
                let now_secs = now_dt
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                for task in tasks {
                    let task_secs = task.timestamp() as u64;
                    let next_trigger = Duration::from_secs(task_secs - now_secs);
                    let scheduled_dt = match now_dt.checked_add(next_trigger) {
                        Some(next_time) => next_time,
                        _ => continue,
                    };
              
                    // job_name = {flow_name}_{topic_id}_{date}_{n}
                    let job_nm = format!(
                        "{}_{}_{}_{}",
                        _tmp_id,
                        _flow_name.replace("-", "_"),
                        cur_date.format("%Y_%m_%d"),
                        n,
                    );

                    let new_job = json!({
                        "job_name": &job_nm,
                        "flow_id": Some(_flow_id),
                        "status": Some("N"),
                        "created_dt": now_dt,
                        "start_dt": scheduled_dt,
                        "updated_dt": Some(now_dt),
                    });

                    let json_str = new_job.to_string();
                    let new_job_form = serde_json::from_str::<JobForm>(&json_str)?;

                    match diesel::insert_into(job::table)
                        .values(&new_job_form)
                        .get_result::<Job>(&conn)
                    {
                        Ok(result) => { info!("main|job_name: {} created|flow_id={}|job_id={}", job_nm, _flow_id, result.id); },
                        Err(err) => { info!("main|job insert failed|err={}",err); },
                    }

                    n += 1;
                }

                let timeout = Duration::from_secs(600 as u64);   // 600s = 10m
                info!("main|all flows have been processed|sleeping for {:?}", timeout);
                thread::sleep(timeout);
            }
        } else { println!("main|no active flows found"); }
    }
}
