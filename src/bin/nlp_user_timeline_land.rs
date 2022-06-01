use conf::{parse_args1, init_logger, get_config};
use ct_nlp::{users_lookup, user_timeline};

use diesel::{
    query_dsl::{QueryDsl, RunQueryDsl},
    expression::dsl::now,
    ExpressionMethods,
};

use base_diesel::{
    models::JobStep,
    schema::{
        topic::dsl::topic,
        topic::id as topic_id,
        topic::search_text,
    },
    schema::{
        job_step::dsl::*,
        //job_step::id as job_step_id,
        job_step::status,
        job_step::updated_dt,
    },
    get_conn,
};

use std::{
    collections::BTreeMap,
    result::Result,
    path::Path,
    fs::File,
};

use log::info;
use clap::ArgMatches;
use chrono::Utc;
use polars::prelude::*;

#[allow(dead_code)]
fn usage() {
    println!("Usage: cargo run 
    --bin nlp_user_timeline_land 
    -- 
    --job_step_id <job> 
    --config <config> 
    --topic_id <topic> 
    --output_dir <output_dir>");
}

// TODO: not sure if flow_step failure update 
// should happen here or just propagate back to controller 
// and handle there...tbd

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: ArgMatches = parse_args1();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid");
    let output_dir = cli_args.value_of("output").expect("ERR: cli [output_dir] is invalid");
    let t_id = cli_args.value_of("topic").expect("ERR: cli [topic_id] is invalid")
        .parse::<i32>().expect("ERR: topic_id <i32> parse failed");
    let js_id = cli_args.value_of("job_step").expect("ERR: cli [job_step_id] is invalid")
        .parse::<i32>().expect("ERR: job_step_id <i32> parse failed");

    let config: BTreeMap<String, String> = get_config(config_name);

    let dt = Utc::now().to_rfc3339();
    let log_dir = String::from(config.get("log_dir").expect("ERR: log_dir is invalid"));
    let log_path = format!("{}/{}_nlp_user_timeline_land.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("main|starting");
    info!("main|topic_id={}", t_id);
    info!("main|job_step_id={}", js_id);

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
            // update failure
            panic!("main|ERR: failed to connect to db|err={}", err);
        }
    };

    let topics = topic
        .filter(topic_id.eq(t_id))
        .select(search_text)  
        .limit(1)
        .load::<String>(&conn)
        .expect(&format!("main|ERR: topic not found for topic_id={}", t_id));

    let target = match topics.is_empty() {
        true => { 
            panic!("main|ERR: topic not found for topic_id={}", t_id);
            // update failure 
        },
        false => &topics[0],
    }; 

    let result = users_lookup(
        config.get("bearer_token").expect("ERR: conf [bearer_token] is invalid)"),
        target,
    ).await;

    let user_id = match result {
        Ok(data) =>  { 
            let user = match data["data"].as_array() {
                Some(user) => user,
                None => panic!("main|ERR: unable to parse data object"),
            };

            let mut user_id = user[0]["id"].to_string();
            user_id = user_id[1..user_id.len()-1].to_string();
            user_id
        },
        Err(err) => {
            // update failure
            panic!("main|username {} cannot be looked up|err={}", target, err)
        },
    };

    let df = user_timeline(
        config.get("bearer_token").expect("ERR: bearer_token is invalid"),
        &user_id,
    ).await;

    match Path::new(&output_dir).exists() {
        true => info!("main|output_dir={}", output_dir),
        false => {
            std::fs::create_dir_all(&output_dir)?;
            info!("main|{} created successfully", output_dir);
        },
    }

    let out_path = format!("{}/{}_nlp_user_timeline_land.parquet", output_dir, &dt[0..10]);
    match Path::new(&out_path).exists() {
        true => {
            info!("main|{} exists|attempting remove", out_path);
            std::fs::remove_file(&out_path)?;
            info!("main|{} successfully removed, proceeding with write...", out_path);
        },
        false => info!("main|out_path={}", out_path),
    }

    match df {
        Ok(frame) => {
            let mut out_df: polars::frame::DataFrame = frame;
            let output_file = File::create(out_path).expect("main|ERR: cannot create output file");
            match ParquetWriter::new(output_file)
                .finish(&mut out_df) 
            {
                Ok(_) => info!("main|file created successfully"),
                Err(err) => info!("main|ERR: unable to write to file|err={}", err),
            }
        },
        Err(err) => { 
            // update failure
            panic!("main|ERR: unable to write to file|err={}", err);
        },
    }

    // update flow 
    let result = diesel::update(job_step)
        .filter(id.eq(js_id))
        .set((
            status.eq("C"),
            updated_dt.eq(now),
        ))
        .get_result::<JobStep>(&conn);

    match result {
        Ok(_) => info!("main|nlp_recent_topic_land completed for job_step_id={}", js_id),
        Err(err) => info!("main|ERR: failed to update db for job_step_id={}|e={}", js_id, err),
    }

    info!("main|completed");
    Ok(())
}

