use ct_nlp::{get_recent_tweets, get_tweet_counts};
use conf::{parse_args, get_config, init_logger};

use chrono::Utc;
use log::info;
use clap::ArgMatches;
use std::collections::BTreeMap;
use std::result::Result;
use std::error::Error;

fn usage() {
    println!("Usage: cargo run --bin ct_nlp_cli  -- --topic <topic> --config <config> --action <action>");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: ArgMatches = parse_args();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid");
    let config: BTreeMap<String, String> = get_config(config_name);

    let dt = Utc::now().to_rfc3339();
    let log_dir = String::from(config.get("log_dir").expect("ERR: log_dir is invalid"));
    let log_path = format!("{}/{}_ct_nlp_cli.log", &log_dir, &dt[0..19]);

    init_logger(&log_path);
    info!("main|starting");

    let cmd = String::from(cli_args.value_of("action").expect("ERR: cli [action] is invalid"));
    match cmd.as_str() {
        "recent" => { 
            let df = get_recent_tweets(
                config.get("bearer_token").expect("ERR: bearer_token is invalid"),
                cli_args.value_of("topic").expect("ERR: cli [topic] is invalid"),
                "100"
            ).await?;

            let id_col: Vec<&str> = df.column("tweet_id")?
                .utf8()?
                .into_no_null_iter()
                .collect();  
            let text_col: Vec<&str> = df.column("text")?
                .utf8()?
                .into_no_null_iter()
                .collect();
            let created_col: Vec<&str> = df.column("created_at")?
                .utf8()?
                .into_no_null_iter()
                .collect();
            
            for i in 0..id_col.len() {
                println!("    {}|{}|{}", id_col[i], created_col[i], text_col[i]);
            }

            info!("main|recent|completed");
        },
        "counts" => { 
            let result = get_tweet_counts(
                config.get("bearer_token").expect("ERR: bearer_token is invalid"),
                cli_args.value_of("topic").expect("ERR: cli [topic] is invalid"),
            ).await?;    

            match result { 
                () => info!("main|counts|completed"),
                _ => info!("main|counts|failed"),
            }
        },
        "tweet_lookup" => {
            println!("tweet_lookup");

        },
        _ => {
            usage();
            std::process::exit(1);
        },
    }

    info!("main|completed");
    Ok(())
}
