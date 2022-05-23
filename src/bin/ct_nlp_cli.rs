use ct_nlp::{get_recent_tweets, get_tweet_counts, tweet_lookup};
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
            ).await;    

            match result {
                Ok(count) => {
                    let data = match count["data"].as_array() {
                        Some(x) => x,
                        _ => return Err("get_tweet_counts|ERR: unable to parse data object".into()),
                    };

                    println!();
                    for row in data {
                        let start_dt = row["start"].to_string();
                        let count = match row["tweet_count"].as_u64() {
                            Some(x) => { println!("{}|count={}", &start_dt[1..start_dt.len()-1], x); },
                            None => { continue; },
                        };
                    }
                    println!();
                },
                Err(e) => { info!("main|ERR: unable to parse counts object|e={:?}", e); },
            }
        },
        "tweet_lookup" => {
            let result = tweet_lookup(
                config.get("bearer_token").expect("ERR: bearer_token is invalid"),
                cli_args.value_of("tweet_id").expect("ERR: cli [tweet_id] is invalid"),
            ).await;

            match result {
                Ok(tweet) => { 
                    let data = match tweet["data"].as_object() {
                        Some(x) => x,
                        None => panic!("main|ERR: unable to parse data object"),
                    };

                    let users = match tweet["includes"].as_object() {
                        Some(includes) => match includes["users"].as_array() {
                            Some(users) => users,
                            None => panic!("main|ERR: unable to parse users object"),
                        },
                        None => panic!("main|ERR: unable to parse includes object"),
                    };

                    let mut author_id = data["author_id"].to_string();
                    let mut created_at = data["created_at"].to_string();
                    let mut text = data["text"].to_string();
                    let mut tweet_id = data["id"].to_string();
                    let mut author_name = users[0]["name"].to_string();
                    let mut author_username = users[0]["username"].to_string();

                    author_id = author_id[1..author_id.len()-1].to_string();
                    created_at = created_at[1..created_at.len()-1].to_string();
                    text = text[1..text.len()-1].to_string();
                    tweet_id = tweet_id[1..tweet_id.len()-1].to_string();
                    author_name = author_name[1..author_name.len()-1].to_string();
                    author_username = author_username[1..author_username.len()-1].to_string();

                    println!("\nAuthor: {} // {} ({})\nCreated Dt: {}\nText: {}\n", 
                            author_name, 
                            author_username, 
                            author_id, 
                            created_at,
                            text);
                },
                Err(e) => { info!("main|ERR: unable to parse tweet object|e={:?}", e); },
            }
        },
        _ => {
            usage();
            std::process::exit(1);
        },
    }

    info!("main|completed");
    Ok(())
}
