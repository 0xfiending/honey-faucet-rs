use conf::{get_config, init_logger};

use log::info;
use std::result::Result;

fn usage() {
    println!("Usage: cargo run --bin nlp_topic_land -- --config <config> --topic <topic>");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    info!("main|starting");

    info!("main|completed");
    Ok(())
}
