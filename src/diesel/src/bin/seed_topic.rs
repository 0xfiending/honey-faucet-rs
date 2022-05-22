extern crate ct_nlp_diesel;
extern crate diesel;

use self::diesel::prelude::*;
use serde_json::json;
use std::time::SystemTime;

use ct_nlp_diesel::schema::topic;
use ct_nlp_diesel::establish_connection;

use ct_nlp_diesel::models::{TopicForm};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = establish_connection();
    let created_dt = SystemTime::now();
    let new_topic = json!({
        "topic_name": "NFT",
        "search_text": "nft",
        "landing_dir": Some(""),
        "archive_dir": Some(""),
        "stage_dir": Some(""),
        "catalog_dir": Some(""),
        "work_dir": Some(""),
        "created_dt": created_dt,
        "updated_dt": Some(created_dt),
    });

    let json_str = new_topic.to_string();
    let new_topic_form = serde_json::from_str::<TopicForm>(&json_str)?;

    diesel::insert_into(topic::table)
        .values(&new_topic_form)
        .execute(&conn)?;

    Ok(())
}
