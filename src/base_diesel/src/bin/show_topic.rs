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

    let results = topic.load::<Topic>(&conn)
        .expect("Err: unable to load Topic");

    for topic in results {
        println!("{:?}", topic);
    }

    Ok(())
}
