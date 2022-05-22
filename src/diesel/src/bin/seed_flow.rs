extern crate ct_nlp_diesel;
extern crate diesel;

use self::diesel::prelude::*;
use serde_json::json;
use std::time::SystemTime;

use ct_nlp_diesel::schema::flow;
use ct_nlp_diesel::establish_connection;

use ct_nlp_diesel::models::{FlowForm};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = establish_connection();
    let created_dt = SystemTime::now();
    let new_flow = json!({
        "flow_name": "std-nlp-topic-land",
        "topic_id": Some(1),
        "is_active": true,
        "frequency": "1D",
        "created_dt": created_dt,
        "updated_dt": Some(created_dt),
    });

    let json_str = new_flow.to_string();
    let new_flow_form = serde_json::from_str::<FlowForm>(&json_str)?;

    diesel::insert_into(flow::table)
        .values(&new_flow_form)
        .execute(&conn)?;

    Ok(())
}
