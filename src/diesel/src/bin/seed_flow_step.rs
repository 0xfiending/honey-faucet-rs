extern crate ct_nlp_diesel;
extern crate diesel;

use self::diesel::prelude::*;
use serde_json::json;
use std::time::SystemTime;

use ct_nlp_diesel::schema::flow_step;
use ct_nlp_diesel::establish_connection;

use ct_nlp_diesel::models::{FlowStepForm};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = establish_connection();
    let created_dt = SystemTime::now();
    let new_flow_step = json!({
        "flow_step_name": "nlp-topic-land",
        "sequence_id": 1,
        "flow_id": 3,
        "input_dir": "",
        "output_dir": "/data/landing/",    // {x}/data/landing/{topic_id}/{date}/{file}
        "script_path": "/nlp_topic_land.rs",
        "script_parameters": Some(""),
        "created_dt": created_dt,
        "updated_dt": Some(created_dt),
    });

    let json_str = new_flow_step.to_string();
    let new_flow_step_form = serde_json::from_str::<FlowStepForm>(&json_str)?;

    diesel::insert_into(flow_step::table)
        .values(&new_flow_step_form)
        .execute(&conn)?;

    Ok(())
}
