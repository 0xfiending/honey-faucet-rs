use std::time::SystemTime;
use super::schema::*;

use serde::{Serialize, Deserialize};

#[derive(Queryable)]
pub struct Topic {
    pub topic_id: i32,
    pub topic_name: String,
    pub search_text: String,
    pub landing_dir: Option<String>,
    pub archive_dir: Option<String>,
    pub stage_dir: Option<String>,
    pub catalog_dir: Option<String>,
    pub work_dir: Option<String>,
    pub created_dt: SystemTime,
    pub updated_dt: Option<SystemTime>,
}

#[derive(Queryable)]
pub struct Flow {
    pub flow_id: i32,
    pub flow_name: String,
    pub topic_id: i32,
    pub frequency: String,
    pub is_active: bool,
    pub created_dt: SystemTime,
    pub updated_dt: Option<SystemTime>,
}

#[derive(Queryable)]
pub struct FlowStep {
    pub flow_step_id: i32,
    pub flow_step_name: String,
    pub sequence_id: i32,
    pub flow_id: i32,
    pub input_dir: String,
    pub output_dir: String,
    pub script_path: String,
    pub script_parameters: Option<String>,
    pub created_dt: SystemTime,
    pub updated_dt: Option<SystemTime>,
}

#[derive(Queryable)]
pub struct Job {
    pub job_id: i32,
    pub job_name: String,
    pub flow_id: i32,
    pub status: char,
    pub created_dt: SystemTime,
    pub updated_dt: Option<SystemTime>,
}

#[derive(Queryable)]
pub struct JobStep {
    pub job_step_id: i32,
    pub job_id: i32,
    pub input_path: String,
    pub output_path: String,
    pub command: String,
    pub status: String,
    pub created_dt: SystemTime,
    pub updated_dt: Option<SystemTime>,
}

#[derive(Deserialize, Insertable)]
#[table_name = "topic"]
pub struct TopicForm<'a> {
    topic_name: &'a str,
    search_text: &'a str,
    landing_dir: Option<&'a str>,
    archive_dir: Option<&'a str>,
    stage_dir: Option<&'a str>,
    catalog_dir: Option<&'a str>,
    work_dir: Option<&'a str>,
    created_dt: SystemTime,
    updated_dt: Option<SystemTime>,
}

#[derive(Deserialize, Insertable)]
#[table_name = "flow"]
pub struct FlowForm<'a> {
    flow_name: &'a str,
    topic_id: i32,
    is_active: bool,
    frequency: Option<&'a str>,
    created_dt: SystemTime,
    updated_dt: Option<SystemTime>,
}

#[derive(Deserialize, Insertable)]
#[table_name = "flow_step"]
pub struct FlowStepForm<'a> {
    flow_step_name: &'a str,
    sequence_id: i32,
    flow_id: i32,
    input_dir: &'a str,
    output_dir: &'a str,
    script_path: &'a str,
    script_parameters: Option<&'a str>,
    created_dt: SystemTime,
    updated_dt: Option<SystemTime>,
}
