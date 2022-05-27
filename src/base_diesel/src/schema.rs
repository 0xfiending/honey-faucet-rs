table! {
    flow (flow_id) {
        flow_id -> Int4,
        flow_name -> Varchar,
        topic_id -> Nullable<Int4>,
        is_active -> Bool,
        frequency -> Varchar,
        created_dt -> Timestamp,
        updated_dt -> Nullable<Timestamp>,
    }
}

table! {
    flow_step (flow_step_id) {
        flow_step_id -> Int4,
        flow_step_name -> Varchar,
        sequence_id -> Int4,
        flow_id -> Int4,
        input_dir -> Varchar,
        output_dir -> Varchar,
        script_path -> Varchar,
        script_parameters -> Nullable<Varchar>,
        created_dt -> Timestamp,
        updated_dt -> Nullable<Timestamp>,
    }
}

table! {
    job (job_id) {
        job_id -> Int4,
        job_name -> Varchar,
        flow_id -> Int4,
        status -> Varchar,
        created_dt -> Timestamp,
        updated_dt -> Nullable<Timestamp>,
    }
}

table! {
    job_step (id) {
        id -> Int4,
        job_id -> Int4,
        input_path -> Varchar,
        output_path -> Varchar,
        command -> Varchar,
        status -> Varchar,
        created_dt -> Timestamp,
        updated_dt -> Nullable<Timestamp>,
    }
}

table! {
    topic (topic_id) {
        topic_id -> Int4,
        topic_name -> Varchar,
        search_text -> Varchar,
        landing_dir -> Nullable<Varchar>,
        archive_dir -> Nullable<Varchar>,
        stage_dir -> Nullable<Varchar>,
        catalog_dir -> Nullable<Varchar>,
        work_dir -> Nullable<Varchar>,
        created_dt -> Timestamp,
        updated_dt -> Nullable<Timestamp>,
    }
}

joinable!(flow -> topic (topic_id));
joinable!(flow_step -> flow (flow_id));
joinable!(job -> flow (flow_id));
joinable!(job_step -> job (job_id));

allow_tables_to_appear_in_same_query!(
    flow,
    flow_step,
    job,
    job_step,
    topic,
);
