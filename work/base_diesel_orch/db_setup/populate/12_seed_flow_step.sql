-- FLOW STEP POPULATE
\c prod;

-- FLOW - nlp-std-recent-flow
INSERT INTO flow_step(
    flow_step_name,
    sequence_id,
    flow_id,
    input_dir,
    output_dir,
    script_path,
    script_parameters,
    created_dt,
    updated_dt
) VALUES (
    'tw-recent-nlp-land',
    1,
    1, 
    '',
    '/Users/daemon1/Dev/dev6/data/landing/1',
    'nlp_recent_topic_land',
    '--job_step_id,--config,--topic_id,--output_dir',
    now(),
    now()
);
