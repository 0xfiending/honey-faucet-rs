CREATE TABLE flow_step (
    flow_step_id SERIAL PRIMARY KEY,
    flow_step_name VARCHAR(256) NOT NULL UNIQUE,
    sequence_id INTEGER NOT NULL,
    flow_id INTEGER REFERENCES flow (flow_id) NOT NULL,
    input_dir VARCHAR(256) NOT NULL,
    output_dir VARCHAR(256) NOT NULL,
    script_path VARCHAR(256) NOT NULL,
    script_parameters VARCHAR(256),
    created_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_dt TIMESTAMP WITHOUT TIME ZONE
);

ALTER TABLE flow_step ADD CONSTRAINT flow_sequence_id_check CHECK (sequence_id > 0);
