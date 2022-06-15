\c prod;

CREATE TABLE IF NOT EXISTS job_step (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job (id) NOT NULL,
    flow_step_id INTEGER REFERENCES flow_step (id) NOT NULL,
    sequence_id INTEGER NOT NULL,
    input_path VARCHAR(256) NOT NULL,
    output_path VARCHAR(256) NOT NULL,
    command VARCHAR(256) NOT NULL,
    status VARCHAR(1) DEFAULT 'N' NOT NULL,
    created_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_dt TIMESTAMP WITHOUT TIME ZONE
);
