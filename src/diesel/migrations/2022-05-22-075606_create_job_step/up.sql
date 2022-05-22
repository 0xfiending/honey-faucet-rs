CREATE TABLE job_step (
    job_step_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job (job_id) NOT NULL,
    input_path VARCHAR(256) NOT NULL,
    output_path VARCHAR(256) NOT NULL,
    command VARCHAR(256) NOT NULL,
    status VARCHAR(1) DEFAULT 'N' NOT NULL,
    created_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_dt TIMESTAMP WITHOUT TIME ZONE
);
