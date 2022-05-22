CREATE TABLE topic (
    topic_id SERIAL PRIMARY KEY,
    topic_name VARCHAR(256) NOT NULL UNIQUE,
    search_text VARCHAR(256) NOT NULL,
    landing_dir VARCHAR(256),
    archive_dir VARCHAR(256),
    stage_dir VARCHAR(256),
    catalog_dir VARCHAR(256),
    work_dir VARCHAR(256),
    created_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_dt TIMESTAMP WITHOUT TIME ZONE
);
