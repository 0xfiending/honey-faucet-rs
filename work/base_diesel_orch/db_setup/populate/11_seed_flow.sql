-- FLOW POPULATE
\c prod;

-- RECENTS FLOW 
INSERT INTO FLOW(
    flow_name,
    topic_id,
    is_active,
    frequency,
    created_dt,
    updated_dt
) VALUES (
    'nlp-std-recent-flow',
    1,
    true,
    '0 5 * * * * *',
    now(),
    now()
);
