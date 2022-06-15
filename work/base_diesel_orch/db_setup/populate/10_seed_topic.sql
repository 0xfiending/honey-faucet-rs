-- TOPIC POPULATE
\c prod;

-- Generic NFT
INSERT INTO topic(
    topic_name,
    search_text,
    landing_dir,
    archive_dir,
    stage_dir,
    catalog_dir,
    work_dir,
    created_dt,
    updated_dt
) VALUES (
    'NFT',
    'nft',
    '/Users/daemon1/Dev/dev5/data/landing/1',
    '/Users/daemon1/Dev/dev5/data/archive/1',
    '/Users/daemon1/Dev/dev5/data/stage/1',
    '/Users/daemon1/Dev/dev5/data/catalog/1',
    '/Users/daemon1/Dev/dev5/data/work/1',
    now(),
    now()
);
