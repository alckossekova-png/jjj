CREATE TABLE IF NOT EXISTS raw_users_by_posts (
    id_pk      SERIAL    PRIMARY KEY,
    post_id    INTEGER   NOT NULL UNIQUE,
    user_id    INTEGER   NOT NULL,
    title      VARCHAR,
    body       VARCHAR,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS top_users_by_posts (
    id_pk         SERIAL    PRIMARY KEY,
    user_id       INTEGER   NOT NULL UNIQUE,
    posts_count   INTEGER   NOT NULL,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


SELECT 'Database initialization complete: raw_users_by_posts and top_users_by_posts tables created with SERIAL PK.' AS status;