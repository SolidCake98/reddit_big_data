CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reddit;

CREATE TABLE IF NOT EXISTS comments(
    uuid uuid,
    id text,
    name text,
    author text,
    body text,
    subreddit text,
    upvotes int,
    downvotes int,
    over_18 boolean,
    permalink text,
    api_timestamp timestamp,
    submission_id text,
    PRIMARY KEY((subreddit), api_timestamp)
)

WITH CLUSTERING ORDER BY (api_timestamp DESC);

CREATE INDEX IF NOT EXISTS ON comments (uuid);

CREATE TABLE IF NOT EXISTS posts(
    uuid uuid,
    id text,
    name text,
    author text,
    title text,
    selftext text,
    subreddit text,
    upvotes int,
    over_18 boolean,
    downvotes int,
    url text,
    api_timestamp timestamp,
    PRIMARY KEY((subreddit), api_timestamp)
)

WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON posts (uuid);
