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
    num_comments int,
    name text,
    author text,
    title text,
    selftext text,
    subreddit text,
    score int,
    over_18 boolean,
    url text,
    api_timestamp timestamp,
    PRIMARY KEY((subreddit), api_timestamp)
)

WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON posts (uuid);

CREATE TABLE IF NOT EXISTS posts_sentiment_score(
    post_uid uuid,
    subreddit text,
    api_timestamp timestamp,
    sentiment_score_title float,
    PRIMARY KEY((subreddit), api_timestamp)
)

WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON posts_sentiment_score (post_uid);

CREATE TABLE IF NOT EXISTS comments_sentiment_score(
    comment_uid uuid,
    subreddit text,
    api_timestamp timestamp,
    sentiment_score_title float,
    PRIMARY KEY((subreddit), api_timestamp)
)

WITH CLUSTERING ORDER BY (api_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON comments_sentiment_score (comment_uid);


CREATE TABLE IF NOT EXISTS posts_sentiment_score_avg(
    uuid uuid,
    subreddit text,
    ingest_timestamp timestamp,
    sentiment_score_avg float,
    PRIMARY KEY((subreddit), ingest_timestamp)
)

WITH CLUSTERING ORDER BY (ingest_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON posts_sentiment_score_avg (uuid);

CREATE TABLE IF NOT EXISTS comments_sentiment_score_avg(
    uuid uuid,
    subreddit text,
    ingest_timestamp timestamp,
    sentiment_score_avg float,
    PRIMARY KEY((subreddit), ingest_timestamp)
)

WITH CLUSTERING ORDER BY (ingest_timestamp DESC);
CREATE INDEX IF NOT EXISTS ON comments_sentiment_score_avg (uuid);