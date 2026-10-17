-- SQLite schema (5 tables, plus 4 that track delivery to the pipeline database and 1 that keeps the
-- last account list read from it).
-- {TS}, {JSON}, {BOOL}, {BIGINT} are replaced by sql_store.py.

CREATE TABLE IF NOT EXISTS monitored_users (
    user_id                 TEXT PRIMARY KEY,
    config_username         TEXT,
    username                TEXT,
    active                  {BOOL} NOT NULL,
    status                  TEXT NOT NULL,
    since_id                TEXT,
    fetch_gaps              {JSON},          -- ranges not collected yet (page cap), filled later
    added_at                {TS} NOT NULL,
    last_posts_fetch_at     {TS},
    last_attempt_at         {TS},            -- last run that tried this account (ok or failed)
    last_profile_refresh_at {TS},
    last_scraped_at         {TS},            -- last run that collected and saved its posts
    posts_last_run          INTEGER          -- own posts saved by that run (new + recovered from gaps)
);

-- One row per account: always the latest profile. Changes to tracked fields
-- (see TRACKED_PROFILE_FIELDS in models.py) are appended to the `changes` JSON list.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id           TEXT PRIMARY KEY,
    username          TEXT,
    name              TEXT,
    description       TEXT,
    location          TEXT,
    url               TEXT,
    created_at        {TS},
    verified          {BOOL},
    verified_type     TEXT,
    protected         {BOOL},
    followers_count   {BIGINT},
    following_count   {BIGINT},
    tweet_count       {BIGINT},
    listed_count      {BIGINT},
    profile_image_url TEXT,
    pinned_tweet_id   TEXT,
    status            TEXT,
    raw_json          {JSON},
    first_seen_at     {TS} NOT NULL,
    refreshed_at      {TS} NOT NULL,
    last_changed_at   {TS},
    changes           {JSON} NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
    post_id             TEXT PRIMARY KEY,
    author_id           TEXT,
    username            TEXT,
    created_at          {TS},
    post_url            TEXT,
    text                TEXT,
    full_text           TEXT,
    lang                TEXT,
    conversation_id     TEXT,
    in_reply_to_user_id TEXT,
    post_type           TEXT,
    referenced_post_id  TEXT,
    referenced_json     {JSON},
    referenced_full_text  TEXT,              -- complete text of the tweet referenced_post_id points to
    referenced_author_id  TEXT,              -- (retweet: original, reply: parent, quote: quoted tweet);
    referenced_created_at {TS},              -- NULL when X could not return it (deleted, protected)
    possibly_sensitive  {BOOL},
    retweet_count       {BIGINT},
    reply_count         {BIGINT},
    like_count          {BIGINT},
    quote_count         {BIGINT},
    bookmark_count      {BIGINT},
    impression_count    {BIGINT},
    metrics_fetched_at  {TS},
    first_seen_at       {TS} NOT NULL,
    raw_json            {JSON}
);

CREATE INDEX IF NOT EXISTS idx_posts_author_created ON posts (author_id, created_at);

CREATE TABLE IF NOT EXISTS media (
    media_key         TEXT NOT NULL,
    post_id           TEXT NOT NULL,
    type              TEXT,
    url               TEXT,
    preview_image_url TEXT,
    duration_ms       {BIGINT},
    width             {BIGINT},
    height            {BIGINT},
    alt_text          TEXT,
    variants_json     {JSON},
    PRIMARY KEY (media_key, post_id)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    started_at    {TS} NOT NULL,
    finished_at   {TS},
    users_ok      INTEGER,
    users_failed  INTEGER,
    posts_new     INTEGER,                   -- the monitored accounts' own posts
    user_reads    INTEGER,
    post_reads    INTEGER,                   -- all billed posts: own posts + referenced tweets
    referenced_reads INTEGER,                -- referenced tweets returned (part of post_reads)
    est_cost_usd  REAL,
    errors_json   {JSON},                    -- [{time, account, message}, ...] (older runs: plain texts)
    credits_before REAL,                     -- X credit balance (USD) at the start of the run
    credits_after  REAL,                     -- ... and at the end
    credits_used   REAL,                     -- before - after: what X actually charged (NULL = unknown)
    warnings_json  {JSON},                   -- [{time, account, message}, ...]
    status         TEXT,                     -- success / partial_success / failed
    items_loaded   INTEGER,                  -- accounts to collect this run
    items_processed INTEGER,                 -- accounts actually processed
    pipeline_inserted   INTEGER,             -- rows added to twitter_posts by this run
    pipeline_duplicates INTEGER              -- posts skipped there because the tweet_id existed
);

-- Delivery to the pipeline database (storage/pipeline.py), per database ("host:port/dbname"):
-- since when posts are delivered (no initial load), and what has been delivered already.
CREATE TABLE IF NOT EXISTS pipeline_targets (
    target      TEXT PRIMARY KEY,
    posts_since {TS} NOT NULL,               -- posts first seen from here on, and runs started from here on
    created_at  {TS} NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_sent_posts (
    target   TEXT NOT NULL,
    post_id  TEXT NOT NULL,
    sent_at  {TS} NOT NULL,
    PRIMARY KEY (target, post_id)
);

CREATE TABLE IF NOT EXISTS pipeline_sent_profiles (
    target    TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    freshness {TS},                          -- the profile version delivered (latest refresh / status change)
    sent_at   {TS} NOT NULL,
    digest    TEXT,                          -- fingerprint of the row delivered: resent only when it differs
    PRIMARY KEY (target, user_id)
);

-- The account list last read from monitor_targets (usernames_source "database"), per database: used when
-- that database cannot be read at the start of a run (src/targets.py).
CREATE TABLE IF NOT EXISTS target_list_cache (
    target     TEXT PRIMARY KEY,             -- "host:port/dbname"
    usernames  {JSON} NOT NULL,              -- the enabled usernames, in collection order
    read_at    {TS} NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_sent_runs (
    target   TEXT NOT NULL,
    run_id   TEXT NOT NULL,
    sent_at  {TS} NOT NULL,
    PRIMARY KEY (target, run_id)
);
