-- PostgreSQL schema: two tables only.
-- Per-user cursor = MAX(post_id) per author. Last profile refresh = user_profiles.refreshed_at.
-- The run log and the config-name links live in the SQLite and JSON outputs.

-- One row per account: always the latest profile. Changes to tracked fields
-- (see TRACKED_PROFILE_FIELDS in models.py) are appended to the `changes` JSON list.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id           TEXT PRIMARY KEY,
    username          TEXT,
    name              TEXT,
    description       TEXT,
    location          TEXT,
    url               TEXT,
    created_at        TIMESTAMPTZ,
    verified          BOOLEAN,
    verified_type     TEXT,
    protected         BOOLEAN,
    followers_count   BIGINT,
    following_count   BIGINT,
    tweet_count       BIGINT,
    listed_count      BIGINT,
    profile_image_url TEXT,
    pinned_tweet_id   TEXT,
    status            TEXT,
    raw_json          JSONB,
    first_seen_at     TIMESTAMPTZ NOT NULL,
    refreshed_at      TIMESTAMPTZ NOT NULL,
    last_changed_at   TIMESTAMPTZ,
    changes           JSONB NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS posts (
    post_id             BIGINT PRIMARY KEY,
    author_id           TEXT NOT NULL,
    username            TEXT,
    created_at          TIMESTAMPTZ,
    post_url            TEXT,
    text                TEXT,
    full_text           TEXT,
    lang                TEXT,
    conversation_id     TEXT,
    in_reply_to_user_id TEXT,
    post_type           TEXT,
    referenced_post_id  TEXT,
    referenced_json     JSONB,
    referenced_full_text  TEXT,              -- complete text of the tweet referenced_post_id points to
    referenced_author_id  TEXT,              -- (retweet: original, reply: parent, quote: quoted tweet);
    referenced_created_at TIMESTAMPTZ,       -- NULL when X could not return it (deleted, protected)
    possibly_sensitive  BOOLEAN,
    retweet_count       BIGINT,
    reply_count         BIGINT,
    like_count          BIGINT,
    quote_count         BIGINT,
    bookmark_count      BIGINT,
    impression_count    BIGINT,
    media               JSONB NOT NULL,
    media_count         INTEGER NOT NULL,
    metrics_fetched_at  TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    raw_json            JSONB
);

CREATE INDEX IF NOT EXISTS idx_posts_author_created ON posts (author_id, created_at);
