"""The pipeline output against a real PostgreSQL: copies of shaktidb's twitter_posts / twitter_profiles /
twitter_pipeline_runs (same columns, constraints, generated text_search and text-cleaning trigger; embedding
is jsonb because the test server has no pgvector). Skipped unless TEST_POSTGRES_DSN points to a test database."""

import os
from datetime import datetime

import pytest

from src import run_daily
from src.run_daily import run

DSN = os.environ.get("TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_POSTGRES_DSN not set")

SCHEMA = r"""
DROP TABLE IF EXISTS twitter_posts, twitter_profiles, twitter_pipeline_runs;
CREATE OR REPLACE FUNCTION clean_text_content(input text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $f$
DECLARE t text := input;
BEGIN
    IF t IS NULL THEN RETURN NULL; END IF;
    t := replace(t, '&amp;', '&'); t := replace(t, '&lt;', '<'); t := replace(t, '&gt;', '>');
    t := replace(t, '&quot;', '"'); t := replace(t, '&#39;', ''''); t := replace(t, '&apos;', '''');
    t := replace(t, '&nbsp;', ' ');
    t := regexp_replace(t, '<a\s+[^>]*href\s*=\s*["'']([^"'']*)["''][^>]*>(.*?)</a>', '\2 (\1)', 'gi');
    t := regexp_replace(t, '<[^>]+>', '', 'g');
    t := regexp_replace(t, '\*\*+', '', 'g'); t := replace(t, '*', '');
    t := regexp_replace(t, '(?<![\w])_([^_\n]+)_(?![\w])', '\1', 'g');
    t := regexp_replace(t, '^#{1,6}\s+', '', 'gn');
    t := regexp_replace(t, '[ \t]{2,}', ' ', 'g');
    t := btrim(t);
    t := regexp_replace(t, '^[-‐‑‒–—―]+\s*', ''); t := btrim(t);
    IF t = '' THEN RETURN NULL; END IF;
    RETURN t;
EXCEPTION WHEN OTHERS THEN RETURN input;
END; $f$;
CREATE OR REPLACE FUNCTION trg_clean_twitter_posts() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN NEW.text := clean_text_content(NEW.text); RETURN NEW; END; $f$;
CREATE OR REPLACE FUNCTION trg_clean_twitter_profiles() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN NEW.bio := clean_text_content(NEW.bio); RETURN NEW; END; $f$;

CREATE TABLE twitter_posts (
    id bigserial PRIMARY KEY, tweet_id text NOT NULL UNIQUE, post_url text, keyword text, content_type text,
    author_id text, author_username text, author_name text, author_url text, author_profile_picture text,
    is_verified boolean, text text, language text, hashtags jsonb DEFAULT '[]'::jsonb,
    mentions jsonb DEFAULT '[]'::jsonb, external_links jsonb DEFAULT '[]'::jsonb, attachments jsonb,
    is_video boolean, like_count integer, retweet_count integer, reply_count integer, impression_count integer,
    bookmark_count integer, quote_count integer, is_reply boolean, parent_tweet_id text, is_ad boolean,
    posted_at timestamptz, scraped_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now(),
    embedding jsonb, text_search tsvector GENERATED ALWAYS AS (to_tsvector('english', COALESCE(text, ''))) STORED,
    entities jsonb, translated_text text, detected_language text, updated_at timestamptz NOT NULL DEFAULT now(),
    referenced_tweet_id text, referenced_full_text text, referenced_author_id text, conversation_id text,
    raw_json jsonb);
CREATE TRIGGER clean_before_write BEFORE INSERT OR UPDATE ON twitter_posts
    FOR EACH ROW EXECUTE FUNCTION trg_clean_twitter_posts();

CREATE TABLE twitter_profiles (
    id bigserial PRIMARY KEY, username text NOT NULL UNIQUE, display_name text, verified boolean,
    followers_count integer, following_count integer, bio text, location text, join_date text,
    last_updated timestamptz, ingested_at timestamptz NOT NULL DEFAULT now(),
    user_id text UNIQUE, profile_image_url text, status text, last_scraped_at timestamptz, posts_last_run integer);
CREATE TRIGGER clean_before_write BEFORE INSERT OR UPDATE ON twitter_profiles
    FOR EACH ROW EXECUTE FUNCTION trg_clean_twitter_profiles();

CREATE TABLE twitter_pipeline_runs (
    id bigserial PRIMARY KEY, run_id text NOT NULL UNIQUE, pipeline_name text NOT NULL, item_type text,
    started_at timestamptz NOT NULL, finished_at timestamptz, duration_seconds numeric, status text,
    items_loaded integer, items_processed integer, rows_collected integer, rows_inserted integer,
    duplicates_skipped integer, warnings integer, errors integer, inserted_at timestamptz NOT NULL DEFAULT now(),
    error_messages jsonb, warning_messages jsonb, credits_used numeric, credits_balance numeric);
"""


def pg(sql, params=None):
    import psycopg
    with psycopg.connect(DSN, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if cur.description else None


@pytest.fixture
def tables():
    import psycopg
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA)
    yield
    with psycopg.connect(DSN, autocommit=True) as conn:     # leave the test database as it was
        conn.execute("DROP TABLE IF EXISTS twitter_posts, twitter_profiles, twitter_pipeline_runs")


@pytest.fixture
def settings_with_pipeline(make_settings, tables):
    from psycopg.conninfo import conninfo_to_dict
    info = conninfo_to_dict(DSN)

    def _make(names):
        settings = make_settings(names)
        settings.outputs["postgres"] = False           # only SQLite + JSON + the pipeline database
        settings.outputs["pipeline"] = True
        settings.pipeline_db = {"host": info.get("host", "localhost"), "port": str(info.get("port", 5432)),
                                "dbname": info["dbname"], "user": info.get("user"), "password": info.get("password")}
        return settings
    return _make


def test_rows_land_in_the_three_tables_in_the_agreed_formats(fake_client, settings_with_pipeline):
    fake_client.add_user("1", "Alice", description="Analyst &amp; writer", created_at="2013-06-28T20:09:40.000Z",
                         verified=True, profile_image_url="https://pbs/a.jpg")
    settings = settings_with_pipeline(["alice", "ghost"])
    assert run(settings, client=fake_client) == 0                  # switch-on: profiles, no posts loaded

    fake_client.add_referenced("900", "cut…", note_tweet={"text": "the complete **original**"})
    fake_client.add_post("1", "100", "Tom &amp; Jerry *bold*", lang="en",
                         entities={"hashtags": [{"tag": "Navy"}], "mentions": [{"username": "bob"}],
                                   "urls": [{"url": "https://t.co/x", "expanded_url": "https://news.example/a"}]})
    fake_client.add_post("1", "101", "RT @carol: cut…", referenced_tweets=[{"type": "retweeted", "id": "900"}])
    assert run(settings, client=fake_client) == 0

    rows = pg("SELECT tweet_id, keyword, content_type, author_name, is_verified, text, hashtags, mentions, "
              "external_links, attachments, is_ad, referenced_tweet_id, referenced_full_text, text_search IS NOT NULL, "
              "embedding, entities, translated_text, detected_language FROM twitter_posts ORDER BY tweet_id")
    assert rows == [
        ("100", "alice", "tweet", "Alice", True, "Tom & Jerry bold", ["Navy"], ["bob"], ["https://news.example/a"], [],
         False, None, None, True, None, None, None, None),
        ("101", "alice", "retweet", "Alice", True, "RT @carol: the complete original", [], [], [], [], False, "900",
         "the complete **original**", True, None, None, None, None),
    ]                                                              # text cleaned by the database's trigger
    [(posted, scraped, raw_id)] = pg("SELECT posted_at, scraped_at, raw_json->>'id' FROM twitter_posts "
                                      "WHERE tweet_id = '100'")
    assert posted == datetime.fromisoformat("2026-10-04T10:00:00+00:00") and scraped is not None and raw_id == "100"

    assert pg("SELECT username, display_name, verified, bio, join_date, user_id, profile_image_url, status "
              "FROM twitter_profiles") == [
        ("Alice", "Alice", True, "Analyst & writer", "Fri Jun 28 20:09:40 +0000 2013", "1", "https://pbs/a.jpg", "ok")]
    import sqlite3
    conn = sqlite3.connect(settings.sqlite_path)
    scraped_at = conn.execute("SELECT last_scraped_at FROM monitored_users WHERE user_id = '1'").fetchone()[0]
    conn.close()
    assert pg("SELECT last_scraped_at, posts_last_run FROM twitter_profiles") == [
        (datetime.fromisoformat(scraped_at), 2)]                   # own posts of the last run (not the original)

    runs = pg("SELECT pipeline_name, item_type, status, items_loaded, rows_collected, rows_inserted, "
              "duplicates_skipped, warnings, errors, warning_messages, error_messages, duration_seconds IS NOT NULL "
              "FROM twitter_pipeline_runs ORDER BY id")
    assert [r[:9] for r in runs] == [("twitter", "posts", "success", 1, 0, 0, 0, 1, 0),   # @ghost each run
                                     ("twitter", "posts", "success", 1, 2, 2, 0, 1, 0)]
    assert [(m["account"], m["message"]) for m in runs[0][9]] == [
        ("ghost", "@ghost: not found (new user, nothing stored)")]
    assert runs[0][10] == [] and runs[0][11] is True

    assert run(settings, client=fake_client) == 0                  # nothing new: nothing added twice
    assert pg("SELECT COUNT(*) FROM twitter_posts") == [(2,)] and pg("SELECT COUNT(*) FROM twitter_profiles") == [(1,)]
    assert pg("SELECT COUNT(*) FROM twitter_pipeline_runs") == [(3,)]


def test_existing_rows_are_kept_and_profiles_matched_like_agreed(fake_client, settings_with_pipeline):
    # Rows the previous pipeline wrote: a tweet we will collect again, and profiles without user_id.
    pg("INSERT INTO twitter_posts (tweet_id, text, keyword) VALUES ('100', 'old pipeline text', 'old')")
    pg("INSERT INTO twitter_profiles (username, display_name) VALUES ('ALICE', 'old row'), ('alice', 'older dup'), "
       "('bob', 'someone else')")
    pg("UPDATE twitter_profiles SET user_id = '999' WHERE username = 'bob'")    # 'bob' is another account

    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    settings = settings_with_pipeline(["alice", "bob"])
    run(settings, client=fake_client)
    fake_client.add_post("1", "100", "our text")
    fake_client.add_post("1", "102", "new")
    run(settings, client=fake_client)

    assert pg("SELECT tweet_id, text, keyword FROM twitter_posts ORDER BY tweet_id") == [
        ("100", "old pipeline text", "old"), ("102", "new", "alice")]        # existing row never changed
    assert pg("SELECT rows_inserted, duplicates_skipped FROM twitter_pipeline_runs ORDER BY id DESC "
              "LIMIT 1") == [(1, 1)]
    profiles = pg("SELECT username, display_name, user_id FROM twitter_profiles ORDER BY id")
    assert ("alice", "Alice", "1") in profiles                              # old row updated, matched ignoring case
    assert not any(p[1] in ("old row", "older dup") for p in profiles)     # its stale duplicate removed
    assert ("bob", "someone else", "999") in profiles                       # another account's row untouched
    warnings = pg("SELECT warning_messages FROM twitter_pipeline_runs ORDER BY id LIMIT 1")[0][0]
    assert any("@bob not updated in twitter_profiles" in w["message"] for w in warnings)

    first_id = pg("SELECT id FROM twitter_profiles WHERE user_id = '1'")[0][0]
    fake_client.users["1"]["username"] = "alice_new"                         # renamed on X
    run(settings, client=fake_client, force_profile_refresh=True)
    assert pg("SELECT id, username, status FROM twitter_profiles WHERE user_id = '1'") == [
        (first_id, "alice_new", "renamed")]                                  # same row, renamed in place


def test_missing_columns_stop_delivery_until_added(fake_client, settings_with_pipeline):
    pg("ALTER TABLE twitter_posts DROP COLUMN referenced_full_text")
    fake_client.add_user("1", "alice")
    settings = settings_with_pipeline(["alice"])
    assert run(settings, client=fake_client) == 1                            # reported as an error
    assert pg("SELECT COUNT(*) FROM twitter_profiles") == [(0,)]             # nothing half-written
    assert run_daily.check(settings) == 1                                    # --check reports it too

    pg("ALTER TABLE twitter_posts ADD COLUMN referenced_full_text text")
    fake_client.add_post("1", "100")
    assert run(settings, client=fake_client) == 0                            # everything delivered now
    assert pg("SELECT COUNT(*) FROM twitter_profiles") == [(1,)] and pg("SELECT COUNT(*) FROM twitter_posts") == [(1,)]
    assert pg("SELECT COUNT(*) FROM twitter_pipeline_runs") == [(2,)]        # both runs' summaries
