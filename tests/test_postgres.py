"""PostgreSQL-only checks. Skipped unless TEST_POSTGRES_DSN points to an empty test database."""

import os
from datetime import datetime

import pytest

from src.run_daily import run

DSN = os.environ.get("TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_POSTGRES_DSN not set")


def pg(sql):
    import psycopg
    with psycopg.connect(DSN) as conn:
        return conn.execute(sql).fetchall()


def test_postgres_has_only_two_tables_with_media_embedded(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100", attachments={"media_keys": ["3_100"]})
    fake_client.add_post("1", "101")
    media = {"media_key": "3_100", "type": "photo", "url": "https://img/1.jpg"}
    original_get = fake_client.get

    def get_with_media(path, params=None):
        body = original_get(path, params)
        if path.endswith("/tweets") and body.get("data"):
            body["includes"] = {"media": [media]}
        return body

    fake_client.get = get_with_media
    settings = make_settings(["alice"])
    assert run(settings, client=fake_client) == 0

    tables = {r[0] for r in pg("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")}
    assert tables == {"user_profiles", "posts"}
    assert pg("SELECT post_id, media_count, media FROM posts ORDER BY post_id") == [
        (100, 1, [{"media_key": "3_100", "type": "photo", "url": "https://img/1.jpg"}]),
        (101, 0, []),
    ]
    assert pg("SELECT user_id, username, status, changes FROM user_profiles") == [("1", "alice", "ok", [])]


def test_postgres_cursor_is_max_post_id(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.add_post("1", "101")
    fake_client.calls.clear()
    run(settings, client=fake_client)
    assert fake_client.timeline_calls()[0]["since_id"] == "100"
    assert pg("SELECT COUNT(*) FROM posts") == [(2,)]


def test_postgres_profile_changes_are_appended(fake_client, make_settings):
    fake_client.add_user("1", "alice", description="old bio")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["description"] = "new bio"
    run(settings, client=fake_client, force_profile_refresh=True)

    [(description, changes, last_changed_at)] = pg(
        "SELECT description, changes, last_changed_at FROM user_profiles WHERE user_id = '1'")
    assert description == "new bio" and last_changed_at is not None
    assert [(c["field"], c["old"], c["new"]) for c in changes] == [("description", "old bio", "new bio")]


def test_postgres_posts_have_post_url(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)
    assert pg("SELECT post_id, post_url FROM posts") == [(100, "https://x.com/alice/status/100")]


def test_postgres_stores_referenced_fields_and_cursor_ignores_them(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_referenced("5000", "cut…", note_tweet={"text": "the full original"})
    fake_client.add_post("1", "100", "RT @c: cut…", referenced_tweets=[{"type": "retweeted", "id": "5000"}])
    settings = make_settings(["alice"])
    run(settings, client=fake_client)
    [(post_id, full, author, created)] = pg("SELECT post_id, referenced_full_text, referenced_author_id, "
                                            "referenced_created_at FROM posts")
    assert (post_id, full, author, created) == (100, "the full original", "900",
                                               datetime.fromisoformat("2026-10-03T08:00:00+00:00"))
    fake_client.calls.clear()
    run(settings, client=fake_client)
    assert fake_client.timeline_calls()[0]["since_id"] == "100"          # MAX(post_id) of own posts only


def serve_media(fake_client, media_by_key):
    """Make the fake API return includes.media for the posts that list these media keys."""
    original_get = fake_client.get

    def get_with_media(path, params=None):
        body = original_get(path, params)
        keys = {k for p in body.get("data") or [] if isinstance(p, dict)
                for k in (p.get("attachments") or {}).get("media_keys") or []}
        if path.endswith("/tweets") and keys:
            body.setdefault("includes", {})["media"] = [media_by_key[k] for k in keys if k in media_by_key]
        return body

    fake_client.get = get_with_media


def test_postgres_outage_is_filled_from_sqlite_without_refetching(fake_client, make_settings, caplog):
    import logging
    import sqlite3

    fake_client.add_user("1", "alice", description="old bio")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)
    # Real runs are at least an hour apart; these test runs can fall in the same second. Make the
    # profile PostgreSQL holds really older than the refresh it is about to miss.
    import psycopg
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("UPDATE user_profiles SET refreshed_at = refreshed_at - interval '1 hour', "
                     "first_seen_at = first_seen_at - interval '1 hour'")

    # PostgreSQL unreachable for two runs: new posts, a profile change and a new account meanwhile
    settings.outputs["postgres"] = False
    serve_media(fake_client, {"3_101": {"media_key": "3_101", "type": "photo", "url": "https://img/101.jpg"}})
    fake_client.add_referenced("900", "cut…", note_tweet={"text": "full original"})
    fake_client.add_post("1", "101", "RT @c: cut…", attachments={"media_keys": ["3_101"]},
                         referenced_tweets=[{"type": "retweeted", "id": "900"}])
    run(settings, client=fake_client)
    fake_client.users["1"]["description"] = "new bio"
    fake_client.add_user("2", "bob")
    fake_client.add_post("2", "200")
    fake_client.add_post("1", "102")
    settings.usernames = ["alice", "bob"]
    run(settings, client=fake_client, force_profile_refresh=True)

    # Back: everything missed is copied from SQLite, and nothing is fetched from X again
    settings.outputs["postgres"] = True
    fake_client.calls.clear()
    with caplog.at_level(logging.INFO):
        assert run(settings, client=fake_client) == 0
    assert any("copied 3 missing post(s) and 2 profile(s) from SQLite" in r.getMessage() for r in caplog.records)
    calls = {path: params for path, params in fake_client.calls if path.endswith("/tweets")}
    assert calls["/users/1/tweets"]["since_id"] == "102" and calls["/users/2/tweets"]["since_id"] == "200"
    conn = sqlite3.connect(settings.sqlite_path)
    assert conn.execute("SELECT posts_new FROM runs ORDER BY rowid DESC LIMIT 1").fetchone() == (0,)
    sqlite_first_seen = conn.execute("SELECT first_seen_at FROM posts WHERE post_id = '101'").fetchone()[0]
    conn.close()

    assert pg("SELECT post_id, author_id FROM posts ORDER BY post_id") == [
        (100, "1"), (101, "1"), (102, "1"), (200, "2")]
    [(full, media, media_count, first_seen)] = pg(
        "SELECT referenced_full_text, media, media_count, first_seen_at FROM posts WHERE post_id = 101")
    assert full == "full original" and media_count == 1
    assert media == [{"media_key": "3_101", "type": "photo", "url": "https://img/101.jpg"}]
    assert first_seen == datetime.fromisoformat(sqlite_first_seen)         # when it was really first seen
    [(description, changes)] = pg("SELECT description, changes FROM user_profiles WHERE user_id = '1'")
    assert description == "new bio"
    assert [(c["field"], c["old"], c["new"]) for c in changes] == [("description", "old bio", "new bio")]
    assert pg("SELECT username FROM user_profiles WHERE user_id = '2'") == [("bob",)]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        run(settings, client=fake_client)
    assert not any("copied" in r.getMessage() for r in caplog.records)   # nothing missing any more


def test_postgres_copied_row_equals_a_direct_save_and_existing_rows_are_untouched(fake_client, make_settings):
    import psycopg

    serve_media(fake_client, {"3_b": {"media_key": "3_b", "type": "video", "preview_image_url": "https://img/b.jpg",
                                      "duration_ms": 900, "variants": [{"url": "https://v/b.mp4"}]},
                              "3_a": {"media_key": "3_a", "type": "photo", "url": "https://img/a.jpg"}})
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100", "first")
    fake_client.add_post("1", "101", "two media\n\nb first", attachments={"media_keys": ["3_b", "3_a"]},
                         possibly_sensitive=True, entities={"hashtags": [{"tag": "x"}]})
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    every_column = "SELECT * FROM posts WHERE post_id = 101"
    direct = pg(every_column)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DELETE FROM posts WHERE post_id = 101")               # as if PostgreSQL missed it
        conn.execute("UPDATE posts SET text = 'changed in PostgreSQL' WHERE post_id = 100")
    fake_client.calls.clear()
    run(settings, client=fake_client)

    assert fake_client.timeline_calls()[0]["since_id"] == "101"            # restored by the copy, not refetched
    assert pg(every_column) == direct                                      # identical, column for column
    assert pg("SELECT text FROM posts WHERE post_id = 100") == [("changed in PostgreSQL",)]   # never overwritten


def test_postgres_old_posts_table_gets_post_url_filled(make_settings):
    import psycopg

    from src.storage import PostgresStore
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS user_profiles, posts")
        conn.execute("CREATE TABLE posts (post_id BIGINT PRIMARY KEY, author_id TEXT NOT NULL, username TEXT, "
                     "created_at TIMESTAMPTZ, first_seen_at TIMESTAMPTZ NOT NULL, media JSONB NOT NULL, "
                     "media_count INTEGER NOT NULL)")
        conn.execute("INSERT INTO posts VALUES (7, '1', 'alice', now(), now(), '[]', 0)")
    store = PostgresStore(DSN)
    store.open()
    store.close()
    assert pg("SELECT post_id, post_url FROM posts") == [(7, "https://x.com/alice/status/7")]
