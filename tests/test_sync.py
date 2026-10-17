"""Filling PostgreSQL in from SQLite (src/storage/sync.py): the parts that need no PostgreSQL server.
The end-to-end checks against a real PostgreSQL are in tests/test_postgres.py."""

import logging
from datetime import datetime, timedelta, timezone

from src.models import normalize_post, normalize_user
from src.problems import RunProblems
from src.run_daily import fill_postgres_from_sqlite
from src.storage import PostgresStore, SQLiteStore
from src.storage.sql_store import POST_COLS, merge_changes, profile_freshness

T = datetime(2026, 10, 8, 6, 0, tzinfo=timezone.utc)

# Every field PostgresStore._post_row reads from a post, apart from first_seen_at (checked separately).
POST_FIELDS = [c for c in POST_COLS if c not in ("raw_json", "referenced_json", "first_seen_at")] + [
    "media", "referenced", "raw"]


def sample_posts():
    media_by_key = {
        "3_b": {"media_key": "3_b", "type": "video", "preview_image_url": "https://img/b.jpg", "duration_ms": 1200,
                "width": 640, "height": 360, "variants": [{"bit_rate": 256000, "url": "https://v/b.mp4"}]},
        "3_a": {"media_key": "3_a", "type": "photo", "url": "https://img/a.jpg", "alt_text": "café ✓"},
    }
    raws = [
        {"id": "101", "text": "two media, b listed first", "author_id": "1", "created_at": "2026-10-08T05:00:00.000Z",
         "possibly_sensitive": True, "attachments": {"media_keys": ["3_b", "3_a"]},
         "public_metrics": {"retweet_count": 2, "like_count": 5}, "entities": {"hashtags": [{"tag": "x"}]}},
        {"id": "102", "text": "RT @c: cut…", "author_id": "1", "created_at": "2026-10-08T05:30:00.000Z",
         "possibly_sensitive": False, "referenced_tweets": [{"type": "retweeted", "id": "900"}]},
        {"id": "103", "text": "short…", "note_tweet": {"text": "the long\n\nform text"}, "author_id": "1"},
    ]
    referenced = {"900": {"id": "900", "text": "cut…", "note_tweet": {"text": "full original"}, "author_id": "7",
                          "created_at": "2026-10-07T23:00:00.000Z"}}
    return [normalize_post(r, media_by_key, T, "alice", referenced) for r in raws]


def test_export_posts_gives_back_exactly_what_was_saved(tmp_path):
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    posts = sample_posts()
    store.save_posts("1", "alice", "alice", posts, "103", T)

    exported = {p["post_id"]: p for p in store.export_posts(["103", "101", "102", "999"])}
    assert sorted(exported) == ["101", "102", "103"]                       # unknown ids are just absent
    for post in posts:
        copy = exported[post["post_id"]]
        assert {f: copy[f] for f in POST_FIELDS} == {f: post[f] for f in POST_FIELDS}, post["post_id"]
        assert copy["first_seen_at"] == T
    assert [m["media_key"] for m in exported["101"]["media"]] == ["3_b", "3_a"]   # the post's own order
    assert exported["101"]["possibly_sensitive"] is True and exported["102"]["possibly_sensitive"] is False
    assert exported["102"]["referenced_full_text"] == "full original"
    assert store.post_ids() == {"101", "102", "103"}
    store.close()


def test_export_profiles_decodes_every_value(tmp_path):
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    raw = {"id": "1", "username": "alice", "name": "Alice", "verified": True, "protected": False,
           "created_at": "2010-04-19T10:50:15.000Z", "public_metrics": {"followers_count": 10}}
    store.save_profile(normalize_user(raw), "alice", "ok", T)
    store.set_status("1", "suspended", T + timedelta(hours=1))

    [profile] = store.export_profiles()
    user = normalize_user(raw)
    assert {k: profile[k] for k in ("username", "name", "created_at", "verified", "protected", "followers_count")} == {
        k: user[k] for k in ("username", "name", "created_at", "verified", "protected", "followers_count")}
    assert profile["verified"] is True and profile["protected"] is False
    assert profile["raw"] == raw and profile["status"] == "suspended"
    assert profile["first_seen_at"] == profile["refreshed_at"] == T
    assert profile["last_changed_at"] == T + timedelta(hours=1)
    assert [(c["field"], c["old"], c["new"]) for c in profile["changes"]] == [("status", "ok", "suspended")]
    store.close()


def test_profile_freshness_counts_status_changes():
    assert profile_freshness(T, None) == T
    assert profile_freshness(T, T + timedelta(hours=2)) == T + timedelta(hours=2)
    assert profile_freshness(None, None) is None


def test_merge_changes_keeps_every_entry_once_oldest_first():
    a = {"changed_at": "2026-10-01T00:00:00+00:00", "field": "name", "old": "A", "new": "B"}
    b = {"changed_at": "2026-10-03T00:00:00+00:00", "field": "status", "old": "ok", "new": "suspended"}
    c = {"changed_at": "2026-10-02T00:00:00+00:00", "field": "description", "old": None, "new": "bio"}
    same_moment_as_a = {**a, "changed_at": "2026-10-01T05:30:00+05:30"}     # same change, other notation
    assert merge_changes([a, b], [same_moment_as_a, c]) == [a, c, b]
    assert merge_changes([], []) == [] and merge_changes([b], None) == [b]


def test_nothing_happens_without_both_databases(tmp_path):
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    problems = RunProblems().attach()
    try:
        fill_postgres_from_sqlite([store])                                  # PostgreSQL not open this run
    finally:
        problems.detach()
    assert problems.errors == [] and problems.warnings == []
    store.close()


class BrokenPostgres(PostgresStore):
    """Stands in for a PostgreSQL connection that fails during the copy."""

    def __init__(self):
        super().__init__("unused")

    def post_ids(self):
        raise ConnectionError("server closed the connection unexpectedly")


def test_a_failed_copy_is_reported_and_never_stops_the_run(tmp_path, caplog):
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    store.save_posts("1", "alice", "alice", sample_posts(), "103", T)
    problems = RunProblems().attach()
    try:
        with caplog.at_level(logging.ERROR):
            fill_postgres_from_sqlite([store, BrokenPostgres()])           # must not raise
    finally:
        problems.detach()
    [error] = [e["message"] for e in problems.errors]
    assert "Could not fill in PostgreSQL" in error and "connection" in error and "fetched again" in error
    store.close()
