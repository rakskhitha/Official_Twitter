"""The tweet each post refers to (retweet original, reply parent, quoted tweet), requested with the
referenced_tweets.id expansion and stored as referenced_* fields on the post itself. It must never
change what is collected: cursor, gaps and the posts stored are the account's own posts only."""

import json
import logging
import sqlite3
from datetime import datetime, timezone

from src.config import DEFAULT_EXPANSIONS
from src.models import normalize_post, utc_now
from src.run_daily import run
from src.storage import SQLiteStore

FULL = "the complete original text, longer than 280 characters " + "x" * 300


def sqlite_rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def json_lines(settings):
    lines = (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
    return {r["post_id"]: r for r in map(json.loads, lines)}


def test_expansion_is_requested_by_default_and_media_still_is():
    assert DEFAULT_EXPANSIONS == ["attachments.media_keys", "referenced_tweets.id"]


def test_normalize_post_fills_referenced_fields():
    referenced = {"5000": {"id": "5000", "text": "cut…", "note_tweet": {"text": FULL}, "author_id": "900",
                           "created_at": "2026-10-03T08:00:00.000Z"}}
    raw = {"id": "1", "text": "RT @carol: cut…", "referenced_tweets": [{"type": "retweeted", "id": "5000"}]}
    post = normalize_post(raw, {}, utc_now(), "alice", referenced)
    assert post["text"] == "RT @carol: cut…"                              # own text untouched
    assert post["referenced_full_text"] == FULL                            # long-form text, not the cut one
    assert post["referenced_author_id"] == "900"
    assert post["referenced_created_at"] == datetime(2026, 10, 3, 8, tzinfo=timezone.utc)

    deleted = normalize_post({**raw, "referenced_tweets": [{"type": "retweeted", "id": "404"}]},
                             {}, utc_now(), "alice", referenced)
    original = normalize_post({"id": "2", "text": "plain"}, {}, utc_now(), "alice", referenced)
    old_call = normalize_post(raw, {}, utc_now(), "alice")                 # without includes
    for post in (deleted, original, old_call):
        assert (post["referenced_full_text"], post["referenced_author_id"], post["referenced_created_at"]) == (
            None, None, None)


def test_run_stores_referenced_tweets_without_moving_the_cursor(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    # Referenced ids are far ABOVE alice's own ids: they must not become her cursor.
    fake_client.add_referenced("5000", "cut…", note_tweet={"text": FULL})
    fake_client.add_referenced("5001", "parent tweet", author_id="901")
    fake_client.add_referenced("5002", "quoted tweet", author_id="1")      # alice quoting herself
    fake_client.add_post("1", "100", "RT @carol: cut…", referenced_tweets=[{"type": "retweeted", "id": "5000"}])
    fake_client.add_post("1", "101", "a reply", referenced_tweets=[{"type": "replied_to", "id": "5001"}])
    fake_client.add_post("1", "102", "a quote", referenced_tweets=[{"type": "quoted", "id": "5002"}])
    fake_client.add_post("1", "103", "RT @dan: gone…", referenced_tweets=[{"type": "retweeted", "id": "4040"}])
    fake_client.add_post("1", "104", "plain")
    settings = make_settings(["alice"])

    assert run(settings, client=fake_client) == 0                          # a deleted original is not an error
    assert fake_client.timeline_calls()[0]["expansions"] == ["attachments.media_keys", "referenced_tweets.id"]

    expected = {
        "100": (FULL, "900", "2026-10-03T08:00:00+00:00"),
        "101": ("parent tweet", "901", "2026-10-03T08:00:00+00:00"),
        "102": ("quoted tweet", "1", "2026-10-03T08:00:00+00:00"),
        "103": (None, None, None),                                          # deleted: left empty
        "104": (None, None, None),                                          # refers to nothing
    }
    rows = sqlite_rows(settings, "SELECT post_id, referenced_full_text, referenced_author_id, "
                                 "referenced_created_at FROM posts ORDER BY post_id")
    assert {r[0]: r[1:] for r in rows} == expected                         # only alice's own 5 posts stored
    lines = json_lines(settings)
    assert {pid: (r["referenced_full_text"], r["referenced_author_id"], r["referenced_created_at"])
            for pid, r in lines.items()} == expected
    assert lines["100"]["text"] == "RT @carol: cut…"

    # Cursor = alice's newest OWN post in every output, never a referenced id
    assert sqlite_rows(settings, "SELECT since_id FROM monitored_users") == [("104",)]
    state = json.loads((settings.json_dir / "_state.json").read_text(encoding="utf-8"))
    assert state["cursors"]["1"] == "104"

    # Next run asks from there and stores nothing twice
    fake_client.calls.clear()
    assert run(settings, client=fake_client) == 0
    assert fake_client.timeline_calls()[0]["since_id"] == "104"
    assert sqlite_rows(settings, "SELECT COUNT(*) FROM posts") == [(5,)] and len(json_lines(settings)) == 5


def test_cost_counts_referenced_tweets_but_posts_new_does_not(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_referenced("5000", "original")
    fake_client.add_post("1", "100", "RT…", referenced_tweets=[{"type": "retweeted", "id": "5000"}])
    fake_client.add_post("1", "101", "RT…", referenced_tweets=[{"type": "retweeted", "id": "4040"}])  # deleted
    settings = make_settings(["alice"])
    assert run(settings, client=fake_client) == 0

    [(posts_new, post_reads, referenced_reads, est, used)] = sqlite_rows(
        settings, "SELECT posts_new, post_reads, referenced_reads, est_cost_usd, credits_used FROM runs")
    assert (posts_new, post_reads, referenced_reads) == (2, 3, 1)          # deleted original: not returned, not billed
    assert est == round(1 * 0.010 + 3 * 0.005, 4)                          # 1 user + 3 posts
    assert used == est                                                     # matches what the fake X charged
    summary = json.loads((settings.json_dir / "_runs.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert (summary["posts_new"], summary["post_reads"], summary["referenced_reads"]) == (2, 3, 1)


def test_unavailable_originals_do_not_flood_the_log(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    for i in range(5):
        fake_client.add_post("1", str(100 + i), "RT…", referenced_tweets=[{"type": "retweeted", "id": str(4000 + i)}])
    settings = make_settings(["alice"])
    with caplog.at_level(logging.INFO):
        assert run(settings, client=fake_client) == 0
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    summaries = [r.getMessage() for r in caplog.records if "referenced tweet(s) not available" in r.getMessage()]
    assert len(summaries) == 1 and "5 referenced tweet(s)" in summaries[0]


def test_other_partial_errors_are_still_warnings(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    original_get = fake_client.get

    def get_with_other_error(path, params=None):
        body = original_get(path, params)
        if path.endswith("/tweets"):
            body["errors"] = [{"detail": "something else", "parameter": "id"}]
        return body

    fake_client.get = get_with_other_error
    with caplog.at_level(logging.WARNING):
        run(make_settings(["alice"]), client=fake_client)
    assert any("Partial error" in r.getMessage() and "something else" in r.getMessage() for r in caplog.records)


def test_gap_fill_also_stores_referenced_tweets(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "1000")
    fake_client.page_size = 2
    settings = make_settings(["alice"], max_pages_per_user_per_run=2)
    assert run(settings, client=fake_client) == 0

    fake_client.add_referenced("9000", "full original")
    fake_client.add_post("1", "1001", "RT…", referenced_tweets=[{"type": "retweeted", "id": "9000"}])
    for i in range(1002, 1007):
        fake_client.add_post("1", str(i))
    run(settings, client=fake_client)        # gets 1006..1003, page cap: gap 1000-1003 recorded
    assert "1001" not in json_lines(settings)
    run(settings, client=fake_client)        # fills the gap: 1002, 1001
    assert sqlite_rows(settings, "SELECT referenced_full_text FROM posts WHERE post_id = '1001'") == [
        ("full original",)]
    assert json_lines(settings)["1001"]["referenced_full_text"] == "full original"
    assert sqlite_rows(settings, "SELECT since_id FROM monitored_users") == [("1006",)]


def test_old_sqlite_database_gets_the_new_columns(tmp_path):
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE posts (post_id TEXT PRIMARY KEY, author_id TEXT, username TEXT, created_at TEXT, "
                 "text TEXT, post_url TEXT, first_seen_at TEXT NOT NULL)")
    conn.execute("INSERT INTO posts VALUES ('7', '1', 'alice', '2026-10-07T00:00:00+00:00', 'RT…', 'u', "
                 "'2026-10-07T00:00:00+00:00')")
    conn.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, post_reads INTEGER)")
    conn.commit()
    conn.close()

    store = SQLiteStore(db)
    store.open()
    store.close()
    conn = sqlite3.connect(db)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(posts)")}
    assert {"referenced_full_text", "referenced_author_id", "referenced_created_at"} <= cols
    assert "referenced_reads" in {r[1] for r in conn.execute("PRAGMA table_info(runs)")}
    assert conn.execute("SELECT post_id, text, referenced_full_text FROM posts").fetchall() == [
        ("7", "RT…", None)]                                                # existing row kept, new field empty
    conn.close()
