"""Output to the shared pipeline database (src/storage/pipeline.py): field mapping, delivery logic and
whole runs. Uses an in-memory stand-in for the database; tests/test_postgres.py has the real-database
checks."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src import run_daily
from src.models import normalize_post, normalize_user
from src.run_daily import run
from src.storage import SQLiteStore
from src.storage.pipeline import (Delivered, attachment, deliver_posts_and_profiles, deliver_runs, join_date,
                                  post_row, post_text, profile_row, run_row)

T = datetime(2026, 10, 11, 6, 0, tzinfo=timezone.utc)
DB_CONFIG = {"host": "testhost", "port": "5432", "dbname": "testdb", "user": "u", "password": "p"}


# --------------------------------------------------------------------------- in-memory pipeline database
class FakePipelineDB:
    """Behaves like PipelineDB: unique tweet_id / run_id, latest-state profiles."""

    instances: list["FakePipelineDB"] = []
    down = False                       # True: open() fails, like an unreachable server

    def __init__(self, config):
        self.config = config
        self.posts: dict[str, dict] = {}
        self.profiles: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self.opened = 0
        FakePipelineDB.instances.append(self)

    target = property(lambda self: f"{self.config['host']}:{self.config['port']}/{self.config['dbname']}")

    def describe(self):
        return f"{self.config['dbname']} on {self.config['host']}:{self.config['port']}"

    def open(self):
        if FakePipelineDB.down:
            raise ConnectionError("connection timeout expired")
        self.opened += 1

    def close(self):
        pass

    def missing_columns(self):
        return {}

    def insert_posts(self, rows):
        added = 0
        for row in rows:
            if row["tweet_id"] not in self.posts:
                self.posts[row["tweet_id"]] = row
                added += 1
        return added

    def save_profile(self, row, config_name=None):
        outcome = "updated" if row["user_id"] in self.profiles else "inserted"
        self.profiles[row["user_id"]] = row
        return outcome

    def insert_run(self, row):
        if row["run_id"] in self.runs:
            return False
        self.runs[row["run_id"]] = row
        return True


class SharedFake:
    """One fake database that survives across runs (each run builds a new PipelineDB object)."""

    def __init__(self):
        self.db = None

    def __call__(self, config):
        if self.db is None:
            self.db = FakePipelineDB(config)
        return self.db


@pytest.fixture
def pipeline(monkeypatch):
    FakePipelineDB.down = False
    shared = SharedFake()
    monkeypatch.setattr(run_daily, "PipelineDB", shared)
    return shared


def pipeline_settings(make_settings, names, **overrides):
    settings = make_settings(names, **overrides)
    settings.outputs["pipeline"] = True
    settings.pipeline_db = dict(DB_CONFIG)
    return settings


# --------------------------------------------------------------------------- field mapping
def video(key="7_1"):
    return {"media_key": key, "type": "video", "preview_image_url": "https://img/prev.jpg", "variants": [
        {"content_type": "application/x-mpegURL", "url": "https://v/pl.m3u8"},
        {"content_type": "video/mp4", "bit_rate": 256000, "url": "https://v/low.mp4"},
        {"content_type": "video/mp4", "bit_rate": 2176000, "url": "https://v/high.mp4"}]}


def make_post(raw, media=None, referenced=None, username="PakistanNavy"):
    post = normalize_post(raw, media or {}, T, username, referenced)
    post["first_seen_at"] = T
    return post


PROFILE = {"user_id": "734598796441718784", "username": "PakistanNavy", "name": "Pakistan Navy",
           "profile_image_url": "https://pbs.twimg.com/profile_images/1/x_normal.jpg", "verified": True}


def test_original_post_row_matches_the_previous_pipeline_formats():
    raw = {"id": "2000", "author_id": "734598796441718784", "created_at": "2026-10-10T08:00:00.000Z", "lang": "en",
           "text": "short…", "note_tweet": {"text": "the long text #Navy @dgprPaknavy"},
           "conversation_id": "2000", "attachments": {"media_keys": ["3_1", "7_1"]},
           "public_metrics": {"like_count": 5, "retweet_count": 2, "reply_count": 1, "quote_count": 0,
                              "bookmark_count": 3, "impression_count": 900},
           "entities": {"hashtags": [{"tag": "Navy"}, {"tag": "Navy"}], "mentions": [{"username": "dgprPaknavy"}],
                        "urls": [{"url": "https://t.co/a", "expanded_url": "https://news.example/article"},
                                 {"url": "https://t.co/b", "expanded_url": "https://x.com/PakistanNavy/status/2000/photo/1",
                                  "media_key": "3_1"}]}}
    media = {"3_1": {"media_key": "3_1", "type": "photo", "url": "https://pbs.twimg.com/media/a.jpg"}, "7_1": video()}
    row = post_row(make_post(raw, media), PROFILE, "PakistanNavy")
    assert row == {
        "tweet_id": "2000", "post_url": "https://x.com/PakistanNavy/status/2000", "keyword": "PakistanNavy",
        "content_type": "tweet", "author_id": "734598796441718784", "author_username": "PakistanNavy",
        "author_name": "Pakistan Navy", "author_url": "https://x.com/PakistanNavy",
        "author_profile_picture": "https://pbs.twimg.com/profile_images/1/x_normal.jpg", "is_verified": True,
        "text": "the long text #Navy @dgprPaknavy", "language": "en",
        "hashtags": ["Navy"], "mentions": ["dgprPaknavy"], "external_links": ["https://news.example/article"],
        "attachments": [{"url": "https://pbs.twimg.com/media/a.jpg", "type": "photo"},
                        {"url": "https://v/high.mp4", "type": "video"}],
        "is_video": True, "like_count": 5, "retweet_count": 2, "reply_count": 1, "impression_count": 900,
        "bookmark_count": 3, "quote_count": 0, "is_reply": False, "parent_tweet_id": None, "is_ad": False,
        "posted_at": datetime(2026, 10, 10, 8, tzinfo=timezone.utc), "scraped_at": T,
        "referenced_tweet_id": None, "referenced_full_text": None, "referenced_author_id": None,
        "conversation_id": "2000", "raw_json": raw,
    }


def test_retweet_text_is_the_complete_original():
    referenced = {"900": {"id": "900", "text": "cut…", "note_tweet": {"text": "Commemorating Independence Day.\n\n"
                                                                          "#PakistanNavy #14August"},
                          "author_id": "111", "created_at": "2026-08-13T19:00:00.000Z"}}
    raw = {"id": "2001", "text": "RT @dgprPaknavy: Commemorating Independence Day.\n\n#PakistanNavy #14Augus…",
           "referenced_tweets": [{"type": "retweeted", "id": "900"}], "author_id": "734598796441718784"}
    row = post_row(make_post(raw, referenced=referenced), PROFILE, "PakistanNavy")
    assert row["text"] == "RT @dgprPaknavy: Commemorating Independence Day.\n\n#PakistanNavy #14August"
    assert row["content_type"] == "retweet" and row["is_reply"] is False and row["parent_tweet_id"] is None
    assert (row["referenced_tweet_id"], row["referenced_author_id"]) == ("900", "111")
    assert row["referenced_full_text"] == "Commemorating Independence Day.\n\n#PakistanNavy #14August"

    deleted = make_post({**raw, "referenced_tweets": [{"type": "retweeted", "id": "404"}]}, referenced=referenced)
    assert post_text(deleted) == raw["text"]                     # original gone: X's own text kept


def test_reply_and_quote_rows():
    parent = {"500": {"id": "500", "text": "the parent", "author_id": "5"}}
    reply = post_row(make_post({"id": "2002", "text": "@x thanks", "referenced_tweets": [
        {"type": "replied_to", "id": "500"}]}, referenced=parent), None, None)
    assert (reply["content_type"], reply["is_reply"], reply["parent_tweet_id"]) == ("reply", True, "500")
    assert reply["referenced_full_text"] == "the parent" and reply["text"] == "@x thanks"
    assert reply["keyword"] == "PakistanNavy"                    # no config name known: the username
    assert reply["author_name"] is None and reply["is_verified"] is None   # no stored profile

    quote = post_row(make_post({"id": "2003", "text": "my view", "referenced_tweets": [
        {"type": "quoted", "id": "500"}]}, referenced=parent), PROFILE, "PakistanNavy")
    assert (quote["content_type"], quote["is_reply"], quote["parent_tweet_id"]) == ("quote", False, None)
    assert quote["text"] == "my view" and quote["referenced_tweet_id"] == "500"


def test_attachments_and_huge_counts():
    assert attachment({"type": "animated_gif", "variants": [{"content_type": "video/mp4", "url": "https://v/g.mp4"}]}) \
        == {"url": "https://v/g.mp4", "type": "animated_gif"}
    assert attachment({"type": "video", "preview_image_url": "https://img/p.jpg"}) == {"url": "https://img/p.jpg",
                                                                                      "type": "video"}
    assert attachment({"type": "photo"}) is None
    raw = {"id": "2004", "text": "viral", "public_metrics": {"impression_count": 3_000_000_000}}
    assert post_row(make_post(raw), None, None)["impression_count"] == 2_147_483_647   # integer column limit
    assert post_row(make_post({"id": "2005", "text": "plain"}), None, None)["attachments"] == []


def test_profile_row_and_join_date_format():
    raw = {"id": "42", "username": "BBCWorld", "name": "BBC News (World)", "verified": True,
           "description": "News &amp; more", "location": "London", "created_at": "2013-06-28T20:09:40.000Z",
           "profile_image_url": "https://pbs/x.jpg",
           "public_metrics": {"followers_count": 40_000_000, "following_count": 10}}
    profile = {**normalize_user(raw), "refreshed_at": T, "status": "ok",
               "last_scraped_at": T + timedelta(minutes=5), "posts_last_run": 12}
    assert profile_row(profile, T + timedelta(hours=1)) == {
        "username": "BBCWorld", "display_name": "BBC News (World)", "verified": True, "followers_count": 40_000_000,
        "following_count": 10, "bio": "News &amp; more", "location": "London",
        "join_date": "Fri Jun 28 20:09:40 +0000 2013", "last_updated": T, "ingested_at": T + timedelta(hours=1),
        "user_id": "42", "profile_image_url": "https://pbs/x.jpg", "status": "ok",
        "last_scraped_at": T + timedelta(minutes=5), "posts_last_run": 12}
    assert join_date(datetime(2024, 1, 7, 0, 0, 5, tzinfo=timezone.utc)) == "Sun Jan 07 00:00:05 +0000 2024"
    assert join_date(None) is None


def test_run_row():
    warning = {"time": "2026-10-11T06:01:00+00:00", "account": "OsintTV",
               "message": "@OsintTV: not found (new user, nothing stored)"}
    row = run_row({"run_id": "r1", "started_at": T, "finished_at": T + timedelta(seconds=117.5), "status": "success",
                   "items_loaded": 155, "items_processed": 155, "posts_new": 69, "pipeline_inserted": 68,
                   "pipeline_duplicates": 1, "errors": [], "warnings": [warning], "credits_used": 0.34,
                   "credits_after": 308.9})
    assert row == {"run_id": "r1", "pipeline_name": "twitter", "item_type": "posts", "started_at": T,
                   "finished_at": T + timedelta(seconds=117.5), "duration_seconds": 117.5, "status": "success",
                   "items_loaded": 155, "items_processed": 155, "rows_collected": 69, "rows_inserted": 68,
                   "duplicates_skipped": 1, "warnings": 1, "errors": 0, "error_messages": [],
                   "warning_messages": [warning], "credits_used": 0.34, "credits_balance": 308.9}
    old_style = run_row({"run_id": "r0", "errors": ["plain text"], "warnings": []})
    assert old_style["error_messages"] == [{"time": None, "account": None, "message": "plain text"}]
    assert old_style["duration_seconds"] is None


# --------------------------------------------------------------------------- delivery logic
def stored_source(tmp_path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    user = normalize_user({"id": "1", "username": "alice", "name": "Alice", "verified": False,
                           "public_metrics": {"followers_count": 3}})
    store.save_profile(user, "Alice_Config", "ok", T)
    return store


def save(store, ids, at):
    posts = [normalize_post({"id": i, "text": f"post {i}", "author_id": "1"}, {}, at, "alice") for i in ids]
    store.save_posts("1", "alice", "Alice_Config", posts, max(ids, key=int), at)


def test_only_posts_first_seen_after_switch_on_are_delivered_and_never_twice(tmp_path):
    store = stored_source(tmp_path)
    save(store, ["10", "11"], T - timedelta(days=1))              # collected before switch-on: not loaded
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    assert store.pipeline_since(db.target, T + timedelta(days=5)) == since == T   # fixed once recorded

    save(store, ["12", "13"], T + timedelta(minutes=5))
    done = deliver_posts_and_profiles(store, db, since)
    assert sorted(db.posts) == ["12", "13"] and (done.posts_inserted, done.posts_duplicate) == (2, 0)
    assert db.posts["12"]["keyword"] == "Alice_Config" and db.posts["12"]["author_name"] == "Alice"
    assert done.profiles == 1 and db.profiles["1"]["username"] == "alice"

    again = deliver_posts_and_profiles(store, db, since)          # nothing new: nothing sent
    assert (again.posts_inserted, again.posts_duplicate, again.profiles) == (0, 0, 0)

    db.posts["14"] = {"tweet_id": "14"}                           # already in the table (e.g. old pipeline)
    save(store, ["14", "15"], T + timedelta(hours=1))
    done = deliver_posts_and_profiles(store, db, since)
    assert (done.posts_inserted, done.posts_duplicate) == (1, 1) and db.posts["14"] == {"tweet_id": "14"}
    store.close()


def test_recovered_old_posts_count_when_first_seen_after_switch_on(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    save(store, ["5"], T + timedelta(hours=2))                    # an OLD post id (gap fill), seen now
    deliver_posts_and_profiles(store, db, since)
    assert list(db.posts) == ["5"]
    store.close()


def test_a_profile_is_sent_again_only_when_it_changed(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    assert deliver_posts_and_profiles(store, db, since).profiles == 1
    assert deliver_posts_and_profiles(store, db, since).profiles == 0

    store.set_status("1", "suspended", T + timedelta(days=1))     # a status change is a new version
    assert deliver_posts_and_profiles(store, db, since).profiles == 1
    assert db.profiles["1"]["status"] == "suspended"
    user = normalize_user({"id": "1", "username": "alice2", "name": "Alice"})
    store.save_profile(user, "Alice_Config", "renamed", T + timedelta(days=7))   # weekly refresh
    assert deliver_posts_and_profiles(store, db, since).profiles == 1
    assert db.profiles["1"]["username"] == "alice2" and db.profiles["1"]["last_updated"] == T + timedelta(days=7)
    store.close()


def test_a_change_in_the_same_second_is_still_sent(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    deliver_posts_and_profiles(store, db, since)
    user = normalize_user({"id": "1", "username": "alice_new", "name": "Alice"})
    store.save_profile(user, "Alice_Config", "renamed", T)          # same refresh time as before
    assert deliver_posts_and_profiles(store, db, since).profiles == 1
    assert (db.profiles["1"]["username"], db.profiles["1"]["status"]) == ("alice_new", "renamed")
    store.close()


def test_profiles_delivered_by_an_older_version_are_sent_once_more(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    store.conn.execute("INSERT INTO pipeline_sent_profiles (target, user_id, freshness, sent_at) "
                       "VALUES (?, '1', ?, ?)", (db.target, T.isoformat(), T.isoformat()))   # no fingerprint
    assert deliver_posts_and_profiles(store, db, since).profiles == 1
    assert deliver_posts_and_profiles(store, db, since).profiles == 0
    store.close()


def test_delivery_targets_are_independent(tmp_path):
    store = stored_source(tmp_path)
    local, shakti = FakePipelineDB(DB_CONFIG), FakePipelineDB({**DB_CONFIG, "host": "10.0.0.1"})
    save(store, ["20"], T + timedelta(minutes=1))
    deliver_posts_and_profiles(store, local, store.pipeline_since(local.target, T))
    later = T + timedelta(days=3)
    save(store, ["21"], later + timedelta(minutes=1))
    deliver_posts_and_profiles(store, shakti, store.pipeline_since(shakti.target, later))
    assert list(local.posts) == ["20"] and list(shakti.posts) == ["21"]   # each starts at its own switch-on
    assert shakti.profiles                                                 # all profiles at switch-on
    store.close()


def test_runs_since_switch_on_are_delivered_once(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    store.log_run({"run_id": "old", "started_at": T - timedelta(hours=1), "errors": [], "warnings": []})
    since = store.pipeline_since(db.target, T)
    for run_id, minutes in (("a", 1), ("b", 61)):
        store.log_run({"run_id": run_id, "started_at": T + timedelta(minutes=minutes),
                       "finished_at": T + timedelta(minutes=minutes + 2), "status": "success", "errors": [],
                       "warnings": [{"time": "t", "account": None, "message": "w"}], "posts_new": 3,
                       "pipeline_inserted": 3, "pipeline_duplicates": 0, "credits_after": 9.5})
    assert deliver_runs(store, db, since) == 2 and sorted(db.runs) == ["a", "b"]   # not the one before
    assert db.runs["a"]["warning_messages"] == [{"time": "t", "account": None, "message": "w"}]
    assert db.runs["a"]["duration_seconds"] == 120.0 and db.runs["a"]["credits_balance"] == 9.5
    assert deliver_runs(store, db, since) == 0
    store.close()


# --------------------------------------------------------------------------- whole runs
def last_run_row(settings):
    conn = sqlite3.connect(settings.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute("SELECT * FROM runs ORDER BY rowid DESC LIMIT 1").fetchone())
    finally:
        conn.close()


def test_runs_deliver_posts_profiles_and_run_rows(fake_client, make_settings, pipeline):
    fake_client.add_user("1", "Alice", description="bio")
    fake_client.add_referenced("900", "cut…", note_tweet={"text": "the complete original"})
    settings = pipeline_settings(make_settings, ["alice", "ghost"])
    assert run(settings, client=fake_client) == 0                  # first run: switch-on, profiles loaded
    db = pipeline.db
    assert db.posts == {} and list(db.profiles) == ["1"] and len(db.runs) == 1
    first = next(iter(db.runs.values()))
    assert (first["status"], first["rows_collected"], first["rows_inserted"], first["warnings"]) == ("success", 0, 0, 1)
    assert first["warning_messages"][0]["account"] == "ghost"
    assert first["warning_messages"][0]["message"] == "@ghost: not found (new user, nothing stored)"

    fake_client.add_post("1", "100", "hello")
    fake_client.add_post("1", "101", "RT @carol: cut…", referenced_tweets=[{"type": "retweeted", "id": "900"}])
    assert run(settings, client=fake_client) == 0
    assert sorted(db.posts) == ["100", "101"]
    assert db.posts["101"]["text"] == "RT @carol: the complete original" and db.posts["101"]["keyword"] == "alice"
    second = list(db.runs.values())[-1]
    assert (second["rows_collected"], second["rows_inserted"], second["duplicates_skipped"]) == (2, 2, 0)
    assert last_run_row(settings)["pipeline_inserted"] == 2


def collection(settings, user_id="1"):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute("SELECT last_scraped_at, posts_last_run FROM monitored_users WHERE user_id = ?",
                            (user_id,)).fetchone()
    finally:
        conn.close()


def test_last_collection_per_account_reaches_twitter_profiles(fake_client, make_settings, pipeline):
    from src.x_client import TransientApiError
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    fake_client.add_referenced("900", "the original")
    fake_client.add_post("1", "100")
    fake_client.add_post("1", "101", "RT @c: x…", referenced_tweets=[{"type": "retweeted", "id": "900"}])
    settings = pipeline_settings(make_settings, ["alice", "bob"])
    run(settings, client=fake_client)
    db = pipeline.db
    first_at, count = collection(settings)
    assert count == 2                                              # own posts only: the referenced tweet not counted
    assert (db.profiles["1"]["posts_last_run"], db.profiles["1"]["last_scraped_at"].isoformat()) == (2, first_at)
    assert db.profiles["2"]["posts_last_run"] == 0                 # checked, nothing found: 0, with a time
    assert db.profiles["2"]["last_scraped_at"] is not None

    run(settings, client=fake_client)                              # no new posts
    later_at, count = collection(settings)
    assert count == 0 and later_at >= first_at and db.profiles["1"]["posts_last_run"] == 0

    original = fake_client.get

    def get(path, params=None):
        if path == "/users/1/tweets":
            raise TransientApiError("Server error 503 on /users/1/tweets", 503)
        return original(path, params)

    fake_client.get = get
    fake_client.add_post("2", "200")
    run(settings, client=fake_client)                              # alice fails, bob collects 1
    assert collection(settings) == (later_at, 0)                   # failed: both kept as they were
    assert collection(settings, "2")[1] == 1 and db.profiles["2"]["posts_last_run"] == 1
    assert db.profiles["1"]["posts_last_run"] == 0


def test_gap_posts_count_and_a_later_failure_keeps_what_was_saved(fake_client, make_settings):
    from src.x_client import TransientApiError
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "1000")
    fake_client.page_size = 2
    settings = make_settings(["alice"], max_pages_per_user_per_run=2)
    run(settings, client=fake_client)
    for i in range(1001, 1007):
        fake_client.add_post("1", str(i))
    run(settings, client=fake_client)                              # 4 new (page cap), gap 1000-1003
    assert collection(settings)[1] == 4
    run(settings, client=fake_client)                              # 0 new + 2 recovered from the gap
    assert collection(settings)[1] == 2

    for i in range(1007, 1013):
        fake_client.add_post("1", str(i))
    run(settings, client=fake_client)                              # 4 new, gap 1006-1009 recorded
    original = fake_client.get

    def get(path, params=None):
        if path == "/users/1/tweets" and params.get("until_id"):   # the gap fill fails
            raise TransientApiError("Server error 503", 503)
        return original(path, params)

    fake_client.get = get
    fake_client.add_post("1", "1013")
    run(settings, client=fake_client)                              # 1 new saved, then the gap fill fails
    assert collection(settings)[1] == 1                            # what was saved still counts


def test_an_unreachable_pipeline_database_catches_up_later(fake_client, make_settings, pipeline):
    fake_client.add_user("1", "alice")
    settings = pipeline_settings(make_settings, ["alice"])
    FakePipelineDB.down = True
    fake_client.add_post("1", "100")
    assert run(settings, client=fake_client) == 1                  # an error: not delivered this run
    row = last_run_row(settings)
    assert row["status"] == "partial_success" and row["pipeline_inserted"] is None
    assert any("delivery failed" in e["message"] and "connection timeout" in e["message"]
               for e in json.loads(row["errors_json"]))

    FakePipelineDB.down = False
    fake_client.add_post("1", "101")
    assert run(settings, client=fake_client) == 0
    db = pipeline.db
    assert sorted(db.posts) == ["100", "101"]                      # the missed post too, no API cost
    assert len(db.runs) == 2                                       # the failed run's summary as well
    assert sorted(r["rows_inserted"] for r in db.runs.values() if r["rows_inserted"] is not None) == [2]


def test_pipeline_needs_sqlite_and_credentials(tmp_path, monkeypatch):
    from src.config import ConfigError, load_settings
    for name in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True, "pipeline": True}}))
    with pytest.raises(ConfigError, match="DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"):
        load_settings(tmp_path, env_path=tmp_path / "missing.env")
    for name, value in (("DB_HOST", "h"), ("DB_PORT", "1"), ("DB_NAME", "d"), ("DB_USER", "u"), ("DB_PASSWORD", "p")):
        monkeypatch.setenv(name, value)
    settings = load_settings(tmp_path, env_path=tmp_path / "missing.env")
    assert settings.pipeline_db == {"host": "h", "port": "1", "dbname": "d", "user": "u", "password": "p"}
    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"json": True, "pipeline": True}}))
    with pytest.raises(ConfigError, match="needs the sqlite output"):
        load_settings(tmp_path, env_path=tmp_path / "missing.env")


def test_delivered_counts_survive_a_failure_part_way(tmp_path):
    store = stored_source(tmp_path)
    db = FakePipelineDB(DB_CONFIG)
    since = store.pipeline_since(db.target, T)
    save(store, ["30"], T + timedelta(minutes=1))

    def broken_profile(row, config_name=None):
        raise ConnectionError("server closed the connection")

    db.save_profile = broken_profile
    done = Delivered()
    with pytest.raises(ConnectionError):
        deliver_posts_and_profiles(store, db, since, done)
    assert done.posts_inserted == 1                                # posts were committed before the failure
    assert store.pipeline_pending_posts(db.target, since) == []    # and recorded as delivered
    assert store.pipeline_sent_profiles(db.target) == {}           # the profile is retried next run
    store.close()
