"""post_url (https://x.com/<username>/status/<post_id>) in every output, including data stored
before the field existed."""

import json
import sqlite3

from src.models import normalize_post, post_url, utc_now
from src.run_daily import run
from src.storage import JsonStore, SQLiteStore


def test_post_url_format():
    assert post_url("timesofindia", "2107712527866188005") == "https://x.com/timesofindia/status/2107712527866188005"
    assert post_url(None, "123") == "https://x.com/i/status/123"
    assert post_url("", "123") == "https://x.com/i/status/123"
    post = normalize_post({"id": "55", "text": "RT @x: hi", "referenced_tweets": [{"type": "retweeted", "id": "9"}]},
                          {}, utc_now(), "alice")
    assert post["post_url"] == "https://x.com/alice/status/55"           # a retweet's own link


def test_new_posts_get_the_same_post_url_in_sqlite_and_json(fake_client, make_settings):
    fake_client.add_user("1", "Alice")
    fake_client.add_post("1", "101")
    fake_client.add_post("1", "102")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    conn = sqlite3.connect(settings.sqlite_path)
    db = dict(conn.execute("SELECT post_id, post_url FROM posts").fetchall())
    conn.close()
    lines = [json.loads(l) for l in (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()]
    js = {r["post_id"]: r["post_url"] for r in lines}
    assert db == js == {"101": "https://x.com/Alice/status/101", "102": "https://x.com/Alice/status/102"}
    assert list(lines[0])[:5] == ["post_id", "author_id", "username", "created_at", "post_url"]


def test_old_sqlite_database_gets_post_url_filled_once(tmp_path):
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE posts (post_id TEXT PRIMARY KEY, author_id TEXT, username TEXT, created_at TEXT, "
                 "text TEXT, first_seen_at TEXT NOT NULL)")
    conn.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?)", [
        ("101", "1", "alice", "2026-10-07T00:00:00+00:00", "hello", "2026-10-07T00:00:00+00:00"),
        ("102", "1", None, "2026-10-07T00:00:00+00:00", "no name", "2026-10-07T00:00:00+00:00")])
    conn.commit()
    conn.close()

    store = SQLiteStore(db)
    store.open()
    store.close()
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT post_id, post_url, text FROM posts ORDER BY post_id").fetchall() == [
        ("101", "https://x.com/alice/status/101", "hello"),
        ("102", "https://x.com/i/status/102", "no name")]                   # other data untouched
    conn.execute("UPDATE posts SET post_url = 'kept' WHERE post_id = '101'")
    conn.commit()
    conn.close()

    store = SQLiteStore(db)                                                # opening again: no second fill
    store.open()
    store.close()
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT post_url FROM posts WHERE post_id = '101'").fetchone() == ("kept",)
    conn.close()


def write_old_json_output(directory, records):
    """tweets.jsonl + _state.json as written by the version before post_url."""
    directory.mkdir(parents=True, exist_ok=True)
    data = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records).encode("utf-8")
    (directory / "tweets.jsonl").write_bytes(data)
    (directory / "_state.json").write_text(json.dumps({
        "tweets_file_size": len(data), "cursors": {"1": max((r["post_id"] for r in records), key=int)},
        "last_posts_fetch_at": {}, "gaps": {}}), encoding="utf-8")


OLD = [{"post_id": "101", "author_id": "1", "username": "alice", "created_at": "2026-10-07T10:00:00+00:00",
        "post_type": "original", "text": "café ✓"},
       {"post_id": "102", "author_id": "1", "username": "alice", "created_at": "2026-10-07T11:00:00+00:00",
        "post_type": "retweet", "text": "RT @x: hi"}]


def read_lines(directory):
    return [json.loads(l) for l in (directory / "tweets.jsonl").read_text(encoding="utf-8").splitlines()]


def test_old_tweets_jsonl_is_rewritten_once_with_post_url(tmp_path):
    out = tmp_path / "json"
    write_old_json_output(out, OLD)
    store = JsonStore(out)
    store.open()

    lines = read_lines(out)
    assert [r["post_id"] for r in lines] == ["101", "102"]                 # same lines, same order
    assert [r["post_url"] for r in lines] == ["https://x.com/alice/status/101", "https://x.com/alice/status/102"]
    assert list(lines[0])[:5] == ["post_id", "author_id", "username", "created_at", "post_url"]
    assert lines[0]["text"] == "café ✓"                                    # non-ASCII kept as is
    state = json.loads((out / "_state.json").read_text(encoding="utf-8"))
    assert state["tweets_file_size"] == (out / "tweets.jsonl").stat().st_size   # crash check stays valid
    assert state["post_urls_added"] is True and "pending_rewrite" not in state
    assert not (out / "tweets.jsonl.rewrite.tmp").exists()
    assert b"\r\n" not in (out / "tweets.jsonl").read_bytes()

    before = (out / "tweets.jsonl").read_bytes()
    store = JsonStore(out)
    store.open()                                                           # second open: nothing rewritten
    assert (out / "tweets.jsonl").read_bytes() == before


def test_new_lines_append_after_the_rewrite(fake_client, make_settings):
    settings = make_settings(["alice"])
    write_old_json_output(settings.json_dir, OLD)
    fake_client.add_user("1", "alice")
    for pid in ("101", "102", "103"):
        fake_client.add_post("1", pid)
    run(settings, client=fake_client)
    lines = read_lines(settings.json_dir)
    assert [r["post_id"] for r in lines] == ["101", "102", "103"]          # no duplicates, new one appended
    assert all(r["post_url"] == f"https://x.com/alice/status/{r['post_id']}" for r in lines)


def test_rewrite_interrupted_before_the_swap_is_redone(tmp_path):
    out = tmp_path / "json"
    write_old_json_output(out, OLD)
    old_size = (out / "tweets.jsonl").stat().st_size
    state = json.loads((out / "_state.json").read_text(encoding="utf-8"))
    state.update(tweets_file_size=old_size + 80, pending_rewrite={"old_size": old_size, "new_size": old_size + 80})
    (out / "_state.json").write_text(json.dumps(state), encoding="utf-8")   # crash after step 1
    (out / "tweets.jsonl.rewrite.tmp").write_text("partial", encoding="utf-8")

    store = JsonStore(out)
    store.open()
    lines = read_lines(out)
    assert [r["post_id"] for r in lines] == ["101", "102"] and all("post_url" in r for r in lines)
    assert json.loads((out / "_state.json").read_text())["tweets_file_size"] == (out / "tweets.jsonl").stat().st_size
    assert not (out / "tweets.jsonl.rewrite.tmp").exists()


def test_rewrite_interrupted_after_the_swap_is_not_cut(tmp_path):
    out = tmp_path / "json"
    write_old_json_output(out, OLD)
    old_size = (out / "tweets.jsonl").stat().st_size
    store = JsonStore(out)
    store.open()                                                           # full rewrite
    new_size = (out / "tweets.jsonl").stat().st_size
    rewritten = (out / "tweets.jsonl").read_bytes()

    state = json.loads((out / "_state.json").read_text(encoding="utf-8"))
    state.pop("post_urls_added")
    state.update(tweets_file_size=new_size, pending_rewrite={"old_size": old_size, "new_size": new_size})
    (out / "_state.json").write_text(json.dumps(state), encoding="utf-8")   # crash after step 2

    store = JsonStore(out)
    store.open()
    assert (out / "tweets.jsonl").read_bytes() == rewritten                # not truncated, not rewritten
    assert json.loads((out / "_state.json").read_text())["tweets_file_size"] == new_size
