"""Filling the JSON output in from SQLite (storage/sync.py copy_missing_to_json): what JSON missed is copied
locally instead of being fetched (and paid) again, and only when that is provably exact."""

import json
from datetime import datetime, timedelta, timezone

from src.models import new_gap, normalize_post
from src.run_daily import run
from src.storage import JsonStore, SQLiteStore
from src.storage.sync import copy_missing_to_json

T = datetime(2026, 10, 11, 6, 0, tzinfo=timezone.utc)


def posts(ids, at=T):
    media = {"3_m": {"media_key": "3_m", "type": "photo", "url": "https://img/m.jpg"}}
    return [normalize_post({"id": i, "text": f"post {i} &amp; more", "author_id": "1", "lang": "en",
                            "created_at": "2026-10-11T05:00:00.000Z", "attachments": {"media_keys": ["3_m"]},
                            "entities": {"hashtags": [{"tag": "t"}]}}, media, at, "alice") for i in ids]


def stores(tmp_path, name):
    sqlite, js = SQLiteStore(tmp_path / f"{name}.db"), JsonStore(tmp_path / name)
    sqlite.open()
    js.open()
    return sqlite, js


def save_both(sqlite, js, ids, cursor, at=T, gap_ops=None, json_too=True):
    for store in (sqlite, js) if json_too else (sqlite,):
        store.save_posts("1", "alice", "alice", posts(ids, at), cursor, at, gap_ops or [])


def lines(js):
    return js.tweets_path.read_text(encoding="utf-8").splitlines()


def test_filled_file_is_identical_to_one_that_never_missed_a_save(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    _, reference = stores(tmp_path, "ref")
    save_both(sqlite, js, ["100"], "100")
    reference.save_posts("1", "alice", "alice", posts(["100"]), "100", T)
    later = T + timedelta(hours=1)
    save_both(sqlite, js, ["101", "102"], "102", at=later, json_too=False)   # JSON misses this save
    reference.save_posts("1", "alice", "alice", posts(["101", "102"], later), "102", later)

    result = copy_missing_to_json(sqlite, js)
    assert (result.posts, result.accounts, result.left_to_refetch) == (2, 1, [])
    assert js.tweets_path.read_bytes() == reference.tweets_path.read_bytes()       # byte for byte
    assert js.load_users()["1"].since_id == "102"
    state = json.loads((tmp_path / "a" / "_state.json").read_text(encoding="utf-8"))
    assert state["tweets_file_size"] == js.tweets_path.stat().st_size              # crash check stays valid

    again = copy_missing_to_json(sqlite, js)                                       # nothing left to do
    assert (again.posts, again.accounts) == (0, 0) and len(lines(js)) == 3


def test_normal_runs_copy_nothing(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100", "101"], "101")
    before = js.tweets_path.read_bytes()
    result = copy_missing_to_json(sqlite, js)
    assert (result.posts, result.accounts, result.left_to_refetch) == (0, 0, [])
    assert js.tweets_path.read_bytes() == before


def test_gap_posts_sqlite_recovered_are_copied_and_the_gap_closed(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    gap = new_gap("100", "105", T)
    save_both(sqlite, js, ["100"], "100")
    save_both(sqlite, js, ["105", "106"], "106", gap_ops=[{"add": gap}])           # both record the gap
    sqlite.save_posts("1", "alice", "alice", posts(["101", "103"]), None, T,      # SQLite fills it,
                      [{"key": gap["key"], "close": True}])                       # JSON's save failed
    assert js.load_users()["1"].gaps and not sqlite.load_users()["1"].gaps

    result = copy_missing_to_json(sqlite, js)
    assert result.posts == 2 and result.left_to_refetch == []
    assert [json.loads(l)["post_id"] for l in lines(js)] == ["100", "105", "106", "101", "103"]
    assert js.load_users()["1"].gaps == [] and js.load_users()["1"].since_id == "106"


def test_gaps_sqlite_still_has_are_kept_for_json(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100"], "100")
    gap = new_gap("100", "110", T)                                                 # SQLite: page cap, gap
    sqlite.save_posts("1", "alice", "alice", posts(["110", "111"]), "111", T, [{"add": gap}])
    result = copy_missing_to_json(sqlite, js)
    assert result.posts == 2
    assert js.load_users()["1"].gaps == [gap] and js.load_users()["1"].since_id == "111"
    # The gap is then filled the normal way: JSON accepts the posts inside its own gap once.
    js.save_posts("1", "alice", "alice", posts(["105"]), None, T, [{"key": gap["key"], "close": True}])
    assert [json.loads(l)["post_id"] for l in lines(js)] == ["100", "110", "111", "105"]


def test_account_json_never_had_gets_all_its_posts(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100", "101"], "101", json_too=False)
    result = copy_missing_to_json(sqlite, js)
    assert result.posts == 2 and js.load_users()["1"].since_id == "101"
    assert "1" in js.profiles                                                      # account entry created


def test_json_ahead_of_sqlite_is_left_to_the_refetch(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100"], "100")
    js.save_posts("1", "alice", "alice", posts(["101"]), "101", T)                # SQLite missed this one
    before = js.tweets_path.read_bytes()
    result = copy_missing_to_json(sqlite, js)
    assert result.left_to_refetch == ["alice"] and result.posts == 0
    assert js.tweets_path.read_bytes() == before and js.load_users()["1"].since_id == "101"


def test_partly_overlapping_gaps_are_left_to_the_refetch(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100"], "100")
    js.save_posts("1", "alice", "alice", posts(["120"]), "120", T, [{"add": new_gap("100", "110", T)}])
    sqlite.save_posts("1", "alice", "alice", posts(["130"]), "130", T, [{"add": new_gap("105", "125", T)}])
    before = js.tweets_path.read_bytes()
    result = copy_missing_to_json(sqlite, js)
    assert result.left_to_refetch == ["alice"] and js.tweets_path.read_bytes() == before


def test_posts_without_author_stop_the_fill_in(tmp_path):
    sqlite, js = stores(tmp_path, "a")
    save_both(sqlite, js, ["100"], "100", json_too=False)
    sqlite.conn.execute("UPDATE posts SET author_id = NULL")
    result = copy_missing_to_json(sqlite, js)
    assert result.posts == 0 and result.left_to_refetch and not js.tweets_path.exists()


def test_run_with_json_skipped_twice_catches_up_without_paying_again(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)
    settings.outputs["json"] = False
    for pid in ("101", "102"):
        fake_client.add_post("1", pid)
        run(settings, client=fake_client)
    settings.outputs["json"] = True
    billed_before = fake_client.credit_balance
    fake_client.calls.clear()
    assert run(settings, client=fake_client) == 0
    assert fake_client.timeline_calls()[0]["since_id"] == "102"                    # from SQLite's cursor
    assert fake_client.credit_balance == billed_before                             # no post paid again
    ids = [json.loads(l)["post_id"] for l in (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8")
           .splitlines()]
    assert ids == ["100", "101", "102"]
