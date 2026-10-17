"""Posts skipped because of the page cap are recorded as a gap and recovered later (review 1.3)."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from conftest import run_messages
from src.models import (SNOWFLAKE_EPOCH_MS, apply_gap_ops, in_gap, merge_gaps, new_gap,
                        snowflake_lower_bound)
from src.run_daily import run
from src.storage import JsonStore

T = datetime(2026, 10, 7, tzinfo=timezone.utc)


def db_ids(settings) -> list[int]:
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return sorted(int(r[0]) for r in conn.execute("SELECT post_id FROM posts"))
    finally:
        conn.close()


def json_ids(settings) -> list[int]:
    path = settings.json_dir / "tweets.jsonl"
    return [int(json.loads(line)["post_id"]) for line in path.read_text(encoding="utf-8").splitlines()]


def sqlite_gaps(settings) -> list[dict]:
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return json.loads(conn.execute("SELECT fetch_gaps FROM monitored_users").fetchone()[0] or "[]")
    finally:
        conn.close()


def json_gaps(settings) -> list[dict]:
    state = json.loads((settings.json_dir / "_state.json").read_text(encoding="utf-8"))
    return state.get("gaps", {}).get("1", [])


def last_errors(settings) -> list[str]:
    return run_messages(settings, "errors")


def setup(fake_client, make_settings, pages=2, page_size=10):
    """alice with one stored post (1000); cap = `pages` x `page_size` posts per run."""
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "1000")
    fake_client.page_size = page_size
    settings = make_settings(["alice"], max_pages_per_user_per_run=pages)
    assert run(settings, client=fake_client) == 0
    return settings


def add_posts(fake_client, first, last):
    for i in range(first, last + 1):
        fake_client.add_post("1", str(i))


# --------------------------------------------------------------------------- unit tests
def test_gap_helpers():
    gap = new_gap("100", "200", T)
    assert gap["key"] == "100-200" and in_gap("150", gap) and not in_gap("100", gap) and not in_gap("200", gap)

    gaps = apply_gap_ops([], [{"add": gap}, {"add": gap}])                 # adding twice keeps one
    assert len(gaps) == 1
    gaps = apply_gap_ops(gaps, [{"key": "100-200", "until_id": "150", "recovered": 5}])
    assert gaps[0]["until_id"] == "150" and gaps[0]["recovered"] == 5 and gaps[0]["key"] == "100-200"
    assert apply_gap_ops(gaps, [{"key": "other", "close": True}]) == gaps  # unknown key ignored
    assert apply_gap_ops(gaps, [{"key": "100-200", "close": True}]) == []

    shrunk, wide = dict(gap, until_id="150"), dict(gap)
    assert merge_gaps([[shrunk], [wide]]) == [wide]                       # widest range wins
    older = new_gap("10", "50", T)
    assert [g["key"] for g in merge_gaps([[older, gap]])] == ["100-200", "10-50"]   # newest first


def test_snowflake_lower_bound_matches_post_ids():
    ms = int(T.timestamp() * 1000) - SNOWFLAKE_EPOCH_MS
    post_at_t = ms << 22                                                  # a post created exactly at T
    bound = int(snowflake_lower_bound(T))
    assert bound < post_at_t                                              # included
    assert bound >= ((ms - 1) << 22)                                      # earlier posts excluded


# --------------------------------------------------------------------------- end to end
def test_review_reproduction_all_posts_recovered(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    add_posts(fake_client, 1001, 1025)                                    # 25 new, cap is 20

    run(settings, client=fake_client)                                     # 1025..1006 + gap (1000, 1006)
    assert db_ids(settings) == [1000] + list(range(1006, 1026))
    assert [(g["since_id"], g["until_id"]) for g in sqlite_gaps(settings)] == [("1000", "1006")]
    assert json_gaps(settings) == sqlite_gaps(settings)

    assert run(settings, client=fake_client) == 0                         # gap filled
    expected = list(range(1000, 1026))
    assert db_ids(settings) == expected
    assert sorted(json_ids(settings)) == expected and len(json_ids(settings)) == 26   # no duplicates
    assert sqlite_gaps(settings) == [] and json_gaps(settings) == []


def test_large_gap_shrinks_over_several_runs_then_closes(fake_client, make_settings):
    settings = setup(fake_client, make_settings, pages=3, page_size=10)  # 30 posts per run
    add_posts(fake_client, 1001, 1100)                                    # 100 new

    run(settings, client=fake_client)                                     # 1100..1071, gap (1000, 1071)
    sizes = []
    for _ in range(10):
        if not sqlite_gaps(settings):
            break
        run(settings, client=fake_client)   # 1 page for (no) new posts, 2 pages = 20 posts for the gap
        sizes.append(len(db_ids(settings)))
    assert sizes == [51, 71, 91, 101]                                     # 20 recovered per run
    assert db_ids(settings) == list(range(1000, 1101))
    assert sorted(json_ids(settings)) == list(range(1000, 1101)) and len(json_ids(settings)) == 101


def test_new_posts_come_first_while_a_gap_is_being_filled(fake_client, make_settings):
    settings = setup(fake_client, make_settings, pages=3, page_size=10)
    add_posts(fake_client, 1001, 1060)
    run(settings, client=fake_client)                                     # 1060..1031, gap (1000, 1031)

    add_posts(fake_client, 1061, 1065)                                    # new posts meanwhile
    fake_client.calls.clear()
    run(settings, client=fake_client)
    calls = fake_client.timeline_calls()
    assert calls[0]["since_id"] == "1060" and "until_id" not in calls[0]  # new posts fetched first
    assert calls[1]["since_id"] == "1000" and calls[1]["until_id"] == "1031"
    conn = sqlite3.connect(settings.sqlite_path)
    assert conn.execute("SELECT since_id FROM monitored_users").fetchone()[0] == "1065"   # cursor moved
    conn.close()
    run(settings, client=fake_client)
    assert db_ids(settings) == list(range(1000, 1066)) and sqlite_gaps(settings) == []


def test_first_run_gap_uses_the_lookback_time(fake_client, make_settings):
    fake_client.filter_start_time = True
    fake_client.page_size = 10
    fake_client.add_user("1", "alice")
    now = datetime.now(timezone.utc)

    def post_id(at):                                                       # realistic snowflake id
        return str((int(at.timestamp() * 1000) - SNOWFLAKE_EPOCH_MS) << 22)

    old = post_id(now - timedelta(days=5))                                 # outside the 2-day window
    recent = [post_id(now - timedelta(hours=40) + timedelta(minutes=m)) for m in range(25)]
    for pid in [old] + recent:
        fake_client.add_post("1", pid)
    settings = make_settings(["alice"], max_pages_per_user_per_run=2)

    run(settings, client=fake_client)                                      # 20 newest + gap
    [gap] = sqlite_gaps(settings)
    assert int(gap["since_id"]) > int(old)                                 # lower bound = lookback time
    run(settings, client=fake_client)
    assert db_ids(settings) == sorted(int(p) for p in recent)              # all 25, old one excluded
    assert sqlite_gaps(settings) == []


def test_two_separate_gaps_are_both_filled(fake_client, make_settings):
    settings = setup(fake_client, make_settings)                           # cap 20
    add_posts(fake_client, 1001, 1030)
    run(settings, client=fake_client)                                      # gap A (1000, 1011)
    add_posts(fake_client, 1031, 1060)
    run(settings, client=fake_client)                                      # cap again: gap B (1030, 1041)
    assert len(sqlite_gaps(settings)) == 2

    for _ in range(3):
        run(settings, client=fake_client)
    assert db_ids(settings) == list(range(1000, 1061)) and sqlite_gaps(settings) == []
    assert sorted(json_ids(settings)) == list(range(1000, 1061)) and len(json_ids(settings)) == 61


def test_gap_that_x_cannot_return_is_closed_with_a_warning(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    add_posts(fake_client, 1001, 1025)
    run(settings, client=fake_client)                                      # gap (1000, 1006)
    fake_client.posts["1"] = [p for p in fake_client.posts["1"] if not 1000 < int(p["id"]) < 1006]

    assert run(settings, client=fake_client) == 0                          # nothing failed: a warning
    assert sqlite_gaps(settings) == [] and json_gaps(settings) == []       # closed, not retried forever
    assert any("could not be filled" in w and "3,200" in w for w in run_messages(settings, "warnings"))
    assert last_errors(settings) == []


def test_failed_output_keeps_its_gap_and_catches_up_without_duplicates(fake_client, make_settings, monkeypatch):
    settings = setup(fake_client, make_settings)
    add_posts(fake_client, 1001, 1025)
    run(settings, client=fake_client)                                      # gap (1000, 1006) in both

    original = JsonStore.save_posts
    failed = {"done": False}

    def flaky(self, user_id, username, config_username, posts, new_since_id, fetched_at, gap_ops=None):
        if gap_ops and not failed["done"] and any("close" in op or "until_id" in op for op in gap_ops):
            failed["done"] = True
            raise OSError("disk full")
        return original(self, user_id, username, config_username, posts, new_since_id, fetched_at, gap_ops)

    monkeypatch.setattr(JsonStore, "save_posts", flaky)
    assert run(settings, client=fake_client) == 1                          # JSON failed during gap fill
    assert sqlite_gaps(settings) == [] and len(json_gaps(settings)) == 1   # JSON still knows its gap
    assert db_ids(settings) == list(range(1000, 1026))

    assert run(settings, client=fake_client) == 0                          # refetched for JSON only
    assert sorted(json_ids(settings)) == list(range(1000, 1026)) and len(json_ids(settings)) == 26
    assert json_gaps(settings) == [] and db_ids(settings) == list(range(1000, 1026))


def test_json_does_not_append_older_posts_outside_its_gaps(fake_client, make_settings):
    settings = setup(fake_client, make_settings, pages=10)
    add_posts(fake_client, 1001, 1005)
    run(settings, client=fake_client)
    store = JsonStore(settings.json_dir)
    store.open()
    from src.models import normalize_post, utc_now
    old_posts = [normalize_post({"id": "1003", "text": "again"}, {}, utc_now(), "alice")]
    store.save_posts("1", "alice", "alice", old_posts, None, utc_now(), [])   # already stored, no gap
    assert json_ids(settings).count(1003) == 1
