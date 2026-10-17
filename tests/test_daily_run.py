import json
import sqlite3
from datetime import timedelta

from conftest import run_messages
from src.models import utc_now
from src.run_daily import run
from src.storage import JsonStore
from src.timeline import fetch_new_posts


def db_rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def profiles(settings):
    data = json.loads((settings.json_dir / "profile.json").read_text(encoding="utf-8"))
    return {u["user_id"]: u for u in data["users"]}


def tweet_ids(settings):
    lines = (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line)["post_id"] for line in lines]


def test_pagination_and_page_cap(fake_client):
    fake_client.add_user("1", "alice")
    for i in range(1, 26):
        fake_client.add_post("1", str(1000 + i))
    fake_client.page_size = 10
    kwargs = dict(tweet_fields=[], media_fields=[], expansions=[])

    full = fetch_new_posts(fake_client, "1", since_id="1000", max_pages=10, **kwargs)
    assert len(full.posts) == 25 and full.pages == 3 and not full.hit_page_cap

    capped = fetch_new_posts(fake_client, "1", since_id="1000", max_pages=2, **kwargs)
    assert len(capped.posts) == 20 and capped.hit_page_cap


def test_first_run_then_incremental_run(fake_client, make_settings):
    fake_client.add_user("1", "Alice")
    fake_client.add_user("2", "bob")
    fake_client.add_post("1", "100", "first")
    fake_client.add_post("1", "101", "RT @carol: long text…", referenced_tweets=[{"type": "retweeted", "id": "50"}])
    fake_client.add_post("2", "200", "reply", referenced_tweets=[{"type": "replied_to", "id": "60"}])
    settings = make_settings(["alice", "bob"])

    assert run(settings, client=fake_client) == 0

    # First run: profiles looked up once, timeline uses start_time (no since_id)
    assert [p for path, p in fake_client.calls if path == "/users/by"][0]["usernames"] == ["alice", "bob"]
    first_calls = fake_client.timeline_calls()
    assert all("start_time" in c and "since_id" not in c for c in first_calls)

    assert db_rows(settings, "SELECT post_id, post_type, referenced_post_id FROM posts ORDER BY post_id") == [
        ("100", "original", None), ("101", "retweet", "50"), ("200", "reply", "60")]
    assert db_rows(settings, "SELECT user_id, since_id FROM monitored_users ORDER BY user_id") == [
        ("1", "101"), ("2", "200")]
    assert tweet_ids(settings) == ["100", "101", "200"]          # oldest first within each user's batch
    first = json.loads((settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()[1])
    assert first["author_id"] == "1" and first["username"] == "Alice" and first["post_type"] == "retweet"
    alice = profiles(settings)["1"]
    assert alice["username"] == "Alice" and alice["config_username"] == "alice"
    assert alice["profile"]["username"] == "Alice" and alice["changes"] == []          # first save: no changes
    assert sorted(profiles(settings)) == ["1", "2"]

    # Second run: one new post; no profile lookup (weekly); since_id used
    fake_client.calls.clear()
    fake_client.add_post("1", "102", "new one")
    assert run(settings, client=fake_client) == 0
    assert not [c for c in fake_client.calls if c[0] == "/users/by"]
    calls = {c[0]: c[1] for c in fake_client.calls}
    assert calls["/users/1/tweets"]["since_id"] == "101"
    assert calls["/users/2/tweets"]["since_id"] == "200"

    assert tweet_ids(settings) == ["100", "101", "200", "102"]   # appended, nothing rewritten
    assert db_rows(settings, "SELECT COUNT(*) FROM posts") == [(4,)]
    assert db_rows(settings, "SELECT COUNT(*) FROM runs") == [(2,)]


def test_profile_refresh_is_weekly(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    # Pretend the last refresh was 8 days ago in every output
    old = (utc_now() - timedelta(days=8)).isoformat()
    conn = sqlite3.connect(settings.sqlite_path)
    conn.execute("UPDATE monitored_users SET last_profile_refresh_at = ?", (old,))
    conn.execute("UPDATE user_profiles SET refreshed_at = ?", (old,))    # written together by save_profile
    conn.commit()
    conn.close()
    profile_path = settings.json_dir / "profile.json"
    data = json.loads(profile_path.read_text())
    data["users"][0]["refreshed_at"] = old
    profile_path.write_text(json.dumps(data))
    if settings.outputs.get("postgres"):          # the latest refresh time across outputs counts
        import psycopg
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as pg:
            pg.execute("UPDATE user_profiles SET refreshed_at = %s", (old,))

    fake_client.calls.clear()
    run(settings, client=fake_client)
    assert [c for c in fake_client.calls if c[0] == "/users/by"]
    assert profiles(settings)["1"]["refreshed_at"] > old         # refreshed again
    assert profiles(settings)["1"]["changes"] == []                 # nothing changed -> nothing logged


def test_lagging_json_output_is_filled_from_sqlite_not_refetched(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    # The JSON output misses a run: its cursor falls behind SQLite's
    fake_client.add_post("1", "101", "missed by JSON", entities={"hashtags": [{"tag": "x"}]})
    settings.outputs["json"] = False
    run(settings, client=fake_client)
    reference = (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8")
    settings.outputs["json"] = True

    fake_client.calls.clear()
    run(settings, client=fake_client)
    assert fake_client.timeline_calls()[0]["since_id"] == "101"   # filled from SQLite: nothing fetched again
    assert tweet_ids(settings) == ["100", "101"]                    # copied once
    assert db_rows(settings, "SELECT COUNT(*) FROM posts") == [(2,)]
    lines = (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
    assert "\n".join(lines[:1]) + "\n" == reference                 # earlier lines untouched
    copied = json.loads(lines[1])
    sqlite_first_seen = db_rows(settings, "SELECT first_seen_at FROM posts WHERE post_id = '101'")[0][0]
    assert copied["text"] == "missed by JSON" and copied["entities"] == {"hashtags": [{"tag": "x"}]}
    assert copied["first_seen_at"] == sqlite_first_seen             # when it was really collected
    assert list(copied) == list(json.loads(lines[0]))               # same fields, same order as a normal line


def test_json_skips_tweets_it_already_has_when_another_output_lags(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.add_post("1", "101")
    settings.outputs["sqlite"] = False          # SQLite misses this run; JSON gets 101
    run(settings, client=fake_client)
    settings.outputs["sqlite"] = True

    fake_client.calls.clear()
    run(settings, client=fake_client)           # refetches 101 for SQLite
    assert fake_client.timeline_calls()[0]["since_id"] == "100"
    assert tweet_ids(settings) == ["100", "101"]  # JSON did not append 101 twice
    assert db_rows(settings, "SELECT COUNT(*) FROM posts") == [(2,)]


def test_rename_detected(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["username"] = "alice_new"
    run(settings, client=fake_client, force_profile_refresh=True)

    entry = profiles(settings)["1"]
    assert entry["username"] == "alice_new" and entry["status"] == "renamed" and entry["config_username"] == "alice"
    assert tweet_ids(settings) == ["100"]
    assert db_rows(settings, "SELECT username, status FROM monitored_users") == [("alice_new", "renamed")]


def test_unknown_and_protected_users(fake_client, make_settings):
    fake_client.add_user("1", "alice", protected=True)
    settings = make_settings(["alice", "ghost"])
    assert run(settings, client=fake_client) == 0           # ghost: a warning (account does not exist)
    assert run_messages(settings, "warnings") == ["@ghost: not found (new user, nothing stored)"]
    assert fake_client.timeline_calls() == []               # protected -> skipped, ghost -> unknown
    assert db_rows(settings, "SELECT status FROM monitored_users") == [("protected",)]


def test_unfinished_append_is_cut_off_and_refetched(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    # Simulate a crash after tweets were appended but before _state.json recorded them
    fake_client.add_post("1", "101")
    with open(settings.json_dir / "tweets.jsonl", "a", encoding="utf-8") as fh:
        fh.write('{"post_id": "101", "author_id": "1"}\n{"post_id": "10')   # full line + half line
    run(settings, client=fake_client)

    assert tweet_ids(settings) == ["100", "101"]   # junk removed, 101 written once by the real run
    rec = json.loads((settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()[1])
    assert rec["username"] == "alice"


def test_state_rebuilt_from_tweets_if_deleted(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    (settings.json_dir / "_state.json").unlink()
    store = JsonStore(settings.json_dir)
    store.open()
    assert store.load_users()["1"].since_id == "100"
    assert store.load_users()["1"].config_username == "alice"   # from profile.json


def test_unreadable_profile_json_disables_json_output_without_touching_it(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    profile_path = settings.json_dir / "profile.json"
    profile_path.write_text("{broken", encoding="utf-8")
    fake_client.add_post("1", "100")
    assert run(settings, client=fake_client) == 1                     # reported as an error
    assert profile_path.read_text(encoding="utf-8") == "{broken"      # left as-is for inspection
    assert db_rows(settings, "SELECT COUNT(*) FROM posts") == [(1,)]  # other outputs still written


def test_merge_takes_status_from_tracking_outputs_and_lowest_cursor_from_all():
    from datetime import datetime, timezone

    from src.run_daily import merge_states
    from src.storage import UserState

    class Stub:
        def __init__(self, tracks, users):
            self.tracks_status, self._users = tracks, users

        def load_users(self):
            return self._users

    later = datetime(2026, 10, 6, tzinfo=timezone.utc)
    sqlite_like = Stub(True, {"1": UserState("1", "alice", "alice", status="renamed", since_id="105",
                                             last_profile_refresh_at=datetime(2026, 10, 1, tzinfo=timezone.utc))})
    postgres_like = Stub(False, {"1": UserState("1", None, "alice", since_id="100", last_profile_refresh_at=later)})

    merged = merge_states([sqlite_like, postgres_like])["1"]
    assert merged.status == "renamed" and merged.config_username == "alice"   # from SQLite, not PostgreSQL
    assert merged.since_id == "100"                                            # lowest cursor across all
    assert merged.last_profile_refresh_at == later


def write_config(folder, usernames, options):
    folder.mkdir(exist_ok=True)
    (folder / "username.json").write_text(json.dumps(usernames))
    (folder / "input.json").write_text(json.dumps(options))
    return folder


def test_config_folder_is_loaded(tmp_path):
    from src.config import load_settings

    cfg = write_config(tmp_path / "config", ["@Alice", "bob", "alice"],
                       {"lookback_days_first_run": 3, "profile_refresh_days": 14,
                        "outputs": {"sqlite": True, "json": True}, "json_dir": "out/json"})
    settings = load_settings(cfg, env_path=tmp_path / "missing.env")
    assert settings.usernames == ["Alice", "bob"]            # '@' stripped, duplicate removed
    assert settings.lookback_days_first_run == 3 and settings.profile_refresh_days == 14
    assert settings.max_pages_per_user_per_run == 10          # default kept
    assert settings.json_dir.parts[-2:] == ("out", "json")


def test_config_folder_errors(tmp_path):
    import pytest

    from src.config import ConfigError, load_settings

    env = tmp_path / "missing.env"
    with pytest.raises(ConfigError, match="Config file not found"):
        load_settings(tmp_path / "nothing", env_path=env)
    (tmp_path / "only_input").mkdir()
    (tmp_path / "only_input" / "input.json").write_text("{}")
    with pytest.raises(ConfigError, match="username.json"):              # required when the source is the file
        load_settings(tmp_path / "only_input", env_path=env)
    cfg = write_config(tmp_path / "c1", [{"id": 1, "username": "alice", "enabled": False}], {})
    with pytest.raises(ConfigError, match="no enabled usernames"):
        load_settings(cfg, env_path=env)
    cfg = write_config(tmp_path / "c2", ["alice"], {"lookback_days_first_run": 0})
    with pytest.raises(ConfigError, match="positive integer"):
        load_settings(cfg, env_path=env)
    cfg = write_config(tmp_path / "c3", [{"id": 1, "username": "alice", "enabled": "yes"}], {})
    with pytest.raises(ConfigError, match="true or false"):
        load_settings(cfg, env_path=env)


def test_username_objects_with_enabled_flag(tmp_path):
    from src.config import load_settings

    cfg = write_config(tmp_path / "config", [
        {"id": 1, "username": "India", "enabled": False},
        {"id": 2, "username": "dgprPaknavy", "enabled": True},
        {"id": 3, "username": "srilankanavyofficial", "enabled": True},   # 20 chars: invalid
        {"id": 4, "username": "@BBCWorld"},                               # no flag -> enabled
        {"id": 5, "username": "DGPRPAKNAVY", "enabled": True},            # duplicate
    ], {"outputs": {"sqlite": True, "json": True}})
    settings = load_settings(cfg, env_path=tmp_path / "missing.env")
    assert settings.usernames == ["dgprPaknavy", "BBCWorld"]
    assert settings.total_usernames == 5
    assert len(settings.config_warnings) == 2
    assert "id 3" in settings.config_warnings[0] and "has 20" in settings.config_warnings[0]
    assert "id 5" in settings.config_warnings[1] and "more than once" in settings.config_warnings[1]


def test_postgres_cannot_be_the_only_output(tmp_path):
    import pytest

    from src.config import ConfigError, load_settings

    cfg = write_config(tmp_path / "config", ["alice"], {"outputs": {"postgres": True}})
    with pytest.raises(ConfigError, match="only output"):
        load_settings(cfg, env_path=tmp_path / "missing.env")
