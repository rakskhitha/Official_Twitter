"""Non-fatal API errors and unexpected exceptions must not abort the run (code review B1)."""

import json
import logging
import sqlite3

from conftest import run_messages
from src import run_daily, users
from src.run_daily import run
from src.x_client import FatalApiError, XApiError

BAD_FIELD = XApiError("HTTP 400 on /users/by: Invalid Request: bad user.fields value", 400)


def rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def fail_on(client, predicate, exc):
    """Make client.get raise `exc` for calls where predicate(path, params) is true."""
    original = client.get

    def get(path, params=None):
        if predicate(path, params or {}):
            raise exc
        return original(path, params)

    client.get = get


def test_400_on_profile_lookup_still_collects_tweets_and_writes_summary(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    assert run(settings, client=fake_client) == 0
    refreshed_before = rows(settings, "SELECT last_profile_refresh_at FROM monitored_users")

    fake_client.add_post("1", "101")
    fail_on(fake_client, lambda path, _: path == "/users/by", BAD_FIELD)
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1   # errors, but not stopped

    assert rows(settings, "SELECT post_id FROM posts ORDER BY post_id") == [("100",), ("101",)]
    assert rows(settings, "SELECT status FROM monitored_users") == [("ok",)]     # not marked not_found
    assert rows(settings, "SELECT last_profile_refresh_at FROM monitored_users") == refreshed_before  # retried
    assert rows(settings, "SELECT COUNT(*) FROM runs") == [(2,)]                 # summary written
    errors = run_messages(settings, "errors")
    assert len(errors) == 1                                                       # reported once, not twice
    assert "Profile lookup failed" in errors[0] and "api.user_fields" in errors[0]
    assert rows(settings, "SELECT status FROM runs ORDER BY rowid DESC LIMIT 1") == [("partial_success",)]


def test_new_user_in_a_failed_lookup_is_not_registered_and_retried(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    fail_on(fake_client, lambda path, _: path == "/users/by", BAD_FIELD)
    assert run(settings, client=fake_client) == 1
    assert rows(settings, "SELECT COUNT(*) FROM monitored_users") == [(0,)]
    assert fake_client.timeline_calls() == []

    fake_client.get = type(fake_client).get.__get__(fake_client)   # API healthy again
    assert run(settings, client=fake_client) == 0
    assert rows(settings, "SELECT user_id, status FROM monitored_users") == [("1", "ok")]


def test_one_failed_batch_does_not_lose_the_other_batches(fake_client, make_settings, monkeypatch):
    monkeypatch.setattr(users, "BATCH_SIZE", 1)
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    fake_client.add_post("2", "200")
    settings = make_settings(["alice", "bob"])
    assert run(settings, client=fake_client) == 0

    fake_client.add_post("2", "201")
    fake_client.users["1"]["name"] = "Alice Updated"
    fail_on(fake_client, lambda path, p: path == "/users/by" and "bob" in p["usernames"], BAD_FIELD)
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1

    assert rows(settings, "SELECT user_id, status FROM monitored_users ORDER BY user_id") == [("1", "ok"), ("2", "ok")]
    assert rows(settings, "SELECT name FROM user_profiles WHERE user_id = '1'") == [("Alice Updated",)]
    assert ("201",) in rows(settings, "SELECT post_id FROM posts")              # bob still collected


def test_400_on_lookup_by_id_keeps_status_and_run_continues(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["username"] = "alice_new"     # renamed: /users/by says not found
    fake_client.add_post("1", "101")
    fail_on(fake_client, lambda path, _: path == "/users/1", XApiError("HTTP 400 on /users/1: bad", 400))
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1

    assert rows(settings, "SELECT username, status FROM monitored_users") == [("alice", "ok")]
    assert ("101",) in rows(settings, "SELECT post_id FROM posts")


def test_fatal_error_in_profile_lookup_still_stops_the_run(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    fail_on(fake_client, lambda path, _: path == "/users/by", FatalApiError("HTTP 401: bad token", 401))
    assert run(settings, client=fake_client) == 2
    assert fake_client.timeline_calls() == []
    assert rows(settings, "SELECT COUNT(*) FROM runs") == [(1,)]


def test_unexpected_exception_is_logged_with_traceback_and_summary_written(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    fail_on(fake_client, lambda path, _: path == "/users/by", KeyError("surprise"))
    with caplog.at_level(logging.ERROR):
        assert run(settings, client=fake_client) == 2
    assert any(r.exc_info and r.exc_info[0] is KeyError for r in caplog.records)
    assert rows(settings, "SELECT COUNT(*) FROM runs") == [(1,)]
    assert rows(settings, "SELECT status FROM runs") == [("failed",)]
    assert run_messages(settings, "errors") == ["Run stopped by an unexpected error: KeyError: 'surprise'"]


def test_bad_post_object_fails_only_that_user(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    settings = make_settings(["alice", "bob"])
    run(settings, client=fake_client)

    fake_client.add_post("2", "200")
    original = fake_client.get

    def get(path, params=None):
        if path == "/users/1/tweets":   # malformed post object (no "id") -> KeyError in normalize_post
            return {"data": [{"text": "no id"}], "meta": {"result_count": 1}}
        return original(path, params)

    fake_client.get = get
    assert run(settings, client=fake_client) == 1
    assert rows(settings, "SELECT post_id FROM posts") == [("200",)]                 # bob saved
    assert rows(settings, "SELECT user_id, since_id FROM monitored_users ORDER BY user_id") == [
        ("1", None), ("2", "200")]                                                     # alice not advanced


def test_main_logs_unexpected_errors_to_the_log_file(tmp_path, monkeypatch):
    config = tmp_path / "config"
    config.mkdir()
    (config / "username.json").write_text(json.dumps(["alice"]))
    (config / "input.json").write_text(json.dumps({
        "outputs": {"sqlite": True}, "sqlite_path": str(tmp_path / "x.db"), "log_dir": str(tmp_path / "logs")}))
    monkeypatch.setenv("X_BEARER_TOKEN", "t")

    def boom(*args, **kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(run_daily, "run", boom)
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        assert run_daily.main(["--config", str(config)]) == 2
    finally:
        for handler in root.handlers[len(before):]:
            handler.close()
            root.removeHandler(handler)
    log_text = (tmp_path / "logs" / "run.log").read_text(encoding="utf-8")
    assert "Unexpected error" in log_text and "ValueError: boom" in log_text and "Traceback" in log_text
