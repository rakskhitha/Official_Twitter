"""A run can never run unbounded or block later runs (review 5, item 1.4)."""

import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from conftest import run_messages
from src import run_daily
from src import scheduler as sched
from src.run_daily import run
from src.x_client import RunTimeLimitReached, XClient

NAMES = [f"user{i:02d}" for i in range(1, 13)]


class Clock:
    """Stands in for time.monotonic(); tweet requests 'take' minutes."""

    def __init__(self):
        self.now = 10_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(time, "monotonic", c)
    # distinct, increasing run start times so the run order (last attempt) is well defined
    base = datetime(2026, 10, 7, tzinfo=timezone.utc)
    ticks = iter(range(1, 1_000_000))
    monkeypatch.setattr(run_daily, "utc_now", lambda: base + timedelta(seconds=next(ticks)))
    return c


def slow_tweets(fake_client, clock, minutes_per_request):
    original = fake_client.get

    def get(path, params=None):
        result = original(path, params)
        if path.endswith("/tweets"):
            clock.now += minutes_per_request * 60
        return result

    fake_client.get = get


def rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def last_warnings(settings):
    return run_messages(settings, "warnings")


def tweet_order(fake_client):
    return [int(path.split("/")[2]) for path, _ in fake_client.calls if path.endswith("/tweets")]


def setup(fake_client, make_settings):
    for i, name in enumerate(NAMES, start=1):
        fake_client.add_user(str(i), name)
    settings = make_settings(list(NAMES))                      # default limit: 50 min
    assert run(settings, client=fake_client) == 0
    fake_client.calls.clear()
    return settings


# --------------------------------------------------------------------------- run budget
def test_time_limit_stops_cleanly_and_the_rest_go_first_next_run(fake_client, make_settings, clock):
    settings = setup(fake_client, make_settings)
    for i in range(1, 13):
        fake_client.add_post(str(i), str(2000 + i))
    slow_tweets(fake_client, clock, minutes_per_request=10)   # 10 min per account -> 5 fit in 50 min

    assert run(settings, client=fake_client) == 0             # a clean stop: a warning, not an error
    assert tweet_order(fake_client) == [1, 2, 3, 4, 5]
    saved = dict(rows(settings, "SELECT user_id, since_id FROM monitored_users"))
    assert [saved[str(i)] for i in range(1, 6)] == [str(2000 + i) for i in range(1, 6)]   # done ones saved
    assert all(saved[str(i)] is None for i in range(6, 13))
    assert any("Time limit reached (50 min" in w and "after 5 of 12 accounts" in w and "other 7" in w
               for w in last_warnings(settings))
    assert rows(settings, "SELECT status, items_loaded, items_processed FROM runs ORDER BY rowid DESC LIMIT 1") == [
        ("success", 12, 5)]

    fake_client.calls.clear()
    clock.now += 3600
    assert run(settings, client=fake_client) == 0             # again 5 accounts fit
    assert tweet_order(fake_client) == [6, 7, 8, 9, 10]       # the ones not reached go first
    fake_client.calls.clear()
    clock.now += 3600
    run(settings, client=fake_client)
    assert tweet_order(fake_client)[:2] == [11, 12]           # then the last two, then the oldest attempts


def test_failing_accounts_do_not_pile_up_at_the_front(fake_client, make_settings, clock):
    """Order is by last ATTEMPT, not last success: accounts that keep failing were attempted, so
    they are not moved to the front of every run (where 10 of them in a row would trip the
    outage stop even though X is fine)."""
    from src.x_client import TransientApiError
    settings = setup(fake_client, make_settings)
    original = fake_client.get

    def get(path, params=None):
        if path in ("/users/7/tweets", "/users/8/tweets"):
            fake_client.calls.append((path, dict(params or {})))
            raise TransientApiError(f"Server error 503 on {path}", 503)
        return original(path, params)

    fake_client.get = get
    for _ in range(2):
        fake_client.calls.clear()
        run(settings, client=fake_client)
        assert tweet_order(fake_client) == list(range(1, 13))   # 7 and 8 stay in their place


def test_time_limit_during_profile_step(fake_client, make_settings, clock):
    settings = setup(fake_client, make_settings)
    original = fake_client.get

    def get(path, params=None):
        if path == "/users/by":
            raise RunTimeLimitReached("rate limited (429): waiting 900 s would pass the run's time limit")
        return original(path, params)

    fake_client.get = get
    assert run(settings, client=fake_client, force_profile_refresh=True) == 0
    assert tweet_order(fake_client) == []
    assert any("before tweet collection started" in w for w in last_warnings(settings))


def test_gap_filling_stops_at_the_time_limit_and_continues_next_run(fake_client, make_settings, clock):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "1000")
    fake_client.page_size = 10
    settings = make_settings(["alice"], max_pages_per_user_per_run=2)
    run(settings, client=fake_client)
    for i in range(1001, 1026):
        fake_client.add_post("1", str(i))
    run(settings, client=fake_client)                          # gap recorded (cap 20 of 25)
    assert json.loads(rows(settings, "SELECT fetch_gaps FROM monitored_users")[0][0])

    fake_client.add_post("1", "1026")
    slow_tweets(fake_client, clock, minutes_per_request=60)   # normal fetch uses up the budget
    assert run(settings, client=fake_client) == 0             # time limit: a warning
    assert ("1026",) in rows(settings, "SELECT post_id FROM posts")                       # new post saved
    assert json.loads(rows(settings, "SELECT fetch_gaps FROM monitored_users")[0][0])     # gap kept

    fake_client.get = type(fake_client).get.__get__(fake_client)
    clock.now += 3600
    assert run(settings, client=fake_client) == 0
    assert json.loads(rows(settings, "SELECT fetch_gaps FROM monitored_users")[0][0]) == []
    assert len(rows(settings, "SELECT post_id FROM posts")) == 27


# --------------------------------------------------------------------------- client waits
class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.headers, self.requests = list(responses), {}, []

    def get(self, url, params=None, timeout=None):
        self.requests.append(timeout)
        return self.responses.pop(0)


def client_with(responses, seconds_left):
    sleeps = []
    session = FakeSession(responses)
    client = XClient("t", session=session, sleep=sleeps.append, max_retries=3)
    client.deadline = time.monotonic() + seconds_left
    return client, session, sleeps


def test_rate_limit_reset_after_the_deadline_stops_without_sleeping():
    reset = str(int(time.time()) + 600)
    client, _, sleeps = client_with([FakeResponse(429, {}, {"x-rate-limit-reset": reset})], seconds_left=120)
    with pytest.raises(RunTimeLimitReached, match="rate limited"):
        client.get("/users/1/tweets")
    assert sleeps == []


def test_rate_limit_reset_before_the_deadline_waits_and_retries():
    reset = str(int(time.time()) + 20)
    client, _, sleeps = client_with([FakeResponse(429, {}, {"x-rate-limit-reset": reset}),
                                     FakeResponse(200, {"data": [1]})], seconds_left=600)
    assert client.get("/x") == {"data": [1]}
    assert len(sleeps) == 1 and 15 <= sleeps[0] <= 30


def test_retry_wait_that_would_cross_the_deadline_stops_the_run():
    client, _, sleeps = client_with([FakeResponse(503), FakeResponse(200)], seconds_left=1)
    with pytest.raises(RunTimeLimitReached, match="retry after HTTP 503"):
        client.get("/x")
    assert sleeps == []


def test_no_request_starts_after_the_deadline():
    client, session, _ = client_with([FakeResponse(200)], seconds_left=-1)
    with pytest.raises(RunTimeLimitReached):
        client.get("/x")
    assert session.requests == []


def test_requests_use_separate_connect_and_read_timeouts():
    client, session, _ = client_with([FakeResponse(200)], seconds_left=600)
    client.get("/x")
    assert session.requests == [(10.0, 30.0)]


# --------------------------------------------------------------------------- scheduler backstop
def test_scheduler_kills_a_stuck_run(monkeypatch, caplog):
    monkeypatch.setattr(sched, "run_command", lambda config_dir, run_id=None: [sys.executable, "-c", "import time; time.sleep(60)"])
    started = time.perf_counter()
    import logging
    with caplog.at_level(logging.ERROR, logger="x_monitor.scheduler"):
        code = sched.run_once(None, kill_after_seconds=1)
    assert code == sched.KILLED_EXIT_CODE
    assert time.perf_counter() - started < 20
    assert re.search(r"Run \d{8}T\d{6}Z-[0-9a-f]{6} killed after", caplog.text)


def test_scheduler_passes_the_kill_limit(monkeypatch):
    seen = {}
    monkeypatch.setattr(sched.subprocess, "run",
                        lambda cmd, **kw: seen.update(kw) or sched.subprocess.CompletedProcess(cmd, 0))
    assert sched.run_once(None, kill_after_seconds=3300) == 0
    assert seen["timeout"] == 3300


# --------------------------------------------------------------------------- config + databases
def test_time_limits_must_fit_inside_the_schedule(tmp_path):
    from src.config import ConfigError, load_settings

    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    for bad in ({"run_time_limit_minutes": 55}, {"scheduler_kill_after_minutes": 60},
                {"schedule_interval_minutes": 30}, {"run_time_limit_minutes": 0}):
        (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, **bad}))
        with pytest.raises(ConfigError):
            load_settings(tmp_path, env_path=tmp_path / "missing.env")

    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}}))
    s = load_settings(tmp_path, env_path=tmp_path / "missing.env")
    assert (s.run_time_limit_minutes, s.scheduler_kill_after_minutes, s.schedule_interval_minutes) == (50, 55, 60)

    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, "run_time_limit_minutes": 10,
                                                     "scheduler_kill_after_minutes": 12,
                                                     "schedule_interval_minutes": 15}))
    assert load_settings(tmp_path, env_path=tmp_path / "missing.env").run_time_limit_minutes == 10


def test_postgres_connection_has_timeouts(monkeypatch):
    import psycopg

    from src.storage.sql_store import PostgresStore
    seen = {}
    monkeypatch.setattr(psycopg, "connect", lambda dsn, **kw: seen.update(kw, dsn=dsn) or object())
    PostgresStore("postgresql://u@h/db")._connect()
    assert seen["options"] == "-c statement_timeout=60000"
    assert seen["keepalives"] == 1 and seen["keepalives_idle"] == 30 and seen["connect_timeout"] == 15
