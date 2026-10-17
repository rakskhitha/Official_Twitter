"""Real credits used per run, from X's balance (GET /2/usage/credits) before and after the run."""

import json
import logging
import sqlite3
import time

import pytest

from src.credits import CreditBalance, credits_used, read_credit_balance
from src.run_daily import run
from src.x_client import FatalApiError, XClient


def last_run(settings):
    conn = sqlite3.connect(settings.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute("SELECT * FROM runs ORDER BY rowid DESC LIMIT 1").fetchone())
    finally:
        conn.close()


def last_json_run(settings):
    return json.loads((settings.json_dir / "_runs.jsonl").read_text(encoding="utf-8").splitlines()[-1])


# --------------------------------------------------------------------------- end to end
def test_credits_used_is_measured_and_logged(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    for i in range(1, 4):
        fake_client.add_post("1", str(100 + i))
    settings = make_settings(["alice", "bob"])

    with caplog.at_level(logging.INFO, logger="x_monitor"):
        assert run(settings, client=fake_client) == 0

    row = last_run(settings)                     # 2 users x $0.010 + 3 posts x $0.005 = $0.035
    assert row["credits_before"] == 100.0 and row["credits_after"] == pytest.approx(99.965)
    assert row["credits_used"] == pytest.approx(0.035)
    assert row["credits_used"] == pytest.approx(row["est_cost_usd"])
    assert last_json_run(settings)["credits_used"] == pytest.approx(0.035)       # JSON summary too
    assert fake_client.credit_calls == 2                                          # start + end

    assert "Credit balance at start: $100.000 (prepaid $100.000, free $0.000)." in caplog.text
    assert "Credit balance at end: $99.965" in caplog.text
    assert "est. cost $0.035, credits used $0.035, balance $99.965, errors=0" in caplog.text


def test_dedup_shows_up_as_lower_real_cost(fake_client, make_settings):
    """X does not charge the same post twice in a UTC day; the real figure then differs from the estimate."""
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "101")
    settings = make_settings(["alice"])
    original = fake_client.get

    def get_with_dedup(path, params=None):
        before = fake_client.credit_balance
        body = original(path, params)
        if path.endswith("/tweets"):
            fake_client.credit_balance = before                 # pretend X deduplicated these posts
        return body

    fake_client.get = get_with_dedup
    run(settings, client=fake_client)
    row = last_run(settings)
    assert row["est_cost_usd"] == pytest.approx(0.015) and row["credits_used"] == pytest.approx(0.010)


def test_balance_going_up_is_not_measurable(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    original = fake_client.get

    def get_with_topup(path, params=None):
        body = original(path, params)
        fake_client.credit_balance += 25.0                       # auto-recharge during the run
        return body

    fake_client.get = get_with_topup
    with caplog.at_level(logging.INFO, logger="x_monitor"):
        assert run(settings, client=fake_client) == 0           # not an error
    row = last_run(settings)
    assert row["credits_used"] is None and row["credits_after"] > row["credits_before"]
    assert "credits used not measurable (balance went up" in caplog.text


def test_unavailable_balance_never_breaks_the_run(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "101")
    settings = make_settings(["alice"])

    def broken():
        raise FatalApiError("HTTP 403 client-forbidden: your app cannot use this endpoint", 403)

    fake_client.get_credit_balance = broken
    with caplog.at_level(logging.INFO, logger="x_monitor"):
        assert run(settings, client=fake_client) == 0            # tweets still collected, exit ok
    row = last_run(settings)
    assert row["credits_before"] is None and row["credits_used"] is None and row["posts_new"] == 1
    assert "Could not read the credit balance at the start" in caplog.text
    assert "credits used unknown (balance not available), balance unknown" in caplog.text


def test_low_balance_warning(fake_client, make_settings, caplog):
    fake_client.add_user("1", "alice")
    fake_client.credit_balance = 1.5
    settings = make_settings(["alice"])                          # default warning level $2.00
    with caplog.at_level(logging.WARNING, logger="x_monitor"):
        run(settings, client=fake_client)
    assert "is below the warning level $2.00" in caplog.text

    caplog.clear()
    settings.low_credit_warning_usd = 0.0                        # 0 = no warning
    with caplog.at_level(logging.WARNING, logger="x_monitor"):
        run(settings, client=fake_client)
    assert "warning level" not in caplog.text


def test_balance_is_read_even_after_a_stopped_run(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    original = fake_client.get

    def stop(path, params=None):
        if path == "/users/by":
            raise FatalApiError("Out of credits / usage capped: credits depleted", 402)
        return original(path, params)

    fake_client.get = stop
    assert run(settings, client=fake_client) == 2
    row = last_run(settings)
    assert row["credits_before"] == 100.0 and row["credits_used"] == 0.0


# --------------------------------------------------------------------------- helpers + client
def test_credits_used_helper():
    a, b = CreditBalance(10.0, 10.0, 0.0), CreditBalance(9.75, 9.75, 0.0)
    assert credits_used(a, b) == (0.25, "$0.250")
    assert credits_used(a, None)[0] is None and credits_used(None, b)[0] is None
    assert credits_used(b, a)[0] is None


def test_read_credit_balance_parses_and_rejects_bad_responses():
    class C:
        def __init__(self, body):
            self.body = body

        def get_credit_balance(self):
            return self.body

    ok = read_credit_balance(C({"data": {"total_balance": 12.5, "prepaid_balance": 10, "free_balance": 2.5,
                                         "free_grants": []}}), "start")
    assert ok == CreditBalance(12.5, 10.0, 2.5)
    assert read_credit_balance(C({"data": {"total_balance": "7.25"}}), "start").total == 7.25
    assert read_credit_balance(C({}), "start") is None
    assert read_credit_balance(C({"data": {"total_balance": "n/a"}}), "start") is None


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.headers, self.urls = list(responses), {}, []

    def get(self, url, params=None, timeout=None):
        self.urls.append(url)
        return self.responses.pop(0)


def test_client_reads_balance_in_one_attempt_ignoring_the_run_deadline():
    session = FakeSession([FakeResponse(200, {"data": {"total_balance": 3.0}})])
    sleeps = []
    client = XClient("t", session=session, sleep=sleeps.append)
    client.deadline = time.monotonic() - 1                       # run time limit already passed
    assert client.get_credit_balance() == {"data": {"total_balance": 3.0}}
    assert session.urls == ["https://api.x.com/2/usage/credits"]


@pytest.mark.parametrize("response", [FakeResponse(429, {}, {"x-rate-limit-reset": str(int(time.time()) + 900)}),
                                      FakeResponse(503)])
def test_client_balance_call_never_waits_or_retries(response):
    session = FakeSession([response])
    sleeps = []
    client = XClient("t", session=session, sleep=sleeps.append)
    with pytest.raises(Exception):
        client.get_credit_balance()
    assert sleeps == [] and len(session.urls) == 1


# --------------------------------------------------------------------------- config + old database
def test_low_credit_warning_setting_is_validated(tmp_path):
    from src.config import ConfigError, load_settings
    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    for bad in (-1, "2", True):
        (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, "low_credit_warning_usd": bad}))
        with pytest.raises(ConfigError, match="low_credit_warning_usd"):
            load_settings(tmp_path, env_path=tmp_path / "missing.env")
    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, "low_credit_warning_usd": 5}))
    assert load_settings(tmp_path, env_path=tmp_path / "missing.env").low_credit_warning_usd == 5.0


def test_existing_database_gets_the_new_columns(tmp_path):
    """A database created before this change has no credit columns; opening it adds them."""
    from src.storage import SQLiteStore
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, "
                 "users_ok INTEGER, users_failed INTEGER, posts_new INTEGER, user_reads INTEGER, "
                 "post_reads INTEGER, est_cost_usd REAL, errors_json TEXT)")
    conn.execute("INSERT INTO runs (run_id, started_at) VALUES ('old-run', '2026-10-07T00:00:00+00:00')")
    conn.commit()
    conn.close()

    store = SQLiteStore(db)
    store.open()
    store.close()
    conn = sqlite3.connect(db)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
    assert {"credits_before", "credits_after", "credits_used"} <= columns
    assert conn.execute("SELECT run_id, credits_used FROM runs").fetchall() == [("old-run", None)]   # old row kept
    conn.close()
