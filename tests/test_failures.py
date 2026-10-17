"""One account failing must not stop the others; many in a row means an outage (review 5, item 1.1)."""

import json
import sqlite3

import pytest

from conftest import run_messages
from src.run_daily import run
from src.x_client import FatalApiError, TransientApiError, XApiError

NAMES = [f"user{i:02d}" for i in range(1, 13)]          # 12 accounts, ids "1".."12"


def rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def setup(fake_client, make_settings, **overrides):
    for i, name in enumerate(NAMES, start=1):
        fake_client.add_user(str(i), name)
        fake_client.add_post(str(i), str(1000 + i))
    settings = make_settings(list(NAMES), **overrides)
    assert run(settings, client=fake_client) == 0          # all registered, one post each
    for i in range(1, 13):
        fake_client.add_post(str(i), str(2000 + i))        # one new post each for the next run
    fake_client.calls.clear()
    return settings


def fail_timelines(fake_client, failing_ids, exc_for):
    original = fake_client.get

    def get(path, params=None):
        if path.endswith("/tweets") and path.split("/")[2] in failing_ids:
            fake_client.calls.append((path, dict(params or {})))
            raise exc_for(path)
        return original(path, params)

    fake_client.get = get


def server_error(path):
    return TransientApiError(f"Server error 503 on {path} after 5 attempts", 503)


def not_found(path):
    return XApiError(f"HTTP 404 on {path}: Not Found", 404)


def cursors(settings):
    return dict(rows(settings, "SELECT user_id, since_id FROM monitored_users"))


def last_errors(settings):
    return run_messages(settings, "errors")


def test_one_account_with_server_errors_does_not_stop_the_others(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fail_timelines(fake_client, {"3"}, server_error)

    assert run(settings, client=fake_client) == 1                     # errors, but not stopped
    assert len(fake_client.timeline_calls()) == 12                    # every account was tried
    c = cursors(settings)
    assert c["3"] == "1003"                                           # failing one not advanced
    assert all(c[str(i)] == str(2000 + i) for i in range(1, 13) if i != 3)
    assert any("@user03" in e and "server/network" in e for e in last_errors(settings))

    fake_client.get = type(fake_client).get.__get__(fake_client)       # X healthy again
    assert run(settings, client=fake_client) == 0
    assert cursors(settings)["3"] == "2003"                           # caught up next run


def test_scattered_failures_never_trip_the_safety_stop(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=2)
    fail_timelines(fake_client, {"1", "3", "5", "7", "9", "11"}, server_error)   # every other one
    assert run(settings, client=fake_client) == 1
    assert len(fake_client.timeline_calls()) == 12


def test_consecutive_server_errors_stop_the_run_as_an_outage(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=10)
    fail_timelines(fake_client, {str(i) for i in range(1, 13)}, server_error)    # X down

    assert run(settings, client=fake_client) == 2                     # stopped
    assert len(fake_client.timeline_calls()) == 10                    # accounts 11, 12 not requested
    errors = last_errors(settings)                                    # summary still written
    assert any("10 accounts in a row failed" in e and "remaining 2 account(s)" in e for e in errors)
    assert cursors(settings) == {str(i): str(1000 + i) for i in range(1, 13)}   # nothing advanced

    fake_client.get = type(fake_client).get.__get__(fake_client)
    assert run(settings, client=fake_client) == 0                     # next run collects everyone
    assert all(cursors(settings)[str(i)] == str(2000 + i) for i in range(1, 13))


def test_unexpected_errors_also_count_toward_the_safety_stop(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=3)
    fail_timelines(fake_client, {str(i) for i in range(1, 13)}, lambda path: KeyError("broken"))
    assert run(settings, client=fake_client) == 2
    assert len(fake_client.timeline_calls()) == 3


def test_definite_per_account_errors_do_not_count(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=3)
    fail_timelines(fake_client, {str(i) for i in range(1, 13)}, not_found)       # 12 x 404
    assert run(settings, client=fake_client) == 1                     # finished, not stopped
    assert len(fake_client.timeline_calls()) == 12


def test_an_answer_from_x_resets_the_count(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=3)
    # 503, 503, 404, 503, 503, 404, ... : never 3 server errors in a row
    failing = {str(i) for i in range(1, 13)}

    def exc_for(path):
        return not_found(path) if int(path.split("/")[2]) % 3 == 0 else server_error(path)

    fail_timelines(fake_client, failing, exc_for)
    assert run(settings, client=fake_client) == 1
    assert len(fake_client.timeline_calls()) == 12


@pytest.mark.parametrize("exc", [FatalApiError("HTTP 401: bad token", 401),
                                 FatalApiError("Usage cap / spending limit reached", 429)])
def test_run_wide_problems_still_stop_immediately(fake_client, make_settings, exc):
    settings = setup(fake_client, make_settings)
    fail_timelines(fake_client, {"1"}, lambda path: exc)
    assert run(settings, client=fake_client) == 2
    assert len(fake_client.timeline_calls()) == 1


def test_server_error_in_profile_lookup_still_collects_tweets(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    original = fake_client.get

    def get(path, params=None):
        if path == "/users/by":
            raise TransientApiError("Server error 503 on /users/by after 5 attempts", 503)
        return original(path, params)

    fake_client.get = get
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1
    assert len(fake_client.timeline_calls()) == 12                    # tweets still collected
    assert {s for (s,) in rows(settings, "SELECT status FROM monitored_users")} == {"ok"}   # nothing not_found


def test_max_consecutive_failures_is_validated(tmp_path):
    from src.config import ConfigError, load_settings

    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, "max_consecutive_failures": 0}))
    with pytest.raises(ConfigError, match="positive integer"):
        load_settings(tmp_path, env_path=tmp_path / "missing.env")
    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}}))
    assert load_settings(tmp_path, env_path=tmp_path / "missing.env").max_consecutive_failures == 10


# ---- 403 handling (review 5, item 1.2)
def forbidden(path):
    from src.x_client import ForbiddenApiError
    return ForbiddenApiError(f"HTTP 403 on {path} (not-authorized-for-resource): Authorization Error", 403)


def test_403_on_one_account_does_not_stop_the_others(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fail_timelines(fake_client, {"4"}, forbidden)
    assert run(settings, client=fake_client) == 1
    assert len(fake_client.timeline_calls()) == 12
    c = cursors(settings)
    assert c["4"] == "1004" and c["5"] == "2005"                      # only @user04 not advanced
    assert any("@user04" in e and "access denied" in e for e in last_errors(settings))


def test_403s_in_a_row_stop_the_run_as_an_access_problem(fake_client, make_settings):
    settings = setup(fake_client, make_settings, max_consecutive_failures=10)
    fail_timelines(fake_client, {str(i) for i in range(1, 13)}, forbidden)
    assert run(settings, client=fake_client) == 2
    assert len(fake_client.timeline_calls()) == 10
    assert any("10 accounts in a row" in e and "app-access problem" in e for e in last_errors(settings))


def test_app_level_403_stops_the_run_immediately(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fail_timelines(fake_client, {"1"}, lambda path: FatalApiError(
        "HTTP 403 client-forbidden: your app cannot use this endpoint", 403))
    assert run(settings, client=fake_client) == 2
    assert len(fake_client.timeline_calls()) == 1
    assert any("client-forbidden" in e for e in last_errors(settings))


def test_403_in_profile_lookup(fake_client, make_settings):
    from src.x_client import ForbiddenApiError
    settings = setup(fake_client, make_settings)
    original = fake_client.get

    def account_level(path, params=None):
        if path == "/users/by":
            raise ForbiddenApiError("HTTP 403 on /users/by (no error type): Forbidden", 403)
        return original(path, params)

    fake_client.get = account_level                                    # per-request 403: tweets still collected
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1
    assert len(fake_client.timeline_calls()) == 12
    assert {s for (s,) in rows(settings, "SELECT status FROM monitored_users")} == {"ok"}

    def app_level(path, params=None):
        if path == "/users/by":
            raise FatalApiError("HTTP 403 client-forbidden: your app cannot use this endpoint", 403)
        return original(path, params)

    fake_client.get = app_level                                        # app-level 403: stop
    fake_client.calls.clear()
    assert run(settings, client=fake_client, force_profile_refresh=True) == 2
    assert fake_client.timeline_calls() == []


def test_real_client_403_classification_end_to_end(make_settings):
    """The real XClient on top of a fake HTTP session: a 403 for one account's timeline is
    isolated, while an app-level 403 stops the run."""
    from src.x_client import XClient

    class Resp:
        def __init__(self, status, body):
            self.status_code, self._body, self.headers = status, body, {}

        def json(self):
            return self._body

    class Session:
        def __init__(self, timeline_403_type):
            self.headers, self.paths, self.kind = {}, [], timeline_403_type

        def get(self, url, params=None, timeout=None):
            path = url.split("/2", 1)[1]
            self.paths.append(path)
            if path == "/users/by":
                users = [{"id": str(i), "username": f"user{i}", "protected": False} for i in (1, 2, 3)]
                return Resp(200, {"data": users})
            if path == "/users/2/tweets":
                return Resp(403, {"title": "Forbidden", "type": f"https://api.x.com/2/problems/{self.kind}"})
            return Resp(200, {"meta": {"result_count": 0}})

    session = Session("not-authorized-for-resource")
    settings = make_settings(["user1", "user2", "user3"])
    assert run(settings, client=XClient("t", session=session, sleep=lambda s: None)) == 1
    assert [p for p in session.paths if p.endswith("/tweets")] == ["/users/1/tweets", "/users/2/tweets",
                                                                   "/users/3/tweets"]

    session = Session("client-forbidden")
    assert run(settings, client=XClient("t", session=session, sleep=lambda s: None)) == 2
    assert [p for p in session.paths if p.endswith("/tweets")] == ["/users/1/tweets", "/users/2/tweets"]
