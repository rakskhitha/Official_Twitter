"""A config name stays linked to the account (user_id) it was first resolved to (code review B3)."""

import sqlite3

from conftest import run_messages
from src.run_daily import run
from src.x_client import XApiError


def rows(settings, sql):
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def everything_stored(settings) -> str:
    """All stored data as one string, to check that an id appears nowhere."""
    parts = [repr(rows(settings, f"SELECT * FROM {t}")) for t in ("monitored_users", "user_profiles", "posts", "media")]
    for name in ("profile.json", "tweets.jsonl", "_state.json"):
        path = settings.json_dir / name
        if path.exists():
            parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def test_renamed_account_whose_old_handle_is_taken_keeps_being_followed(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    assert run(settings, client=fake_client) == 0

    fake_client.users["1"]["username"] = "alice_renamed"          # original renames
    fake_client.add_post("1", "101")
    fake_client.add_user("9999", "alice")                         # stranger takes the freed handle
    fake_client.add_post("9999", "900", "stranger tweet")
    fake_client.calls.clear()
    assert run(settings, client=fake_client, force_profile_refresh=True) == 0   # handled: warnings only

    assert [p for p, _ in fake_client.calls if p.endswith("/tweets")] == ["/users/1/tweets"]
    assert rows(settings, "SELECT user_id, username, config_username, status FROM monitored_users") == [
        ("1", "alice_renamed", "alice", "renamed")]
    assert rows(settings, "SELECT post_id FROM posts ORDER BY post_id") == [("100",), ("101",)]
    assert "9999" not in everything_stored(settings)              # stranger stored nowhere
    warnings = run_messages(settings, "warnings")
    assert any("now belongs to a different account (id 9999)" in w for w in warnings)
    assert any("was renamed to @alice_renamed" in w for w in warnings)
    assert run_messages(settings, "errors") == []

    # Next refresh uses the new handle: no conflict any more, and the stranger is never followed.
    fake_client.calls.clear()
    assert run(settings, client=fake_client, force_profile_refresh=True) == 0
    assert [p["usernames"] for path, p in fake_client.calls if path == "/users/by"] == [["alice_renamed"]]
    assert "9999" not in everything_stored(settings)


def test_deleted_original_with_handle_taken_stops_collection_without_switching(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    del fake_client.users["1"]                                     # original account gone
    fake_client.add_user("9999", "alice")
    fake_client.add_post("9999", "900")
    fake_client.calls.clear()
    assert run(settings, client=fake_client, force_profile_refresh=True) == 0   # handled: warnings only

    assert any("Skipping its posts" in w for w in run_messages(settings, "warnings"))
    assert rows(settings, "SELECT user_id, status FROM monitored_users") == [("1", "not_found")]
    assert not [p for p, _ in fake_client.calls if p.endswith("/tweets")]   # nobody's tweets fetched
    assert "9999" not in everything_stored(settings)


def test_failed_id_lookup_during_conflict_keeps_status_and_never_switches(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["username"] = "alice_renamed"
    fake_client.add_user("9999", "alice")
    original = fake_client.get

    def get(path, params=None):
        if path == "/users/1":
            raise XApiError("HTTP 400 on /users/1: bad", 400)
        return original(path, params)

    fake_client.get = get
    fake_client.calls.clear()
    assert run(settings, client=fake_client, force_profile_refresh=True) == 1
    assert rows(settings, "SELECT user_id, username, status FROM monitored_users") == [("1", "alice", "ok")]
    assert [p for p, _ in fake_client.calls if p.endswith("/tweets")] == ["/users/1/tweets"]
    assert "9999" not in everything_stored(settings)


def test_config_name_linked_to_two_accounts_always_follows_the_original(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_user("9999", "intruder")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    # Simulate bad stored data (e.g. written by the old bug): a later account linked to 'alice'.
    conn = sqlite3.connect(settings.sqlite_path)
    conn.execute("INSERT INTO monitored_users (user_id, config_username, username, active, status, added_at) "
                 "VALUES ('9999', 'alice', 'intruder', 1, 'ok', '2099-01-01T00:00:00+00:00')")
    conn.commit()
    conn.close()

    for _ in range(3):                                             # same choice on every run
        fake_client.calls.clear()
        assert run(settings, client=fake_client) == 1
        assert [p for p, _ in fake_client.calls if p.endswith("/tweets")] == ["/users/1/tweets"]
    assert any("linked to more than one account" in e and "id 1)" in e for e in run_messages(settings))


def test_two_rows_for_the_same_account_are_collected_once(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    # The account renames and the user adds the new handle as a second row, keeping the old one.
    fake_client.users["1"]["username"] = "alice_renamed"
    settings.usernames = ["alice", "alice_renamed"]
    fake_client.calls.clear()
    run(settings, client=fake_client, force_profile_refresh=True)

    assert rows(settings, "SELECT user_id, config_username FROM monitored_users") == [("1", "alice")]
    assert [p for p, _ in fake_client.calls if p.endswith("/tweets")] == ["/users/1/tweets"]


def test_replacing_the_row_with_the_new_handle_relinks_the_same_account(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    fake_client.add_post("1", "100")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["username"] = "alice_renamed"
    settings.usernames = ["alice_renamed"]                         # old row removed, new row added
    fake_client.add_post("1", "101")
    assert run(settings, client=fake_client) == 0

    assert rows(settings, "SELECT user_id, config_username, since_id FROM monitored_users") == [
        ("1", "alice_renamed", "101")]                             # same account, cursor kept


def test_user_reads_count_every_billed_profile(fake_client, make_settings):
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    run(settings, client=fake_client)

    fake_client.users["1"]["username"] = "alice_renamed"
    fake_client.add_user("9999", "alice")
    run(settings, client=fake_client, force_profile_refresh=True)
    # stranger returned by /users/by (billed, not kept) + original found by id = 2 reads
    assert rows(settings, "SELECT user_reads FROM runs ORDER BY rowid DESC LIMIT 1") == [(2,)]
