"""user_profiles keeps one row per account (the latest profile); changes to tracked fields are
appended to its `changes` list. profile.json mirrors this per user."""

import json
import logging
import sqlite3

from src.run_daily import run


def sqlite_profile(settings, user_id="1") -> dict:
    conn = sqlite3.connect(settings.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM user_profiles WHERE user_id = ?", (user_id,)).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1                                   # always exactly one row per account
    row = dict(rows[0])
    row["changes"] = json.loads(row["changes"])
    return row


def json_profile(settings, user_id="1") -> dict:
    data = json.loads((settings.json_dir / "profile.json").read_text(encoding="utf-8"))
    return next(u for u in data["users"] if u["user_id"] == user_id)


def summary(changes):
    return [(c["field"], c["old"], c["new"]) for c in changes]


def setup(fake_client, make_settings, **profile):
    fake_client.add_user("1", "alice", description="old bio", verified=True, **profile)
    settings = make_settings(["alice"])
    assert run(settings, client=fake_client) == 0
    return settings


def test_first_save_has_no_changes(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    row = sqlite_profile(settings)
    assert row["changes"] == [] and row["last_changed_at"] is None
    assert row["first_seen_at"] == row["refreshed_at"] and row["status"] == "ok"
    entry = json_profile(settings)
    assert entry["changes"] == [] and entry["last_changed_at"] is None and entry["first_seen_at"]


def test_bio_change_updates_the_row_and_is_logged_in_both_outputs(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["description"] = "new bio"
    run(settings, client=fake_client, force_profile_refresh=True)

    row = sqlite_profile(settings)
    assert row["description"] == "new bio"
    assert summary(row["changes"]) == [("description", "old bio", "new bio")]
    assert row["last_changed_at"] == row["refreshed_at"]
    entry = json_profile(settings)
    assert entry["profile"]["description"] == "new bio"
    assert entry["changes"] == row["changes"]                # identical in SQLite and JSON
    assert entry["last_changed_at"] == row["last_changed_at"]


def test_follower_count_changes_are_not_logged(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["public_metrics"]["followers_count"] = 99999
    run(settings, client=fake_client, force_profile_refresh=True)

    row = sqlite_profile(settings)
    assert row["followers_count"] == 99999 and row["changes"] == [] and row["last_changed_at"] is None
    assert json_profile(settings)["changes"] == []


def test_lost_verification_is_logged(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["verified"] = False
    run(settings, client=fake_client, force_profile_refresh=True)
    assert summary(sqlite_profile(settings)["changes"]) == [("verified", True, False)]
    assert summary(json_profile(settings)["changes"]) == [("verified", True, False)]


def test_rename_is_logged_with_old_and_new_handle(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["username"] = "alice_new"
    run(settings, client=fake_client, force_profile_refresh=True)

    expected = [("username", "alice", "alice_new"), ("status", "ok", "renamed")]
    assert summary(sqlite_profile(settings)["changes"]) == expected
    assert summary(json_profile(settings)["changes"]) == expected


def test_account_disappearing_is_logged_as_status_change(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    del fake_client.users["1"]
    run(settings, client=fake_client, force_profile_refresh=True)

    row = sqlite_profile(settings)
    assert row["status"] == "not_found"
    assert summary(row["changes"]) == [("status", "ok", "not_found")]
    assert row["last_changed_at"] is not None
    assert summary(json_profile(settings)["changes"]) == [("status", "ok", "not_found")]


def test_no_change_and_repeated_refresh_add_nothing(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["description"] = "new bio"
    run(settings, client=fake_client, force_profile_refresh=True)
    first = sqlite_profile(settings)

    run(settings, client=fake_client, force_profile_refresh=True)   # same data, same day
    run(settings, client=fake_client, force_profile_refresh=True)
    again = sqlite_profile(settings)
    assert again["changes"] == first["changes"] and len(again["changes"]) == 1
    assert again["last_changed_at"] == first["last_changed_at"]
    assert len(json_profile(settings)["changes"]) == 1


def test_changes_accumulate_across_refreshes(fake_client, make_settings):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["description"] = "bio 2"
    run(settings, client=fake_client, force_profile_refresh=True)
    fake_client.users["1"]["name"] = "Alice B."
    run(settings, client=fake_client, force_profile_refresh=True)
    fake_client.users["1"]["description"] = "bio 3"
    run(settings, client=fake_client, force_profile_refresh=True)

    expected = [("description", "old bio", "bio 2"), ("name", "Alice", "Alice B."), ("description", "bio 2", "bio 3")]
    assert summary(sqlite_profile(settings)["changes"]) == expected
    assert summary(json_profile(settings)["changes"]) == expected


def test_changes_are_written_to_the_run_log(fake_client, make_settings, caplog):
    settings = setup(fake_client, make_settings)
    fake_client.users["1"]["description"] = "new bio"
    fake_client.users["1"]["location"] = "Paris"
    with caplog.at_level(logging.INFO, logger="x_monitor"):
        run(settings, client=fake_client, force_profile_refresh=True)
    assert "@alice profile changed: description, location" in caplog.text
