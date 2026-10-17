"""The account list against a real PostgreSQL: a copy of shaktidb's monitor_targets (same columns, defaults
and unique rule). Skipped unless TEST_POSTGRES_DSN points to a test database."""

import json
import os

import pytest

from src import import_usernames
from src.run_daily import run

DSN = os.environ.get("TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_POSTGRES_DSN not set")

SCHEMA = """
DROP TABLE IF EXISTS monitor_targets;
CREATE TABLE monitor_targets (
    id bigserial PRIMARY KEY, platform text NOT NULL, target_type text NOT NULL, value text NOT NULL, url text,
    external_id text, group_name text, enabled boolean NOT NULL DEFAULT true, sort_order integer NOT NULL DEFAULT 0,
    extra jsonb NOT NULL DEFAULT '{}'::jsonb, created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (platform, target_type, value, group_name));
"""


def pg(sql, params=None):
    import psycopg
    with psycopg.connect(DSN, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if cur.description else None


@pytest.fixture
def db_env(tmp_path, monkeypatch):
    """The test database as DB_* (in a test .env file and the environment)."""
    from psycopg.conninfo import conninfo_to_dict
    pg(SCHEMA)
    info = conninfo_to_dict(DSN)
    values = {"DB_HOST": info.get("host", "localhost"), "DB_PORT": str(info.get("port", 5432)),
              "DB_NAME": info["dbname"], "DB_USER": info.get("user", ""), "DB_PASSWORD": info.get("password", "")}
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    env = tmp_path / "test.env"
    env.write_text("\n".join(f"{k}={v}" for k, v in values.items()))
    yield env, {"host": values["DB_HOST"], "port": values["DB_PORT"], "dbname": values["DB_NAME"],
                "user": values["DB_USER"], "password": values["DB_PASSWORD"]}
    pg("DROP TABLE IF EXISTS monitor_targets")


def config_dir(tmp_path, usernames):
    folder = tmp_path / "config"
    folder.mkdir()
    (folder / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}}))
    (folder / "username.json").write_text(json.dumps(usernames))
    return folder


def twitter_rows():
    return pg("SELECT value, enabled, sort_order, extra, external_id, group_name, url FROM monitor_targets "
              "WHERE platform = 'twitter' ORDER BY sort_order, id")


def test_import_dry_run_then_apply_adds_only_missing_accounts(tmp_path, db_env, capsys):
    env, _ = db_env
    pg("INSERT INTO monitor_targets (platform, target_type, value, sort_order, extra) VALUES "
       "('twitter', 'username', '@BBCWorld', 40, '{\"src_id\": 4}'), "
       "('telegram', 'channel', 'alice', 41, '{}')")                      # another platform: not counted
    folder = config_dir(tmp_path, [{"id": 1, "username": "alice", "enabled": True},
                                   {"id": 2, "username": "bbcworld", "enabled": True},
                                   {"id": 3, "username": "off_account", "enabled": False},
                                   {"id": 4, "username": "ALICE"}, {"id": 5, "username": "bad name"}])
    args = ["--config", str(folder), "--env", str(env)]

    assert import_usernames.main(args) == 0                               # dry run
    out = capsys.readouterr().out
    assert "In both (1): bbcworld" in out and "to be added: alice, off_account (disabled)" in out
    assert "nothing written" in out and len(twitter_rows()) == 1

    assert import_usernames.main(args + ["--apply"]) == 0
    assert twitter_rows() == [
        ("@BBCWorld", True, 40, {"src_id": 4}, None, None, None),          # existing row untouched
        ("alice", True, 42, {"src_id": 1}, None, None, None),              # after the highest sort_order (41)
        ("off_account", False, 44, {"src_id": 3}, None, None, None)]
    assert import_usernames.main(args + ["--apply"]) == 0                 # again: nothing added twice
    assert len(twitter_rows()) == 3 and "to be added: -" in capsys.readouterr().out


def test_run_reads_the_table_and_fills_external_id_once(tmp_path, fake_client, make_settings, db_env):
    _, db_config = db_env
    pg("INSERT INTO monitor_targets (platform, target_type, value, sort_order, external_id) VALUES "
       "('twitter', 'username', 'alice', 1, NULL), ('twitter', 'username', '@Bob', 2, 'kept-as-is'), "
       "('twitter', 'username', 'carol', 3, NULL)")
    pg("UPDATE monitor_targets SET enabled = false WHERE value = 'carol'")
    before = dict(pg("SELECT value, updated_at FROM monitor_targets"))
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    fake_client.add_user("3", "carol")
    settings = make_settings(["from_file"])
    settings.usernames_source, settings.pipeline_db = "database", db_config

    assert run(settings, client=fake_client) == 0
    looked_up = sorted(n for path, p in fake_client.calls if path == "/users/by" for n in p["usernames"])
    assert looked_up == ["alice", "bob"]                                  # carol is disabled; not the file
    rows = {v: (e, u) for v, e, u in pg("SELECT value, external_id, updated_at FROM monitor_targets")}
    assert rows["alice"][0] == "1" and rows["alice"][1] > before["alice"]  # filled, updated_at moved
    assert rows["@Bob"] == ("kept-as-is", before["@Bob"])                 # never overwritten
    assert rows["carol"] == (None, before["carol"])                       # not collected, not touched

    pg("UPDATE monitor_targets SET external_id = NULL WHERE value = 'alice'")
    pg("UPDATE monitor_targets SET external_id = 'set by someone' WHERE value = 'alice'")
    run(settings, client=fake_client)
    assert pg("SELECT external_id FROM monitor_targets WHERE value = 'alice'") == [("set by someone",)]
