"""The account list from monitor_targets (src/targets.py): cleaning, the fallback order and the run using it.
The database is replaced by a stand-in here; tests/test_targets_postgres.py uses a real one."""

import json
import logging
from datetime import datetime, timezone

import pytest

from conftest import run_messages
from src import targets
from src.config import ConfigError, load_settings
from src.run_daily import run
from src.storage import SQLiteStore
from src.targets import load_accounts

T = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
DB = {"host": "h", "port": "5432", "dbname": "d", "user": "u", "password": "p"}


def row(row_id, value, enabled=True, external_id=None, sort_order=0):
    return {"id": row_id, "value": value, "enabled": enabled, "sort_order": sort_order, "external_id": external_id}


class Table:
    """Stands in for monitor_targets: `rows`, or `down` = unreachable."""

    def __init__(self, rows=(), down=False):
        self.rows, self.down = list(rows), down


@pytest.fixture
def table(monkeypatch):
    t = Table()

    class FakeDB:
        def __init__(self, config):
            self.config = config
            self.target = f"{config['host']}:{config['port']}/{config['dbname']}"

        def describe(self):
            return f"{self.config['dbname']} on {self.config['host']}:{self.config['port']}"

        def open(self):
            if t.down:
                raise ConnectionError("connection timeout expired")

        def close(self):
            pass

    monkeypatch.setattr(targets, "PipelineDB", FakeDB)
    monkeypatch.setattr(targets, "read_target_rows", lambda db: list(t.rows))
    return t


def db_settings(make_settings, file_names=()):
    settings = make_settings(list(file_names))
    settings.usernames_source = "database"
    settings.pipeline_db = dict(DB)
    settings.file_usernames = list(file_names)
    return settings


@pytest.fixture
def sqlite(tmp_path):
    store = SQLiteStore(tmp_path / "x.db")
    store.open()
    yield store
    store.close()


def test_file_source_is_unchanged(make_settings, sqlite):
    settings = make_settings(["alice", "bob"])
    accounts = load_accounts(settings, sqlite, T)
    assert (accounts.usernames, accounts.source, accounts.rows) == (["alice", "bob"], "username.json", [])


def test_table_rows_are_cleaned_like_username_json(make_settings, sqlite, table):
    table.rows = [row(1, "@BBCWorld "), row(2, "bbcworld"), row(3, "not valid!"), row(4, "off", enabled=False),
                  row(5, "dgprPaknavy"), row(6, "")]
    accounts = load_accounts(db_settings(make_settings), sqlite, T)
    assert accounts.usernames == ["BBCWorld", "dgprPaknavy"]                 # table order kept
    assert accounts.warning_prefix == "monitor_targets" and accounts.source == "monitor_targets (d on h:5432)"
    assert [w.split(":")[0] for w in accounts.warnings] == ["id 2", "id 3", "id 6"]
    assert "more than once" in accounts.warnings[0] and "not a valid X username" in accounts.warnings[1]
    assert sqlite.load_target_list("h:5432/d") == (["BBCWorld", "dgprPaknavy"], T)   # saved as the fallback


def test_disabled_rows_never_warn_and_never_block_an_enabled_row(make_settings, sqlite, table):
    table.rows = [row(1, "srilankanavyofficial", enabled=False), row(2, "@Nepal Flood", enabled=False),
                  row(3, "BBCWorld", enabled=False), row(4, "bbcworld"), row(5, "alice"), row(6, "ALICE")]
    accounts = load_accounts(db_settings(make_settings), sqlite, T)
    assert accounts.usernames == ["bbcworld", "alice"]          # the enabled bbcworld is not taken as a duplicate
    assert len(accounts.warnings) == 1 and accounts.warnings[0].startswith("id 6:")   # only enabled rows warn
    assert "more than once" in accounts.warnings[0]


def test_check_mode_does_not_save_the_list(make_settings, sqlite, table):
    table.rows = [row(1, "alice")]
    load_accounts(db_settings(make_settings), sqlite, T, save=False)
    assert sqlite.load_target_list("h:5432/d") is None


def test_unreachable_table_uses_the_saved_list_then_username_json_then_stops(make_settings, sqlite, table, caplog):
    settings = db_settings(make_settings, file_names=["from_file"])
    table.down = True
    with caplog.at_level(logging.WARNING):
        accounts = load_accounts(settings, sqlite, T)                       # nothing saved yet: the file
    assert accounts.usernames == ["from_file"] and accounts.source == "username.json (fallback)"
    assert "no list has been saved yet; using username.json (1 accounts)" in caplog.text

    table.down, table.rows = False, [row(1, "alice"), row(2, "bob")]
    load_accounts(settings, sqlite, T)                                      # a successful read is saved
    table.down = True
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        accounts = load_accounts(settings, sqlite, T)
    assert accounts.usernames == ["alice", "bob"] and accounts.rows == []   # the saved list, not the file
    assert "using the list saved at its last read, 2026-10-13T09:00:00+00:00 (2 accounts)" in caplog.text

    nothing = db_settings(make_settings)
    other = SQLiteStore(sqlite.path.with_name("empty.db"))
    other.open()
    with caplog.at_level(logging.ERROR):
        assert load_accounts(nothing, other, T) is None                     # no list anywhere: stop
    assert "nothing to collect" in caplog.text
    other.close()


def test_a_table_without_enabled_accounts_stops_the_run(make_settings, sqlite, table, caplog):
    table.rows = [row(1, "alice", enabled=False)]
    with caplog.at_level(logging.ERROR):
        assert load_accounts(db_settings(make_settings, ["x"]), sqlite, T) is None
    assert "no enabled twitter username rows" in caplog.text


def test_run_collects_the_tables_accounts(fake_client, make_settings, table, monkeypatch):
    from src import run_daily
    written = []
    monkeypatch.setattr(run_daily, "fill_external_ids",
                        lambda settings, rows, user_ids: written.append(([r["id"] for r in rows], user_ids)) or 1)
    fake_client.add_user("1", "alice")
    fake_client.add_user("2", "bob")
    fake_client.add_post("1", "100")
    table.rows = [row(10, "Alice"), row(11, "nope nope")]
    settings = db_settings(make_settings, file_names=["bob"])               # the file is not used
    assert run(settings, client=fake_client) == 0
    assert written == [([10, 11], {"alice": "1"})]                          # listed name -> X user id
    assert [p["usernames"] for path, p in fake_client.calls if path == "/users/by"] == [["alice"]]
    assert settings.usernames == ["bob"]                                    # the caller's settings unchanged
    assert run_messages(settings, "warnings") == [
        "monitor_targets id 11: 'nope nope' is not a valid X username (letters, digits, _; max 15 chars, "
        "this has 9), skipped"]
    lines = (settings.json_dir / "tweets.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0])["post_id"] == "100"


def test_run_stops_without_any_account_list(fake_client, make_settings, table):
    table.down = True
    settings = db_settings(make_settings)
    assert run(settings, client=fake_client) == 2
    assert fake_client.calls == []                                          # nothing requested from X


def write_config(folder, options, usernames=None):
    folder.mkdir()
    (folder / "input.json").write_text(json.dumps(options))
    if usernames is not None:
        (folder / "username.json").write_text(json.dumps(usernames))
    return folder


def test_config_database_source(tmp_path, monkeypatch):
    for name, value in (("DB_HOST", "h"), ("DB_PORT", "1"), ("DB_NAME", "d"), ("DB_USER", "u"), ("DB_PASSWORD", "p")):
        monkeypatch.setenv(name, value)
    env = tmp_path / "missing.env"
    options = {"usernames_source": "database", "outputs": {"sqlite": True}}
    settings = load_settings(write_config(tmp_path / "a", options), env_path=env)      # no username.json needed
    assert settings.usernames_source == "database" and settings.file_usernames == []
    assert settings.file_problem and "username.json" in settings.file_problem
    settings = load_settings(write_config(tmp_path / "b", options, ["alice", "bad name"]), env_path=env)
    assert settings.file_usernames == ["alice"]                                        # kept as the fallback

    with pytest.raises(ConfigError, match="usernames_source must be one of file, database"):
        load_settings(write_config(tmp_path / "c", {"usernames_source": "table"}, ["a"]), env_path=env)
    monkeypatch.delenv("DB_PASSWORD")
    with pytest.raises(ConfigError, match='usernames_source is "database" but these are not set in .env: DB_PASSWORD'):
        load_settings(write_config(tmp_path / "d", options), env_path=env)
