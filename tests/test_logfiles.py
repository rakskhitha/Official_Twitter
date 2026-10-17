"""One current log file each (logs/run.log, logs/scheduler.log) with a size limit and N old copies."""

import json
import re
import logging
from logging.handlers import RotatingFileHandler

import pytest

from src import run_daily
from src import scheduler as sched
from src.logfiles import backup_path, rotate_if_needed


@pytest.fixture
def clean_root_logging():
    """setup_logging() adds handlers to the root logger; remove and close them after the test."""
    root = logging.getLogger()
    before, level = list(root.handlers), root.level
    yield
    for handler in root.handlers[:]:
        if handler not in before:
            handler.close()
            root.removeHandler(handler)
    root.setLevel(level)


def write(path, size, fill="x"):
    path.write_text(fill * size, encoding="utf-8")


def test_no_rotation_below_the_limit_or_without_a_file(tmp_path):
    path = tmp_path / "run.log"
    assert rotate_if_needed(path, 100, 5) is False            # no file yet
    write(path, 99)
    assert rotate_if_needed(path, 100, 5) is False
    assert path.read_text() == "x" * 99 and not backup_path(path, 1).exists()


def test_rotation_moves_current_to_1_and_shifts_older_copies(tmp_path):
    path = tmp_path / "run.log"
    write(path, 100, "a")
    assert rotate_if_needed(path, 100, 5) is True
    assert not path.exists() and backup_path(path, 1).read_text() == "a" * 100

    write(path, 100, "b")
    rotate_if_needed(path, 100, 5)
    assert backup_path(path, 1).read_text() == "b" * 100      # .1 is always the newest old copy
    assert backup_path(path, 2).read_text() == "a" * 100


def test_only_the_configured_number_of_old_copies_is_kept(tmp_path):
    path = tmp_path / "run.log"
    for i in range(8):                                        # 8 rollovers, 3 backups allowed
        write(path, 10, str(i))
        rotate_if_needed(path, 10, 3)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["run.log.1", "run.log.2", "run.log.3"]
    assert [backup_path(path, n).read_text()[0] for n in (1, 2, 3)] == ["7", "6", "5"]   # oldest dropped


def test_backups_must_be_at_least_one(tmp_path):
    with pytest.raises(ValueError):
        rotate_if_needed(tmp_path / "run.log", 10, 0)


def test_runs_append_to_one_run_log_and_rotate_only_at_run_start(tmp_path, clean_root_logging):
    logs = tmp_path / "logs"
    log = logging.getLogger("x_monitor")

    def one_run(text):
        run_daily.setup_logging(logs, verbose=False, max_bytes=200, backups=2)
        for i in range(5):                                    # a "run" writing several lines
            log.info("%s line %d", text, i)
        for handler in logging.getLogger().handlers[:]:       # run ends: process exits
            if isinstance(handler, logging.FileHandler):
                handler.close()
                logging.getLogger().removeHandler(handler)

    one_run("first")                                          # > 200 bytes, but no mid-run rotation
    current = (logs / "run.log").read_text(encoding="utf-8")
    assert current.count("first line") == 5 and not (logs / "run.log.1").exists()

    one_run("second")                                         # starts over the limit -> rotated first
    assert (logs / "run.log.1").read_text(encoding="utf-8").count("first line") == 5
    second = (logs / "run.log").read_text(encoding="utf-8")
    assert second.count("second line") == 5 and "first line" not in second   # each run kept whole


def test_main_uses_the_configured_run_log_limits(tmp_path, monkeypatch, clean_root_logging):
    config = tmp_path / "config"
    config.mkdir()
    (config / "username.json").write_text(json.dumps(["alice"]))
    (config / "input.json").write_text(json.dumps({
        "outputs": {"sqlite": True}, "sqlite_path": str(tmp_path / "x.db"), "log_dir": str(tmp_path / "logs"),
        "log_max_mb": 1, "log_backups": 2}))
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "run.log").write_bytes(b"x" * (1024 * 1024))     # exactly 1 MB -> rotate at start
    seen = {}
    monkeypatch.setattr(run_daily, "check", lambda settings: seen.setdefault("ok", 0))
    assert run_daily.main(["--config", str(config), "--check"]) == 0
    assert (logs / "run.log.1").stat().st_size == 1024 * 1024
    assert "username.json: 1 entries" in (logs / "run.log").read_text(encoding="utf-8")


def test_scheduler_log_is_size_limited(tmp_path, clean_root_logging):
    sched.setup_logging(tmp_path, max_bytes=300, backups=2)
    [handler] = [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]
    assert handler.baseFilename.endswith("scheduler.log")
    assert handler.maxBytes == 300 and handler.backupCount == 2

    for i in range(40):
        sched.log.info("scheduler line %03d", i)
    handler.flush()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["scheduler.log", "scheduler.log.1", "scheduler.log.2"]
    assert "scheduler line 039" in (tmp_path / "scheduler.log").read_text(encoding="utf-8")


def test_log_settings_are_validated(tmp_path):
    from src.config import ConfigError, load_settings

    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    for bad in ({"log_max_mb": 0}, {"log_backups": 0}, {"scheduler_log_max_mb": "10"}, {"scheduler_log_backups": True}):
        (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, **bad}))
        with pytest.raises(ConfigError, match="positive integer"):
            load_settings(tmp_path, env_path=tmp_path / "missing.env")

    (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}}))
    settings = load_settings(tmp_path, env_path=tmp_path / "missing.env")
    assert (settings.log_max_mb, settings.log_backups) == (50, 5)
    assert (settings.scheduler_log_max_mb, settings.scheduler_log_backups) == (10, 5)


# ---- run id on every line
def write_config(tmp_path):
    config = tmp_path / "config"
    config.mkdir(exist_ok=True)
    (config / "username.json").write_text(json.dumps(["alice"]))
    (config / "input.json").write_text(json.dumps({
        "outputs": {"sqlite": True}, "sqlite_path": str(tmp_path / "x.db"), "log_dir": str(tmp_path / "logs")}))
    return config


def test_every_line_carries_the_run_id_including_other_loggers(tmp_path, clean_root_logging):
    run_daily.setup_logging(tmp_path, verbose=False, run_id="20261007T060736Z-30d73f")
    logging.getLogger("x_monitor").info("from the run")
    logging.getLogger("src.users").warning("from a module")
    logging.getLogger("urllib3").warning("from a library")
    lines = (tmp_path / "run.log").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert all("[20261007T060736Z-30d73f]" in line for line in lines)


def test_main_uses_one_run_id_for_all_lines_and_the_run(tmp_path, monkeypatch, clean_root_logging):
    config = write_config(tmp_path)
    monkeypatch.setenv("X_BEARER_TOKEN", "t")
    seen = {}

    def fake_run(settings, force, run_id=None):
        seen["run_id"] = run_id
        logging.getLogger("x_monitor").info("doing the run")
        return 0

    monkeypatch.setattr(run_daily, "run", fake_run)
    assert run_daily.main(["--config", str(config)]) == 0
    lines = (tmp_path / "logs" / "run.log").read_text(encoding="utf-8").splitlines()
    assert any("username.json" in line for line in lines)                   # a setup line, before the run
    assert all(f"[{seen['run_id']}]" in line for line in lines)
    assert re.match(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$", seen["run_id"])

    assert run_daily.main(["--config", str(config), "--run-id", "20261007T000000Z-abc123"]) == 0
    assert seen["run_id"] == "20261007T000000Z-abc123"                      # id from the scheduler is used


def test_invalid_run_id_is_rejected(tmp_path):
    with pytest.raises(SystemExit):
        run_daily.main(["--config", str(write_config(tmp_path)), "--run-id", "bad id; rm"])


def test_check_lines_are_labelled_check(tmp_path, monkeypatch, clean_root_logging):
    config = write_config(tmp_path)
    monkeypatch.setenv("X_BEARER_TOKEN", "t")
    assert run_daily.main(["--config", str(config), "--check"]) == 0
    lines = (tmp_path / "logs" / "run.log").read_text(encoding="utf-8").splitlines()
    assert lines and all("[check]" in line for line in lines)


def test_run_summary_uses_the_given_run_id(fake_client, make_settings):
    import sqlite3
    fake_client.add_user("1", "alice")
    settings = make_settings(["alice"])
    run_daily.run(settings, client=fake_client, run_id="20261007T000000Z-abc123")
    conn = sqlite3.connect(settings.sqlite_path)
    assert conn.execute("SELECT run_id FROM runs").fetchall() == [("20261007T000000Z-abc123",)]
    conn.close()
