import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import pytest

from src import scheduler as sched
from src.run_daily import RunLock


def test_job_runs_hourly_never_overlaps_and_starts_now(make_settings):
    settings = make_settings(["alice"])
    scheduler = sched.build_scheduler(settings, None)
    [job] = scheduler.get_jobs()

    assert job.id == sched.JOB_ID and job.func is sched.run_once
    assert job.trigger.interval == timedelta(minutes=60)
    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.misfire_grace_time is None
    assert abs((job.next_run_time - datetime.now().astimezone()).total_seconds()) < 5


def test_interval_and_run_on_start_come_from_config(make_settings):
    settings = make_settings(["alice"], schedule_interval_minutes=15, schedule_run_on_start=False)
    [job] = sched.build_scheduler(settings, "D:/cfg").get_jobs()
    assert job.trigger.interval == timedelta(minutes=15)
    assert job.args == ("D:/cfg", 55 * 60)               # kill-after (default 55 min) passed to the run
    assert not hasattr(job, "next_run_time") or job.next_run_time is None  # first run after one interval


def test_run_once_starts_run_daily_as_child_process(monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"], seen["cwd"] = cmd, kwargs["cwd"]
        return subprocess.CompletedProcess(cmd, 1)

    monkeypatch.setattr(sched.subprocess, "run", fake_run)
    assert sched.run_once("D:/cfg") == 1
    assert seen["cmd"][:5] == [sys.executable, "-m", "src.run_daily", "--config", "D:/cfg"]
    assert seen["cmd"][5] == "--run-id" and re.match(r"^\d{8}T\d{6}Z-[0-9a-f]{6}$", seen["cmd"][6])
    assert seen["cwd"] == sched.PROJECT_ROOT


def test_run_once_survives_a_process_that_cannot_start(monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("no python")

    monkeypatch.setattr(sched.subprocess, "run", broken)
    assert sched.run_once(None) == -1


def test_schedule_options_validated(tmp_path):
    import json

    from src.config import ConfigError, load_settings

    (tmp_path / "username.json").write_text(json.dumps(["alice"]))
    for bad in ({"schedule_interval_minutes": 0}, {"schedule_interval_minutes": True},
                {"schedule_run_on_start": "yes"}):
        (tmp_path / "input.json").write_text(json.dumps({"outputs": {"sqlite": True}, **bad}))
        with pytest.raises(ConfigError):
            load_settings(tmp_path, env_path=tmp_path / "missing.env")


def test_run_lock_blocks_a_second_run_and_is_reusable(tmp_path):
    path = tmp_path / ".run.lock"
    with RunLock(path):
        with pytest.raises(RuntimeError, match="Another run is in progress"):
            with RunLock(path):
                pass
    with RunLock(path):  # released after the first run ended
        pass


def test_run_lock_released_when_process_is_killed(tmp_path):
    path = tmp_path / ".run.lock"
    holder = subprocess.Popen([sys.executable, "-c", (
        "import sys, time; sys.path.insert(0, sys.argv[2]);"
        "from pathlib import Path; from src.run_daily import RunLock;"
        "lock = RunLock(Path(sys.argv[1])); lock.__enter__(); print('locked', flush=True); time.sleep(60)"),
        str(path), str(sched.PROJECT_ROOT)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "locked"
        with pytest.raises(RuntimeError):
            with RunLock(path):
                pass
    finally:
        holder.kill()   # simulate a crash: no clean exit, lock file left on disk
        holder.wait()

    deadline = time.time() + 5
    while True:
        try:
            with RunLock(path):
                break
        except RuntimeError:
            if time.time() > deadline:
                raise
            time.sleep(0.1)
