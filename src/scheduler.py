"""Run the monitor on a fixed interval (default every 60 minutes) with APScheduler.

    python -m src.scheduler                        # runs until you stop it (Ctrl+C)
    python -m src.scheduler --config D:\\other\\config

Each run is started as its own process (`python -m src.run_daily`), so:
- config/username.json and config/input.json are re-read on every run,
- a crash or hang inside one run cannot take the scheduler down,
- each run logs to logs/run.log exactly as when started by hand.
The scheduler's own events (run started/ended, exit code, skipped runs) go to
logs/scheduler.log. When it reaches scheduler_log_max_mb (default 10 MB) it is rolled
over to scheduler.log.1 (older copies shift up; scheduler_log_backups, default 5, are kept).

A run stops itself cleanly after run_time_limit_minutes (default 50); a run that is
still going after scheduler_kill_after_minutes (default 55) is killed, so it can never
block the next scheduled run. A run that is still going when the next one is due is
NOT started twice: the due run is skipped (max_instances=1). If the PC was asleep and several runs were
missed, only one catch-up run is made (coalesce=True). The run itself also holds
an OS file lock (RunLock in run_daily.py) as a second guard.
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import PROJECT_ROOT, ConfigError, Settings, load_settings
from .logfiles import MB
from .run_daily import new_run_id

log = logging.getLogger("x_monitor.scheduler")

JOB_ID = "x_monitor_run"
SCHEDULER_LOG_FILE = "scheduler.log"
EXIT_MEANING = {
    0: "ok",
    1: "finished with some errors, see the run log",
    2: "stopped: config/token/credits problem or another run in progress, see the run log",
}


def setup_logging(log_dir: Path, max_bytes: int = 10 * MB, backups: int = 5) -> None:
    """The scheduler is the only writer of scheduler.log and its lines are one-liners, so the
    standard size-based rollover (checked on every write) is used here."""
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    handlers: list[logging.Handler] = [RotatingFileHandler(
        log_dir / SCHEDULER_LOG_FILE, maxBytes=max_bytes, backupCount=backups, encoding="utf-8")]
    if sys.stdout is not None:  # None when started without a console (pythonw)
        handlers.append(logging.StreamHandler(sys.stdout))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in handlers:
        handler.setFormatter(fmt)
        root.addHandler(handler)


KILLED_EXIT_CODE = -9


def run_command(config_dir: str | None, run_id: str | None = None) -> list[str]:
    cmd = [sys.executable, "-m", "src.run_daily"]
    if config_dir:
        cmd += ["--config", config_dir]
    if run_id:
        cmd += ["--run-id", run_id]
    return cmd


def run_once(config_dir: str | None, kill_after_seconds: float | None = None) -> int:
    """Start one monitor run as a child process and wait for it. Returns its exit code.

    Backstop for a run that is stuck in a way it cannot notice itself: after `kill_after_seconds`
    the child is killed. That is safe: the OS releases the run lock, the databases roll back any
    open transaction, and the JSON output trims unfinished lines on its next start.
    """
    # Without a console there is nowhere to print; the run still writes its own log file.
    output = None if sys.stdout is not None else subprocess.DEVNULL
    # The scheduler picks the run id and hands it to the run, so the same id appears here in
    # scheduler.log and on every line the run writes to run.log.
    run_id = new_run_id()

    log.info("Run %s starting.", run_id)
    started = time.monotonic()
    try:
        code = subprocess.run(run_command(config_dir, run_id), cwd=PROJECT_ROOT, stdout=output, stderr=output,
                              check=False, timeout=kill_after_seconds).returncode
    except subprocess.TimeoutExpired:   # subprocess.run has already killed and reaped the child
        log.error("Run %s killed after %.0f min (time limit): it was stuck. The next run starts on schedule "
                  "and continues where this one stopped.", run_id, (time.monotonic() - started) / 60)
        return KILLED_EXIT_CODE
    except OSError as exc:
        log.error("Run %s could not be started: %s", run_id, exc)
        return -1
    log.info("Run %s ended after %.0f s: exit code %d (%s).", run_id, time.monotonic() - started, code,
             EXIT_MEANING.get(code, "unexpected exit code, see the run log"))
    return code


def build_scheduler(settings: Settings, config_dir: str | None) -> BlockingScheduler:
    scheduler = BlockingScheduler()
    job_options = {
        "trigger": IntervalTrigger(minutes=settings.schedule_interval_minutes),
        "args": [config_dir, settings.scheduler_kill_after_minutes * 60],
        "id": JOB_ID,
        "name": "X monitor run",
        "max_instances": 1,          # never two runs at once; a due run is skipped instead
        "coalesce": True,            # after sleep/downtime, missed runs collapse into one
        "misfire_grace_time": None,  # a late run (e.g. after the PC wakes up) still runs
        "replace_existing": True,
    }
    if settings.schedule_run_on_start:
        job_options["next_run_time"] = datetime.now().astimezone()
    scheduler.add_job(run_once, **job_options)
    return scheduler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the X monitor on a fixed interval")
    parser.add_argument("--config", default=None, help="config folder (default: ./config)")
    args = parser.parse_args(argv)
    # Absolute path, because each run is started with the project folder as working directory.
    config_dir = str(Path(args.config).resolve()) if args.config else None

    try:
        settings = load_settings(config_dir)
    except ConfigError as exc:
        logging.basicConfig()
        log.error("%s", exc)
        return 2
    setup_logging(settings.log_dir, settings.scheduler_log_max_mb * MB, settings.scheduler_log_backups)

    scheduler = build_scheduler(settings, config_dir)
    log.info("Scheduler started: a run every %d minute(s)%s. Press Ctrl+C to stop.",
             settings.schedule_interval_minutes,
             ", first run now" if settings.schedule_run_on_start else "")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        log.info("Scheduler stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
