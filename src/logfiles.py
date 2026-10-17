"""Size-limited log files that are always read as ONE current file (e.g. logs/run.log).

When the current file reaches its size limit it is rolled over:
    run.log.(N-1) -> run.log.N (the old run.log.N is replaced, i.e. deleted), …,
    run.log.1 -> run.log.2, run.log -> run.log.1, then a fresh run.log is started.
So run.log.1 is always the newest old file and at most N old files are kept.
"""

import os
import sys
from pathlib import Path

MB = 1024 * 1024


def backup_path(path: Path, number: int) -> Path:
    return path.with_name(f"{path.name}.{number}")


def rotate_if_needed(path: Path, max_bytes: int, backups: int) -> bool:
    """Roll `path` over if it is at least `max_bytes`. Returns True if it was rolled over.

    Called once at the start of a run, so a run's lines are never split across two files.
    If a file is locked by another program (Windows), rotation is skipped this time and
    logging simply continues in the current file; it is retried at the next run.
    """
    if backups < 1:
        raise ValueError("backups must be at least 1")
    try:
        if not path.exists() or path.stat().st_size < max_bytes:
            return False
        for number in range(backups - 1, 0, -1):
            older = backup_path(path, number)
            if older.exists():
                os.replace(older, backup_path(path, number + 1))
        os.replace(path, backup_path(path, 1))
        return True
    except OSError as exc:
        if sys.stderr is not None:
            print(f"Log rotation of {path} postponed: {exc}", file=sys.stderr)
        return False
