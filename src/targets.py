"""The accounts to collect: from username.json, or from the monitor_targets table (usernames_source).

monitor_targets (in the pipeline database, DB_* in .env) is shared with other pipelines; this program reads
the rows with platform 'twitter' and target_type 'username' (any group_name), and writes only one thing:
external_id = the account's X user id, where that is still empty.

Reading at the start of each run, in this order:
    1. the table                                       -> its enabled usernames; the list is saved in SQLite
    2. table unreadable: the list saved at the last successful read   (warning)
    3. no saved list yet: username.json                                (warning)
    4. none of these: the run stops                                    (error)
The enabled rows' usernames are cleaned like username.json's: a leading @ removed; invalid names and duplicates
(ignoring letter case; the table's unique rule does not catch them when group_name is empty) are skipped with
a warning. Disabled rows are ignored entirely.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime

from .config import Settings, parse_username_entries
from .storage.pipeline import PipelineDB
from .storage.sql_store import SQLiteStore

log = logging.getLogger("x_monitor")

PLATFORM, TARGET_TYPE = "twitter", "username"


@dataclass
class AccountList:
    usernames: list[str]
    source: str                                     # where the list came from, for run.log
    warnings: list[str] = field(default_factory=list)   # skipped entries (invalid, duplicate, empty)
    warning_prefix: str = "username.json"           # what the entry labels ("id 3") refer to
    rows: list[dict] = field(default_factory=list)  # the table rows read (empty when not read this run)


def read_target_rows(db: PipelineDB) -> list[dict]:
    """This platform's username rows, in collection order (sort_order, then id)."""
    cols = ["id", "value", "enabled", "sort_order", "external_id"]
    rows = db.conn.execute(f"SELECT {', '.join(cols)} FROM monitor_targets WHERE platform = %s AND target_type = %s "
                           f"ORDER BY sort_order, id", (PLATFORM, TARGET_TYPE)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def clean_name(value: str | None) -> str:
    """A handle as the table may hold it ("@BBCWorld ", "BBCWorld") -> "BBCWorld"."""
    return str(value or "").strip().lstrip("@")


def load_accounts(settings: Settings, sqlite: SQLiteStore | None, now: datetime, save: bool = True) -> AccountList | None:
    """The account list for this run, or None if there is none to use (the run must stop)."""
    if settings.usernames_source != "database":
        return AccountList(list(settings.usernames), "username.json", list(settings.config_warnings))

    db = PipelineDB(settings.pipeline_db)
    try:
        db.open()
        rows = read_target_rows(db)
    except Exception as exc:  # noqa: BLE001 - fall back below
        return _fallback(settings, sqlite, db, exc)
    finally:
        db.close()

    # Only enabled rows are checked: a disabled row is switched off on purpose, so it never causes a warning
    # and never counts as a duplicate of an enabled row with the same name.
    usernames, warnings = parse_username_entries(
        [{"id": r["id"], "username": clean_name(r["value"]), "enabled": True} for r in rows if r["enabled"]])
    if not usernames:
        log.error("Account list: monitor_targets on %s has no enabled %s %s rows; nothing to collect",
                  db.describe(), PLATFORM, TARGET_TYPE)
        return None
    if save and sqlite is not None:
        try:
            sqlite.save_target_list(db.target, usernames, now)
        except Exception as exc:  # noqa: BLE001 - only the fallback copy is affected
            log.warning("Account list: could not save it in SQLite as the fallback (%s)", exc)
    return AccountList(usernames, f"monitor_targets ({db.describe()})", warnings, "monitor_targets", rows)


def _fallback(settings: Settings, sqlite: SQLiteStore | None, db: PipelineDB, exc: Exception) -> AccountList | None:
    reason = f"{type(exc).__name__}: {exc}"
    saved = None
    if sqlite is not None:
        try:
            saved = sqlite.load_target_list(db.target)
        except Exception:  # noqa: BLE001
            saved = None
    if saved:
        usernames, read_at = saved
        log.warning("Account list: monitor_targets on %s could not be read (%s); using the list saved at its last "
                    "read, %s (%d accounts)", db.describe(), reason, read_at.isoformat(), len(usernames))
        return AccountList(usernames, f"the saved list of {read_at.isoformat()}")
    if settings.file_usernames:
        log.warning("Account list: monitor_targets on %s could not be read (%s) and no list has been saved yet; "
                    "using username.json (%d accounts)", db.describe(), reason, len(settings.file_usernames))
        return AccountList(list(settings.file_usernames), "username.json (fallback)", list(settings.config_warnings))
    problem = f" ({settings.file_problem})" if settings.file_problem else ""
    log.error("Account list: monitor_targets on %s could not be read (%s), no list has been saved yet and "
              "username.json has no usable accounts%s; nothing to collect", db.describe(), reason, problem)
    return None


def fill_external_ids(settings: Settings, rows: list[dict], user_ids: dict[str, str]) -> int:
    """Write each account's X user id into monitor_targets.external_id where it is still empty.
    `user_ids`: lower(username as listed) -> user_id. Returns the number of rows filled."""
    pending = [(r["id"], user_ids[clean_name(r["value"]).lower()]) for r in rows
               if not r.get("external_id") and clean_name(r["value"]).lower() in user_ids]
    if not pending:
        return 0
    db = PipelineDB(settings.pipeline_db)
    filled = 0
    try:
        db.open()
        with db.conn.transaction():
            for row_id, user_id in pending:
                filled += db.conn.execute(
                    "UPDATE monitor_targets SET external_id = %s, updated_at = now() "
                    "WHERE id = %s AND (external_id IS NULL OR external_id = '')", (user_id, row_id)).rowcount
    finally:
        db.close()
    return filled
