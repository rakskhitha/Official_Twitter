"""Copy the accounts in config/username.json into the monitor_targets table (one time).

Usage:
    python -m src.import_usernames            # dry run: compares the file and the table, writes nothing
    python -m src.import_usernames --apply    # adds the file's accounts the table does not have yet

The database is the one in DB_* (.env). A name already in the table (ignoring letter case and a leading @)
is never added again; existing rows are never changed. New rows follow the existing twitter rows: platform
'twitter', target_type 'username', group_name and url empty, extra {"src_id": <username.json id>},
enabled as in the file, and sort_order after the table's highest, in the file's order.
"""

import argparse
import json
import sys
from pathlib import Path

from psycopg.types.json import Jsonb

from .config import PROJECT_ROOT, USERNAME_FILE, USERNAME_RE, ConfigError, load_settings
from .storage.pipeline import PipelineDB
from .targets import PLATFORM, TARGET_TYPE, clean_name


def file_entries(path) -> tuple[list[dict], list[str]]:
    """username.json as [{"src_id", "username", "enabled"}] (valid, first of any duplicates) and problems."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries, problems, seen = [], [], set()
    for pos, item in enumerate(raw, start=1):
        item = item if isinstance(item, dict) else {"username": item}
        src_id = item.get("id", pos)
        name = clean_name(item.get("username"))
        if not USERNAME_RE.match(name):
            problems.append(f"id {src_id}: '{name}' is not a valid X username, not imported")
        elif name.lower() in seen:
            problems.append(f"id {src_id}: '{name}' is listed more than once, imported once")
        else:
            seen.add(name.lower())
            entries.append({"src_id": src_id, "username": name, "enabled": item.get("enabled", True) is not False})
    return entries, problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import config/username.json into monitor_targets")
    parser.add_argument("--apply", action="store_true", help="add the missing accounts (default: dry run)")
    parser.add_argument("--config", default=None, help="config folder (default: ./config)")
    parser.add_argument("--env", default=None, help=".env file (default: ./.env)")
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.config, env_path=args.env)
    except ConfigError as exc:
        print(f"Config error: {exc}")
        return 2
    if not settings.pipeline_db or not all(settings.pipeline_db.values()):
        print("DB_HOST, DB_PORT, DB_NAME, DB_USER and DB_PASSWORD must be set in .env")
        return 2
    config_dir = Path(args.config) if args.config else PROJECT_ROOT / "config"
    entries, problems = file_entries(config_dir / USERNAME_FILE)

    db = PipelineDB(settings.pipeline_db)
    db.open()
    try:
        with db.conn.transaction():
            if args.apply:   # nobody else may add rows between the check and the insert
                db.conn.execute("LOCK TABLE monitor_targets IN SHARE ROW EXCLUSIVE MODE")
            rows = db.conn.execute("SELECT value FROM monitor_targets WHERE platform = %s AND target_type = %s",
                                   (PLATFORM, TARGET_TYPE)).fetchall()
            in_table = {clean_name(r[0]).lower(): clean_name(r[0]) for r in rows}
            in_file = {e["username"].lower() for e in entries}
            both = [e["username"] for e in entries if e["username"].lower() in in_table]
            only_file = [e for e in entries if e["username"].lower() not in in_table]
            only_table = sorted((v for k, v in in_table.items() if k not in in_file), key=str.lower)

            print(f"Database: {db.describe()}  |  {USERNAME_FILE}: {len(entries)} valid account(s)  |  "
                  f"monitor_targets ({PLATFORM}/{TARGET_TYPE}): {len(rows)} row(s)")
            print(f"\nIn both ({len(both)}): {', '.join(both) or '-'}")
            print(f"\nOnly in the table ({len(only_table)}), left as they are: {', '.join(only_table) or '-'}")
            print(f"\nOnly in {USERNAME_FILE} ({len(only_file)}), to be added: "
                  f"{', '.join(e['username'] + ('' if e['enabled'] else ' (disabled)') for e in only_file) or '-'}")
            for problem in problems:
                print(f"  note: {USERNAME_FILE} {problem}")

            if not args.apply:
                print("\nDry run: nothing written. Add --apply to add the accounts listed above.")
                return 0
            base = db.conn.execute("SELECT COALESCE(MAX(sort_order), 0) FROM monitor_targets").fetchone()[0]
            for e in only_file:
                db.conn.execute(
                    "INSERT INTO monitor_targets (platform, target_type, value, enabled, sort_order, extra) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (PLATFORM, TARGET_TYPE, e["username"], e["enabled"], base + int(e["src_id"]),
                     Jsonb({"src_id": e["src_id"]})))
        print(f"\nAdded {len(only_file)} account(s) to monitor_targets.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
