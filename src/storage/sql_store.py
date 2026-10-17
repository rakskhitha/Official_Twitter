"""SQLite and PostgreSQL outputs.

SQLite keeps all five tables (it is the state store: users, status, cursors, run log).
PostgreSQL keeps only user_profiles and posts (media embedded as JSONB); its cursor
is derived from posts and its last-refresh time from user_profiles.

user_profiles holds one row per account (the latest profile). On every refresh the tracked
fields are compared with the stored row and any differences are appended to its `changes`
list, in the same transaction as the update.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from ..models import TRACKED_PROFILE_FIELDS, apply_gap_ops, diff_profile, max_id, parse_time
from .base import Store, UserState

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
POSTGRES_SCHEMA_PATH = Path(__file__).with_name("schema_postgres.sql")

# PostgreSQL connection limits so a slow or silently dead connection cannot hang a run:
# no statement may run longer than 60 s, and TCP keepalives detect a dead peer in about a minute.
# (tcp_user_timeout is applied where the OS supports it; libpq ignores it elsewhere.)
POSTGRES_CONNECT_OPTIONS = {
    "connect_timeout": 15,
    "options": "-c statement_timeout=60000",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
    "tcp_user_timeout": 60000,
}

def split_sql(script: str) -> list[str]:
    """Split an SQL script into statements.

    Comments (`-- …` to the end of the line, whether on their own line or after code) are removed
    BEFORE splitting at `;`, so a `;` inside a comment can never cut a statement. Text inside
    single quotes (e.g. DEFAULT '[]', or 'a;b' / 'a--b') is left untouched; '' is an escaped quote.
    """
    statements: list[str] = []
    current: list[str] = []
    in_quote = False
    i = 0
    while i < len(script):
        char = script[i]
        if in_quote:
            current.append(char)
            if char == "'":
                if script[i + 1:i + 2] == "'":       # '' = escaped quote, still inside the string
                    current.append("'")
                    i += 1
                else:
                    in_quote = False
        elif char == "'":
            in_quote = True
            current.append(char)
        elif script.startswith("--", i):
            end = script.find("\n", i)
            i = len(script) if end == -1 else end   # keep the newline itself
            continue
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)
        i += 1
    statements.append("".join(current))
    if in_quote:
        raise ValueError("SQL script has an unterminated quoted string")
    return [s.strip() for s in statements if s.strip()]


def schema_statements(path: Path, types: dict[str, str]) -> list[str]:
    """The CREATE statements of a schema file, with the per-database type placeholders filled in."""
    script = path.read_text(encoding="utf-8")
    for key, value in types.items():
        script = script.replace("{" + key + "}", value)
    return split_sql(script)


# Profile columns copied from normalize_user() on every refresh.
PROFILE_COLS = ["username", "name", "description", "location", "url", "created_at", "verified",
                "verified_type", "protected", "followers_count", "following_count", "tweet_count",
                "listed_count", "profile_image_url", "pinned_tweet_id"]

POST_COLS = ["post_id", "author_id", "username", "created_at", "post_url", "text", "full_text", "lang",
             "conversation_id", "in_reply_to_user_id", "post_type", "referenced_post_id",
             "referenced_json", "referenced_full_text", "referenced_author_id", "referenced_created_at",
             "possibly_sensitive", "retweet_count", "reply_count", "like_count",
             "quote_count", "bookmark_count", "impression_count", "metrics_fetched_at",
             "first_seen_at", "raw_json"]


COPY_BATCH = 500   # posts read from SQLite / written to PostgreSQL per step when filling in missed data

MEDIA_COLS = ["media_key", "post_id", "type", "url", "preview_image_url", "duration_ms", "width", "height",
              "alt_text", "variants_json"]


def profile_freshness(refreshed_at: datetime | None, last_changed_at: datetime | None) -> datetime | None:
    """When a user_profiles row last changed: a refresh sets refreshed_at, a status change (set_status)
    only last_changed_at."""
    return max((t for t in (refreshed_at, last_changed_at) if t is not None), default=None)


def merge_changes(*lists: list[dict]) -> list[dict]:
    """Combine `changes` histories: every entry once, oldest first (entries are compared by moment,
    field, old and new value, so the same change recorded by two outputs counts once)."""
    seen, merged = set(), []
    for entry in (e for changes in lists for e in changes or []):
        key = (parse_time(entry.get("changed_at")), entry.get("field"),
               json.dumps(entry.get("old"), sort_keys=True), json.dumps(entry.get("new"), sort_keys=True))
        if key not in seen:
            seen.add(key)
            merged.append(entry)
    merged.sort(key=lambda e: parse_time(e.get("changed_at")) or datetime.min.replace(tzinfo=timezone.utc))
    return merged


class SqlStore(Store):
    """Shared logic. SQL is written with '?' placeholders; PostgreSQL swaps them for '%s'."""

    types: dict[str, str] = {}
    placeholder = "?"
    schema_path = SCHEMA_PATH
    post_cols = POST_COLS

    def __init__(self):
        self.conn = None

    # --- dialect hooks -------------------------------------------------
    def _connect(self):
        raise NotImplementedError

    @contextmanager
    def _tx(self):
        raise NotImplementedError
        yield  # pragma: no cover

    def _json(self, obj):
        raise NotImplementedError

    def _value(self, value):
        return value

    def _read_time(self, value) -> datetime | None:
        return value

    def _read_json(self, value):
        return value

    # --- helpers ---------------------------------------------------------
    def _sql(self, sql: str) -> str:
        return sql if self.placeholder == "?" else sql.replace("?", self.placeholder)

    def _exec(self, sql: str, params: tuple = ()):
        return self.conn.execute(self._sql(sql), tuple(self._value(p) for p in params))

    # --- Store API -----------------------------------------------------
    # Columns added after the first release, per table. CREATE TABLE IF NOT EXISTS never changes an
    # existing table, so a database created by an older version gets them added (and, where
    # listed in _backfill_sql, filled for existing rows) when it is opened.
    ADDED_COLUMNS: dict[str, dict[str, str]] = {}
    POST_ID_AS_TEXT = "post_id"

    def open(self) -> None:
        self.conn = self._connect()
        with self._tx():
            for statement in schema_statements(self.schema_path, self.types):
                self.conn.execute(statement)
            self._upgrade_schema()

    def _existing_columns(self, table: str) -> set[str]:
        raise NotImplementedError

    def _upgrade_schema(self) -> None:
        """Runs inside open()'s transaction: either the whole upgrade happens or none of it."""
        for table, columns in self.ADDED_COLUMNS.items():
            existing = self._existing_columns(table)
            for column, sql_type in columns.items():
                if column in existing:
                    continue
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")
                backfill = self._backfill_sql(table, column)
                if backfill:
                    self.conn.execute(backfill)   # once, at the moment the column is added

    def _backfill_sql(self, table: str, column: str) -> str | None:
        if (table, column) == ("posts", "post_url"):
            pid = self.POST_ID_AS_TEXT        # same rule as models.post_url()
            return ("UPDATE posts SET post_url = CASE WHEN username IS NULL OR username = '' "
                    f"THEN 'https://x.com/i/status/' || {pid} "
                    f"ELSE 'https://x.com/' || username || '/status/' || {pid} END WHERE post_url IS NULL")
        return None

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def load_users(self) -> dict[str, UserState]:
        rows = self._exec(
            "SELECT user_id, config_username, username, status, active, since_id, last_profile_refresh_at, "
            "added_at, fetch_gaps, last_attempt_at FROM monitored_users").fetchall()
        return {
            r[0]: UserState(user_id=r[0], config_username=r[1], username=r[2], status=r[3],
                            active=bool(r[4]), since_id=r[5], last_profile_refresh_at=self._read_time(r[6]),
                            added_at=self._read_time(r[7]), gaps=list(self._read_json(r[8]) or []),
                            last_attempt_at=self._read_time(r[9]))
            for r in rows
        }

    def record_attempts(self, user_ids: set[str], attempted_at: datetime) -> None:
        with self._tx():
            for user_id in user_ids:
                self._exec("UPDATE monitored_users SET last_attempt_at = ? WHERE user_id = ?",
                           (attempted_at, user_id))

    def _upsert_user(self, user_id, config_username, username, now, status=None, refreshed_at=None):
        self._exec(
            "INSERT INTO monitored_users (user_id, config_username, username, active, status, added_at, "
            "last_profile_refresh_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (user_id) DO UPDATE SET config_username = excluded.config_username, "
            "username = excluded.username, active = excluded.active, "
            "status = COALESCE(?, monitored_users.status), "
            "last_profile_refresh_at = COALESCE(excluded.last_profile_refresh_at, "
            "monitored_users.last_profile_refresh_at)",
            (user_id, config_username, username, True, status or "ok", now, refreshed_at, status))

    def _save_profile_row(self, user: dict, status: str, refreshed_at: datetime) -> list[dict]:
        """Insert the account's row, or compare + append changes + update it. Call inside _tx().
        Returns the changes recorded (empty on a first save or when nothing changed)."""
        row = self._exec(f"SELECT {', '.join(TRACKED_PROFILE_FIELDS)}, changes FROM user_profiles "
                         f"WHERE user_id = ?", (user["user_id"],)).fetchone()
        values = [user[c] for c in PROFILE_COLS] + [status, self._json(user["raw"]), refreshed_at]
        cols = PROFILE_COLS + ["status", "raw_json", "refreshed_at"]

        if row is None:
            self._exec(f"INSERT INTO user_profiles (user_id, {', '.join(cols)}, first_seen_at, changes) "
                       f"VALUES ({', '.join('?' * (len(cols) + 3))})",
                       (user["user_id"], *values, refreshed_at, self._json([])))
            return []

        old = dict(zip(TRACKED_PROFILE_FIELDS, row[:-1]))
        changes = list(self._read_json(row[-1]) or [])
        new_changes = diff_profile(old, {**user, "status": status}, refreshed_at)
        changes.extend(new_changes)
        set_cols, set_values = cols + ["changes"], [*values, self._json(changes)]
        if new_changes:
            set_cols.append("last_changed_at")
            set_values.append(refreshed_at)
        self._exec(f"UPDATE user_profiles SET {', '.join(f'{c} = ?' for c in set_cols)} WHERE user_id = ?",
                   (*set_values, user["user_id"]))
        return new_changes

    def _record_status_change(self, user_id: str, status: str, changed_at: datetime) -> list[dict]:
        """Update user_profiles.status and log the change if it differs. Call inside _tx()."""
        row = self._exec("SELECT status, changes FROM user_profiles WHERE user_id = ?", (user_id,)).fetchone()
        if row is None:
            return []
        new_changes = diff_profile({"status": row[0]}, {"status": status}, changed_at)
        if not new_changes:
            return []
        changes = list(self._read_json(row[1]) or []) + new_changes
        self._exec("UPDATE user_profiles SET status = ?, changes = ?, last_changed_at = ? WHERE user_id = ?",
                   (status, self._json(changes), changed_at, user_id))
        return new_changes

    def _upsert_post(self, values: dict) -> None:
        cols = self.post_cols
        updates = ", ".join(f"{c} = excluded.{c}" for c in cols[1:] if c != "first_seen_at")
        self._exec(f"INSERT INTO posts ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
                   f"ON CONFLICT (post_id) DO UPDATE SET {updates}", tuple(values[c] for c in cols))

    def save_profile(self, user: dict, config_username: str, status: str, refreshed_at: datetime) -> list[dict]:
        with self._tx():
            self._upsert_user(user["user_id"], config_username, user["username"], refreshed_at,
                              status=status, refreshed_at=refreshed_at)
            return self._save_profile_row(user, status, refreshed_at)

    def set_status(self, user_id: str, status: str, changed_at: datetime) -> None:
        with self._tx():
            self._exec("UPDATE monitored_users SET status = ? WHERE user_id = ?", (status, user_id))
            self._record_status_change(user_id, status, changed_at)

    def set_active_users(self, active_user_ids: set[str]) -> None:
        with self._tx():
            for user_id, in self._exec("SELECT user_id FROM monitored_users").fetchall():
                self._exec("UPDATE monitored_users SET active = ? WHERE user_id = ?",
                           (user_id in active_user_ids, user_id))

    def save_posts(self, user_id: str, username: str, config_username: str, posts: list[dict],
                   new_since_id: str | None, fetched_at: datetime, gap_ops: list[dict] | None = None) -> None:
        media_cols = MEDIA_COLS
        media_updates = ", ".join(f"{c} = excluded.{c}" for c in media_cols[2:])
        media_sql = (f"INSERT INTO media ({', '.join(media_cols)}) VALUES ({', '.join('?' * len(media_cols))}) "
                     f"ON CONFLICT (media_key, post_id) DO UPDATE SET {media_updates}")

        with self._tx():
            row = self._exec("SELECT since_id, fetch_gaps FROM monitored_users WHERE user_id = ?",
                             (user_id,)).fetchone()
            if row is None:
                self._upsert_user(user_id, config_username, username, fetched_at)
            current = row[0] if row else None
            gaps = apply_gap_ops(list(self._read_json(row[1]) or []) if row else [], gap_ops or [])

            for p in posts:
                self._upsert_post({**p, "referenced_json": self._json(p["referenced"]),
                                   "raw_json": self._json(p["raw"]), "first_seen_at": fetched_at})
                for m in p["media"]:
                    mvalues = {**m, "variants_json": self._json(m["variants"]) if m["variants"] else None}
                    self._exec(media_sql, tuple(mvalues[c] for c in media_cols))

            # Cursor and gaps move together with the posts (same transaction): the cursor can
            # never advance past a range that is not also recorded as a gap.
            self._exec("UPDATE monitored_users SET since_id = ?, last_posts_fetch_at = ?, username = ?, "
                       "fetch_gaps = ? WHERE user_id = ?",
                       (max_id([current, new_since_id]), fetched_at, username, self._json(gaps), user_id))

    def log_run(self, summary: dict) -> None:
        cols = ["run_id", "started_at", "finished_at", "users_ok", "users_failed", "posts_new",
                "user_reads", "post_reads", "referenced_reads", "est_cost_usd", "credits_before", "credits_after",
                "credits_used", "status", "items_loaded", "items_processed", "pipeline_inserted", "pipeline_duplicates"]
        values = [summary.get(c) for c in cols] + [self._json(summary["errors"]),
                                                   self._json(summary.get("warnings", []))]
        with self._tx():
            self._exec(f"INSERT INTO runs ({', '.join(cols)}, errors_json, warnings_json) VALUES "
                       f"({', '.join('?' * (len(cols) + 2))})", tuple(values))


class SQLiteStore(SqlStore):
    name = "sqlite"
    types = {"TS": "TEXT", "JSON": "TEXT", "BOOL": "INTEGER", "BIGINT": "INTEGER"}

    ADDED_COLUMNS = {
        "monitored_users": {"fetch_gaps": "TEXT", "last_attempt_at": "TEXT", "last_scraped_at": "TEXT",
                            "posts_last_run": "INTEGER"},
        "runs": {"credits_before": "REAL", "credits_after": "REAL", "credits_used": "REAL",
                 "referenced_reads": "INTEGER", "warnings_json": "TEXT", "status": "TEXT", "items_loaded": "INTEGER",
                 "items_processed": "INTEGER", "pipeline_inserted": "INTEGER", "pipeline_duplicates": "INTEGER"},
        "posts": {"post_url": "TEXT", "referenced_full_text": "TEXT", "referenced_author_id": "TEXT",
                  "referenced_created_at": "TEXT"},
        "pipeline_sent_profiles": {"digest": "TEXT"},
    }

    def __init__(self, path: Path):
        super().__init__()
        self.path = Path(path)

    def _existing_columns(self, table: str) -> set[str]:
        return {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}

    def record_collection(self, user_id: str, collected_at: datetime, posts: int) -> None:
        with self._tx():
            self._exec("UPDATE monitored_users SET last_scraped_at = ?, posts_last_run = ? WHERE user_id = ?",
                       (collected_at, posts, user_id))

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # explicit BEGIN/COMMIT in _tx; wait up to 30 s if another program holds a write lock
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @contextmanager
    def _tx(self):
        self.conn.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    def _json(self, obj):
        return None if obj is None else json.dumps(obj, ensure_ascii=False)

    def _value(self, value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    def _read_time(self, value):
        return parse_time(value) if isinstance(value, str) else value

    def _read_json(self, value):
        return json.loads(value) if isinstance(value, str) else value

    # --- source for filling in another output (see storage/sync.py) --------------------------
    def post_ids(self) -> set[str]:
        return {r[0] for r in self._exec("SELECT post_id FROM posts").fetchall()}

    def posts_without_author(self) -> int:
        return self._exec("SELECT COUNT(*) FROM posts WHERE author_id IS NULL OR author_id = ''").fetchone()[0]

    def post_ids_of(self, author_id: str) -> list[str]:
        """One account's post ids (posts.author_id), any order."""
        return [r[0] for r in self._exec("SELECT post_id FROM posts WHERE author_id = ?", (author_id,)).fetchall()]

    def export_posts(self, post_ids: list[str]) -> list[dict]:
        """The given posts as save_posts received them (the normalize_post fields: times as datetimes,
        JSON decoded, media as a list in the post's own attachment order), plus their first_seen_at."""
        if not post_ids:
            return []
        marks = ", ".join("?" * len(post_ids))
        media: dict[str, list[dict]] = {}
        for row in self._exec(f"SELECT {', '.join(MEDIA_COLS)} FROM media WHERE post_id IN ({marks})",
                              tuple(post_ids)).fetchall():
            item = dict(zip(MEDIA_COLS, row))
            item["variants"] = self._read_json(item.pop("variants_json"))
            media.setdefault(item["post_id"], []).append(item)

        posts = []
        for row in self._exec(f"SELECT {', '.join(POST_COLS)} FROM posts WHERE post_id IN ({marks})",
                              tuple(post_ids)).fetchall():
            post = dict(zip(POST_COLS, row))
            post["raw"] = self._read_json(post.pop("raw_json"))
            post["referenced"] = self._read_json(post.pop("referenced_json")) or []
            for col in ("created_at", "referenced_created_at", "metrics_fetched_at", "first_seen_at"):
                post[col] = self._read_time(post[col])
            if post["possibly_sensitive"] is not None:
                post["possibly_sensitive"] = bool(post["possibly_sensitive"])
            order = ((post["raw"] or {}).get("attachments") or {}).get("media_keys") or []
            post["media"] = sorted(media.get(post["post_id"], []),
                                   key=lambda m: order.index(m["media_key"]) if m["media_key"] in order else len(order))
            posts.append(post)
        return posts

    def export_profiles(self) -> list[dict]:
        """Every user_profiles row with values decoded (times as datetimes, flags as booleans), plus the
        account's last collection (monitored_users.last_scraped_at / posts_last_run; None if never)."""
        cols = ["user_id", *PROFILE_COLS, "status", "raw_json", "first_seen_at", "refreshed_at", "last_changed_at",
                "changes"]
        extra = ["last_scraped_at", "posts_last_run"]
        profiles = []
        for row in self._exec(f"SELECT {', '.join('p.' + c for c in cols)}, {', '.join('m.' + c for c in extra)} "
                              f"FROM user_profiles p LEFT JOIN monitored_users m ON m.user_id = p.user_id").fetchall():
            profile = dict(zip(cols + extra, row))
            for col in ("created_at", "first_seen_at", "refreshed_at", "last_changed_at", "last_scraped_at"):
                profile[col] = self._read_time(profile[col])
            for col in ("verified", "protected"):
                if profile[col] is not None:
                    profile[col] = bool(profile[col])
            profile["raw"] = self._read_json(profile.pop("raw_json"))
            profile["changes"] = list(self._read_json(profile["changes"]) or [])
            profiles.append(profile)
        return profiles

    # --- delivery to the pipeline database (see storage/pipeline.py) -------------------------
    # Everything is kept per target database ("host:port/dbname"), so a test database and the real
    # one each get their own start point and delivery record.
    def pipeline_since(self, target: str, start: datetime) -> datetime:
        """When delivery to `target` began: posts first seen and runs started from then on are delivered
        (nothing collected before is loaded). Recorded as `start` the first time it is asked."""
        with self._tx():
            self._exec("INSERT INTO pipeline_targets (target, posts_since, created_at) VALUES (?, ?, ?) "
                       "ON CONFLICT (target) DO NOTHING", (target, start, start))
            return self._read_time(self._exec("SELECT posts_since FROM pipeline_targets WHERE target = ?",
                                              (target,)).fetchone()[0])

    def pipeline_pending_posts(self, target: str, since: datetime) -> list[str]:
        """Posts first seen since `since` that were not delivered to `target` yet, oldest first."""
        # Times are stored as UTC ISO text, so the text comparison is exact; checked again below.
        rows = self._exec("SELECT p.post_id, p.first_seen_at FROM posts p LEFT JOIN pipeline_sent_posts s "
                          "ON s.target = ? AND s.post_id = p.post_id WHERE s.post_id IS NULL AND p.first_seen_at >= ?",
                          (target, since)).fetchall()
        return sorted((r[0] for r in rows if self._read_time(r[1]) >= since), key=int)

    def pipeline_mark_posts(self, target: str, post_ids: list[str], sent_at: datetime) -> None:
        with self._tx():
            for post_id in post_ids:
                self._exec("INSERT INTO pipeline_sent_posts (target, post_id, sent_at) VALUES (?, ?, ?) "
                           "ON CONFLICT (target, post_id) DO NOTHING", (target, post_id, sent_at))

    def pipeline_sent_profiles(self, target: str) -> dict[str, str | None]:
        """user_id -> fingerprint of the profile row last delivered to `target` (None: delivered by a version
        that did not record it, so it is sent again once)."""
        return {r[0]: r[1] for r in self._exec(
            "SELECT user_id, digest FROM pipeline_sent_profiles WHERE target = ?", (target,)).fetchall()}

    def pipeline_mark_profile(self, target: str, user_id: str, freshness: datetime | None, digest: str,
                              sent_at: datetime) -> None:
        with self._tx():
            self._exec("INSERT INTO pipeline_sent_profiles (target, user_id, freshness, sent_at, digest) "
                       "VALUES (?, ?, ?, ?, ?) ON CONFLICT (target, user_id) DO UPDATE SET freshness = excluded.freshness, "
                       "sent_at = excluded.sent_at, digest = excluded.digest",
                       (target, user_id, freshness, sent_at, digest))

    def pipeline_pending_runs(self, target: str, since: datetime) -> list[dict]:
        """Run summaries of runs started since `since` that were not delivered to `target` yet, oldest first."""
        cols = ["run_id", "started_at", "finished_at", "status", "items_loaded", "items_processed", "posts_new",
                "pipeline_inserted", "pipeline_duplicates", "errors_json", "warnings_json", "credits_used",
                "credits_after"]
        runs = []
        for row in self._exec(f"SELECT {', '.join('r.' + c for c in cols)} FROM runs r LEFT JOIN pipeline_sent_runs s "
                              f"ON s.target = ? AND s.run_id = r.run_id WHERE s.run_id IS NULL ORDER BY r.rowid",
                              (target,)).fetchall():
            run = dict(zip(cols, row))
            run["started_at"] = self._read_time(run["started_at"])
            run["finished_at"] = self._read_time(run["finished_at"])
            if run["started_at"] is None or run["started_at"] < since:
                continue
            run["errors"] = list(self._read_json(run.pop("errors_json")) or [])
            run["warnings"] = list(self._read_json(run.pop("warnings_json")) or [])
            runs.append(run)
        return sorted(runs, key=lambda r: r["started_at"])

    def pipeline_mark_run(self, target: str, run_id: str, sent_at: datetime) -> None:
        with self._tx():
            self._exec("INSERT INTO pipeline_sent_runs (target, run_id, sent_at) VALUES (?, ?, ?) "
                       "ON CONFLICT (target, run_id) DO NOTHING", (target, run_id, sent_at))

    def save_target_list(self, target: str, usernames: list[str], read_at: datetime) -> None:
        """Remember the account list read from `target`'s monitor_targets (the fallback, src/targets.py)."""
        with self._tx():
            self._exec("INSERT INTO target_list_cache (target, usernames, read_at) VALUES (?, ?, ?) "
                       "ON CONFLICT (target) DO UPDATE SET usernames = excluded.usernames, read_at = excluded.read_at",
                       (target, self._json(list(usernames)), read_at))

    def load_target_list(self, target: str) -> tuple[list[str], datetime] | None:
        row = self._exec("SELECT usernames, read_at FROM target_list_cache WHERE target = ?", (target,)).fetchone()
        return (list(self._read_json(row[0])), self._read_time(row[1])) if row else None

    def config_names(self) -> dict[str, str]:
        """user_id -> the account's name in username.json (config_username)."""
        return {r[0]: r[1] for r in self._exec(
            "SELECT user_id, config_username FROM monitored_users WHERE config_username IS NOT NULL").fetchall()}


class PostgresStore(SqlStore):
    """Two tables: user_profiles and posts (media embedded as JSONB).

    Cursor = MAX(post_id) per author; last refresh = user_profiles.refreshed_at.
    user_profiles.status is kept so status changes can be logged, but the config-name links
    and the run log are not kept here (SQLite/JSON track them), hence tracks_status = False.
    """

    name = "postgres"
    tracks_status = False
    placeholder = "%s"
    schema_path = POSTGRES_SCHEMA_PATH
    ADDED_COLUMNS = {"posts": {"post_url": "TEXT", "referenced_full_text": "TEXT", "referenced_author_id": "TEXT",
                               "referenced_created_at": "TIMESTAMPTZ"}}
    POST_ID_AS_TEXT = "post_id::text"     # post_id is BIGINT here
    post_cols = [c for c in POST_COLS if c != "raw_json"] + ["media", "media_count", "raw_json"]

    def __init__(self, dsn: str):
        super().__init__()
        self.dsn = dsn

    def _existing_columns(self, table: str) -> set[str]:
        rows = self._exec("SELECT column_name FROM information_schema.columns "
                          "WHERE table_schema = current_schema() AND table_name = ?", (table,)).fetchall()
        return {row[0] for row in rows}

    def _connect(self):
        import psycopg
        return psycopg.connect(self.dsn, autocommit=True, **POSTGRES_CONNECT_OPTIONS)

    @contextmanager
    def _tx(self):
        with self.conn.transaction():
            yield

    def _json(self, obj):
        from psycopg.types.json import Jsonb
        return None if obj is None else Jsonb(obj)

    def load_users(self) -> dict[str, UserState]:
        users: dict[str, UserState] = {}
        for user_id, username, last_refresh in self._exec(
                "SELECT user_id, username, refreshed_at FROM user_profiles").fetchall():
            users[user_id] = UserState(user_id=user_id, config_username=None, username=username,
                                       last_profile_refresh_at=last_refresh)
        for author_id, since_id in self._exec(
                "SELECT author_id, MAX(post_id) FROM posts GROUP BY author_id").fetchall():
            state = users.setdefault(author_id, UserState(user_id=author_id, config_username=None, username=None))
            state.since_id = str(since_id)
        return users

    def save_profile(self, user: dict, config_username: str, status: str, refreshed_at: datetime) -> list[dict]:
        with self._tx():
            return self._save_profile_row(user, status, refreshed_at)

    def set_status(self, user_id: str, status: str, changed_at: datetime) -> None:
        with self._tx():
            self._record_status_change(user_id, status, changed_at)

    def set_active_users(self, active_user_ids: set[str]) -> None:
        pass  # active flag is tracked by SQLite/JSON

    def record_attempts(self, user_ids: set[str], attempted_at: datetime) -> None:
        pass  # run ordering is tracked by SQLite/JSON (no monitored_users table here)

    def _post_row(self, p: dict, author_id: str | None, first_seen_at: datetime) -> dict:
        """A post (normalize_post fields) as a row of this posts table, media embedded as JSONB."""
        media = [{k: v for k, v in m.items() if k != "post_id" and v is not None} for m in p["media"]]
        return {
            **p,
            "post_id": int(p["post_id"]),
            "author_id": p["author_id"] or author_id,
            "referenced_json": self._json(p["referenced"]),
            "media": self._json(media),
            "media_count": len(media),
            "first_seen_at": first_seen_at,
            "raw_json": self._json(p["raw"]),
        }

    def save_posts(self, user_id: str, username: str, config_username: str, posts: list[dict],
                   new_since_id: str | None, fetched_at: datetime, gap_ops: list[dict] | None = None) -> None:
        # Inserting the posts is what advances the cursor (MAX(post_id)), so this is all-or-nothing.
        # No gap list is kept here: recovered posts are simply upserted like any others.
        with self._tx():
            for p in posts:
                self._upsert_post(self._post_row(p, user_id, fetched_at))

    # --- filling in what this output missed, from SQLite (see storage/sync.py) ----------------
    def post_ids(self) -> set[str]:
        return {str(r[0]) for r in self._exec("SELECT post_id FROM posts").fetchall()}

    def insert_missing_posts(self, posts: list[dict]) -> int:
        """Add posts exported by SQLiteStore.export_posts, in one transaction. A post already here is
        left exactly as it is (DO NOTHING). Returns the number of posts added."""
        cols = self.post_cols
        sql = (f"INSERT INTO posts ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
               f"ON CONFLICT (post_id) DO NOTHING")
        added = 0
        with self._tx():
            for p in posts:
                row = self._post_row(p, None, p["first_seen_at"])
                added += self._exec(sql, tuple(row[c] for c in cols)).rowcount
        return added

    def profile_freshness(self) -> dict[str, datetime | None]:
        """user_id -> when its row last changed here (latest of refreshed_at and last_changed_at)."""
        return {user_id: profile_freshness(refreshed, changed) for user_id, refreshed, changed in self._exec(
            "SELECT user_id, refreshed_at, last_changed_at FROM user_profiles").fetchall()}

    def merge_profile(self, profile: dict) -> None:
        """Take over a newer profile row exported by SQLiteStore.export_profiles. The `changes`
        histories of both are combined (nothing is lost), first_seen_at keeps the earliest value and
        last_changed_at the latest."""
        cols = PROFILE_COLS + ["status", "raw_json", "refreshed_at"]
        with self._tx():
            row = self._exec("SELECT changes, first_seen_at, last_changed_at FROM user_profiles "
                             "WHERE user_id = ? FOR UPDATE", (profile["user_id"],)).fetchone()
            values = [profile[c] for c in PROFILE_COLS] + [profile["status"], self._json(profile["raw"]),
                                                           profile["refreshed_at"]]
            if row is None:
                self._exec(f"INSERT INTO user_profiles (user_id, {', '.join(cols)}, first_seen_at, last_changed_at, "
                           f"changes) VALUES ({', '.join('?' * (len(cols) + 4))})",
                           (profile["user_id"], *values, profile["first_seen_at"], profile["last_changed_at"],
                            self._json(profile["changes"])))
                return
            changes = merge_changes(list(row[0] or []), profile["changes"])
            first_seen = min(t for t in (row[1], profile["first_seen_at"]) if t is not None)
            last_changed = max((t for t in (row[2], profile["last_changed_at"]) if t is not None), default=None)
            self._exec(f"UPDATE user_profiles SET {', '.join(f'{c} = ?' for c in cols)}, first_seen_at = ?, "
                       f"last_changed_at = ?, changes = ? WHERE user_id = ?",
                       (*values, first_seen, last_changed, self._json(changes), profile["user_id"]))

    def log_run(self, summary: dict) -> None:
        pass  # run log is kept in SQLite (runs table) and data/json/_runs.jsonl
