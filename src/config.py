"""Load config/username.json + config/input.json + .env into one settings object."""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")

USERNAME_FILE = "username.json"   # list of handles to monitor (usernames_source "file")
USERNAME_SOURCES = ("file", "database")
INPUT_FILE = "input.json"         # all other options

# Field lists sent to the API. Names follow the curl examples in docs.x.com
# (tweet.* naming). They can be overridden in config/input.json under "api" if the
# live API turns out to expect different names (see plan.md section 1.5).
DEFAULT_USER_FIELDS = [
    "created_at", "description", "entities", "location", "profile_image_url",
    "profile_banner_url", "protected", "public_metrics", "url", "verified",
    "verified_type", "verified_followers_count", "parody", "withheld",
    "pinned_tweet_id",
]
DEFAULT_TWEET_FIELDS = [
    "created_at", "author_id", "conversation_id", "lang", "public_metrics",
    "entities", "referenced_tweets", "in_reply_to_user_id", "attachments",
    "note_tweet", "possibly_sensitive", "reply_settings", "edit_history_tweet_ids",
]
DEFAULT_MEDIA_FIELDS = [
    "type", "url", "preview_image_url", "duration_ms", "width", "height",
    "alt_text", "variants",
]
# referenced_tweets.id: X also returns (in includes.tweets) the tweet each post refers to, i.e. a
# retweet's original, a reply's parent, a quote's quoted tweet. Each one is billed as a post read.
DEFAULT_EXPANSIONS = ["attachments.media_keys", "referenced_tweets.id"]

# .env names of the pipeline database's connection settings (outputs.pipeline).
PIPELINE_ENV = {"host": "DB_HOST", "port": "DB_PORT", "dbname": "DB_NAME", "user": "DB_USER",
                "password": "DB_PASSWORD"}

# Prices in USD per resource returned (docs.x.com pricing page, Oct 2026).
DEFAULT_PRICES = {"user_read": 0.010, "post_read": 0.005}


class ConfigError(Exception):
    pass


@dataclass
class Settings:
    usernames: list[str]
    lookback_days_first_run: int = 2
    profile_refresh_days: int = 7
    max_pages_per_user_per_run: int = 10
    max_consecutive_failures: int = 10       # stop the run: this many accounts in a row hit server/network errors
    run_time_limit_minutes: int = 50         # a run stops cleanly after this (rest collected next run)
    scheduler_kill_after_minutes: int = 55   # the scheduler kills a run still going after this
    schedule_interval_minutes: int = 60      # used by src/scheduler.py
    schedule_run_on_start: bool = True       # run once immediately when the scheduler starts
    # pipeline: the shared database with twitter_posts / twitter_profiles / twitter_pipeline_runs
    outputs: dict[str, bool] = field(default_factory=lambda: {"sqlite": True, "postgres": True, "json": True,
                                                              "pipeline": False})
    sqlite_path: Path = PROJECT_ROOT / "data" / "x_monitor.db"
    json_dir: Path = PROJECT_ROOT / "data" / "json"
    log_dir: Path = PROJECT_ROOT / "logs"
    log_max_mb: int = 50                     # logs/run.log rolls over to run.log.1 at this size
    log_backups: int = 5                     # run.log.1 … run.log.5 kept, older deleted
    scheduler_log_max_mb: int = 10           # same for logs/scheduler.log
    scheduler_log_backups: int = 5
    user_fields: list[str] = field(default_factory=lambda: list(DEFAULT_USER_FIELDS))
    tweet_fields: list[str] = field(default_factory=lambda: list(DEFAULT_TWEET_FIELDS))
    media_fields: list[str] = field(default_factory=lambda: list(DEFAULT_MEDIA_FIELDS))
    expansions: list[str] = field(default_factory=lambda: list(DEFAULT_EXPANSIONS))
    prices: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_PRICES))
    low_credit_warning_usd: float = 2.0      # warn in run.log when the balance is below this (0 = off)
    bearer_token: str | None = None
    postgres_dsn: str | None = None
    pipeline_db: dict | None = None          # host, port, dbname, user, password (from DB_* in .env)
    config_warnings: list[str] = field(default_factory=list)   # about username.json entries
    total_usernames: int = 0
    # Where the accounts come from: "file" (username.json) or "database" (monitor_targets, src/targets.py).
    usernames_source: str = "file"
    file_usernames: list[str] = field(default_factory=list)    # username.json's list (the last fallback)
    file_problem: str | None = None                            # why username.json could not be read


def _resolve(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def parse_username_entries(raw: list) -> tuple[list[str], list[str]]:
    """Return (enabled usernames, warnings).

    Each entry is either {"id": 1, "username": "BBCWorld", "enabled": true} or a plain
    string (treated as enabled). "id" is only a row number for you; it is not used.
    Disabled entries are skipped. Invalid names and duplicates are skipped with a warning,
    so one bad row never stops the whole run.
    """
    seen: set[str] = set()
    result: list[str] = []
    warnings: list[str] = []
    for pos, item in enumerate(raw, start=1):
        if isinstance(item, str):
            label, name, enabled = f"entry {pos}", item, True
        elif isinstance(item, dict):
            label = f"id {item['id']}" if "id" in item else f"entry {pos}"
            name, enabled = item.get("username"), item.get("enabled", True)
            if not isinstance(enabled, bool):
                raise ConfigError(f'{USERNAME_FILE} {label}: "enabled" must be true or false')
        else:
            raise ConfigError(f"{USERNAME_FILE} entry {pos} must be an object or a string")

        name = str(name or "").strip().lstrip("@")
        if not name:
            warnings.append(f"{label}: empty username, skipped")
            continue
        if not USERNAME_RE.match(name):
            warnings.append(f"{label}: '{name}' is not a valid X username "
                            f"(letters, digits, _; max 15 chars, this has {len(name)}), skipped")
            continue
        if name.lower() in seen:
            warnings.append(f"{label}: '{name}' is listed more than once, duplicate ignored")
            continue
        seen.add(name.lower())
        if enabled:
            result.append(name)
    return result, warnings


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Config file is not valid JSON: {path}: {exc}") from exc


def read_username_file(path: Path) -> tuple[list[str], list[str], int]:
    """Return (enabled usernames, warnings, number of entries) from username.json."""
    entries = _read_json(path)
    if not isinstance(entries, list):
        raise ConfigError(f'{USERNAME_FILE} must be a JSON list, e.g. '
                          f'[{{"id": 1, "username": "BBCWorld", "enabled": true}}]')
    usernames, warnings = parse_username_entries(entries)
    return usernames, warnings, len(entries)


def load_settings(config_dir: str | Path | None = None, env_path: str | Path | None = None) -> Settings:
    """Read config/input.json (options) and config/username.json (handles), plus .env.

    With "usernames_source": "database" the accounts come from the monitor_targets table at the start of
    each run (src/targets.py); username.json is then optional and only a last fallback."""
    config_dir = Path(config_dir) if config_dir else PROJECT_ROOT / "config"
    load_dotenv(env_path or PROJECT_ROOT / ".env")

    data = _read_json(config_dir / INPUT_FILE)
    if not isinstance(data, dict):
        raise ConfigError(f"{INPUT_FILE} must be a JSON object")
    source = data.get("usernames_source", "file")
    if source not in USERNAME_SOURCES:
        raise ConfigError(f'usernames_source must be one of {", ".join(USERNAME_SOURCES)} (now "{source}")')

    file_problem = None
    try:
        usernames, warnings, total = read_username_file(config_dir / USERNAME_FILE)
    except ConfigError as exc:
        if source == "file":
            raise
        usernames, warnings, total, file_problem = [], [], 0, str(exc)   # only a fallback in database mode
    if source == "file" and not usernames:
        raise ConfigError(f'{USERNAME_FILE} has no enabled usernames (set "enabled": true on at least one)')

    settings = Settings(usernames=list(usernames), config_warnings=warnings, total_usernames=total,
                        usernames_source=source, file_usernames=list(usernames), file_problem=file_problem)
    for key in ("lookback_days_first_run", "profile_refresh_days", "max_pages_per_user_per_run",
                "max_consecutive_failures", "run_time_limit_minutes", "scheduler_kill_after_minutes",
                "schedule_interval_minutes", "log_max_mb", "log_backups",
                "scheduler_log_max_mb", "scheduler_log_backups"):
        if key in data:
            value = data[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigError(f"{key} must be a positive integer")
            setattr(settings, key, value)
    if "low_credit_warning_usd" in data:
        value = data["low_credit_warning_usd"]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ConfigError("low_credit_warning_usd must be a number of dollars, 0 or more (0 = no warning)")
        settings.low_credit_warning_usd = float(value)
    if "schedule_run_on_start" in data:
        if not isinstance(data["schedule_run_on_start"], bool):
            raise ConfigError("schedule_run_on_start must be true or false")
        settings.schedule_run_on_start = data["schedule_run_on_start"]
    if "outputs" in data:
        settings.outputs = {k: bool(data["outputs"].get(k, False)) for k in ("sqlite", "postgres", "json", "pipeline")}
    if "sqlite_path" in data:
        settings.sqlite_path = _resolve(data["sqlite_path"])
    if "json_dir" in data:
        settings.json_dir = _resolve(data["json_dir"])
    if "log_dir" in data:
        settings.log_dir = _resolve(data["log_dir"])

    api = data.get("api", {})
    for key in ("user_fields", "tweet_fields", "media_fields", "expansions"):
        if key in api:
            setattr(settings, key, list(api[key]))
    if "prices" in data:
        settings.prices.update(data["prices"])

    settings.bearer_token = os.environ.get("X_BEARER_TOKEN") or None
    settings.postgres_dsn = os.environ.get("POSTGRES_DSN") or None
    pipeline_env = {key: os.environ.get(name) or None for key, name in PIPELINE_ENV.items()}
    if any(pipeline_env.values()):
        settings.pipeline_db = pipeline_env

    if not (settings.run_time_limit_minutes < settings.scheduler_kill_after_minutes
            < settings.schedule_interval_minutes):
        raise ConfigError(
            "Time limits must satisfy run_time_limit_minutes < scheduler_kill_after_minutes < "
            f"schedule_interval_minutes (now {settings.run_time_limit_minutes} / "
            f"{settings.scheduler_kill_after_minutes} / {settings.schedule_interval_minutes}), so every run "
            "ends before the next one is due")
    if not any(settings.outputs.values()):
        raise ConfigError("All outputs are disabled in config/input.json")
    if settings.outputs.get("postgres") and not (settings.outputs.get("sqlite") or settings.outputs.get("json")):
        raise ConfigError("PostgreSQL cannot be the only output: enable sqlite or json too "
                          "(they track user status and the run log)")
    if settings.outputs.get("postgres") and not settings.postgres_dsn:
        raise ConfigError("PostgreSQL output is enabled but POSTGRES_DSN is not set in .env")
    if settings.outputs.get("pipeline") and not settings.outputs.get("sqlite"):
        raise ConfigError("The pipeline output needs the sqlite output enabled too: its data is copied from SQLite")
    for needed, what in ((settings.outputs.get("pipeline"), "The pipeline output is enabled"),
                         (source == "database", 'usernames_source is "database"')):
        missing = [name for key, name in PIPELINE_ENV.items() if not (settings.pipeline_db or {}).get(key)]
        if needed and missing:
            raise ConfigError(f"{what} but these are not set in .env: {', '.join(missing)}")
    return settings
