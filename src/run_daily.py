"""Daily job: refresh profiles (weekly / new users), fetch new posts, write all outputs.

Usage:
    python -m src.run_daily                      # normal daily run
    python -m src.run_daily --check              # validate config + outputs, no API calls
    python -m src.run_daily --force-profile-refresh
"""

import argparse
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from .config import ConfigError, Settings, load_settings
from .credits import CreditBalance, credits_used, read_credit_balance
from .logfiles import MB, rotate_if_needed
from .models import (max_id, merge_gaps, new_gap, normalize_post, normalize_user, snowflake_lower_bound,
                     utc_now)
from .problems import RunProblems, account
from .storage import JsonStore, PostgresStore, SQLiteStore, Store, UserState
from .storage.pipeline import Delivered, PipelineDB, deliver_posts_and_profiles, deliver_runs
from .storage.sync import copy_missing_to_json, copy_missing_to_postgres
from .targets import AccountList, fill_external_ids, load_accounts
from .timeline import fetch_new_posts
from .users import lookup_by_id, lookup_by_usernames
from .x_client import (FatalApiError, ForbiddenApiError, RunTimeLimitReached, TransientApiError, XApiError,
                       XClient)

log = logging.getLogger("x_monitor")

FETCHABLE_STATUSES = {"ok", "renamed"}


# --------------------------------------------------------------------------- setup
RUN_LOG_FILE = "run.log"
LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(run_id)s] %(name)s: %(message)s"


def new_run_id() -> str:
    """e.g. 20261007T060736Z-30d73f: the run's start time in UTC plus a short random suffix."""
    return f"{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


class RunIdFilter(logging.Filter):
    """Stamps every log record with this process's run id, so every line in run.log (including
    lines from libraries and from before "Run … started") shows which run wrote it."""

    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def setup_logging(log_dir: Path, verbose: bool, max_bytes: int = 50 * MB, backups: int = 5,
                  run_id: str = "-") -> None:
    """All runs append to logs/run.log. The size check happens here, once at the start of a run,
    so one run's lines always stay together in one file (the limit may be exceeded by one run)."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / RUN_LOG_FILE
    rotate_if_needed(log_path, max_bytes, backups)
    fmt = logging.Formatter(LOG_FORMAT)
    run_id_filter = RunIdFilter(run_id)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    handlers: list[logging.Handler] = [logging.FileHandler(log_path, encoding="utf-8")]
    if sys.stdout is not None:  # None when started without a console (pythonw)
        handlers.append(logging.StreamHandler(sys.stdout))
    for handler in handlers:
        handler.addFilter(run_id_filter)    # on the handler: covers records from every logger
        handler.setFormatter(fmt)
        root.addHandler(handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


class LockBusyError(RuntimeError):
    """Another run already holds the lock."""


class RunLock:
    """Prevents two runs from overlapping, using an operating-system file lock.

    The OS releases the lock automatically when the process ends, even after a crash,
    a kill or a power cut, so a dead run can never block the following runs.
    The lock file itself is left in place; only the lock on it matters.
    """

    def __init__(self, path: Path):
        self.path = path
        self.fd: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT)
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise LockBusyError(f"Another run is in progress (lock held on {self.path})") from None
        self.fd = fd
        return self

    def __exit__(self, *exc):
        if self.fd is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
        finally:
            os.close(self.fd)  # closing also releases the lock
            self.fd = None


def open_stores(settings: Settings) -> list[Store]:
    """Open every enabled output. One that cannot be opened is logged as an error and skipped."""
    candidates: list[Store] = []
    if settings.outputs.get("sqlite"):
        candidates.append(SQLiteStore(settings.sqlite_path))
    if settings.outputs.get("postgres"):
        candidates.append(PostgresStore(settings.postgres_dsn))
    if settings.outputs.get("json"):
        candidates.append(JsonStore(settings.json_dir))

    stores = []
    for store in candidates:
        try:
            store.open()
            stores.append(store)
            log.info("Output ready: %s", store.name)
        except Exception as exc:  # noqa: BLE001 - one broken output must not stop the others
            log.error("Output %s unavailable, skipped this run: %s", store.name, exc)
    return stores


def fill_postgres_from_sqlite(stores: list[Store]) -> None:
    """Copy into PostgreSQL what it missed while unreachable (storage/sync.py), so it is not fetched
    from X again. Needs both outputs open this run. A failure is reported, never stops the run: the
    posts PostgreSQL still lacks are then fetched again as before (its cursor stays lower)."""
    sqlite = next((s for s in stores if isinstance(s, SQLiteStore)), None)
    postgres = next((s for s in stores if isinstance(s, PostgresStore)), None)
    if sqlite is None or postgres is None:
        return
    try:
        copied = copy_missing_to_postgres(sqlite, postgres)
    except Exception as exc:  # noqa: BLE001 - the run goes on; the refetch still covers PostgreSQL
        log.error("Could not fill in PostgreSQL from SQLite (%s: %s); missing posts are fetched again",
                  type(exc).__name__, exc)
        return
    if copied.posts or copied.profiles:
        log.info("PostgreSQL: copied %d missing post(s) and %d profile(s) from SQLite (no API cost).",
                 copied.posts, copied.profiles)
    if copied.skipped_posts:
        log.warning("PostgreSQL: %d post(s) in SQLite have no author_id and were not copied; they are fetched "
                    "again instead.", copied.skipped_posts)


class PipelineDelivery:
    """The pipeline output for one run (storage/pipeline.py): at the end of the run, posts and profiles
    are copied from SQLite, then the run summaries. Problems are logged as errors and never stop the run;
    whatever was not delivered is delivered by a later run."""

    def __init__(self, settings: Settings, stores: list[Store]):
        self.db = PipelineDB(settings.pipeline_db)
        self.source = next((s for s in stores if isinstance(s, SQLiteStore)), None)
        self.since = None
        self.connected = False

    def deliver_collected(self, run_started_at) -> Delivered | None:
        """Posts and profiles. Returns the counts, or None if nothing could be delivered this run."""
        if self.source is None:
            log.error("Pipeline DB: SQLite (the source of its data) is not available this run; delivery waits for "
                      "the next run")
            return None
        done = Delivered()
        try:
            self.since = self.source.pipeline_since(self.db.target, run_started_at)
            self.db.open()
            missing = self.db.missing_columns()
            if missing:
                log.error("Pipeline DB %s: missing column(s) %s; nothing delivered (add them, then the next run "
                          "delivers everything)", self.db.describe(),
                          "; ".join(f"{t}: {', '.join(c)}" for t, c in missing.items()))
                self.db.close()
                return None
            self.connected = True
            log.info("Pipeline DB: connected to %s.", self.db.describe())
            deliver_posts_and_profiles(self.source, self.db, self.since, done)
        except Exception as exc:  # noqa: BLE001 - the data stays in SQLite and is delivered next run
            self.connected = False
            self.db.close()
            log.error("Pipeline DB %s: delivery failed (%s: %s); what was not delivered is delivered next run",
                      self.db.describe(), type(exc).__name__, exc)
            return done if (done.posts_inserted or done.posts_duplicate or done.profiles) else None
        log.info("Pipeline DB: %d post(s) added to twitter_posts (%d already there), %d profile(s) written to "
                 "twitter_profiles.", done.posts_inserted, done.posts_duplicate, done.profiles)
        return done

    def deliver_runs(self) -> None:
        """Run summaries (this run's included). Only when the connection worked earlier in this run."""
        try:
            if self.connected:
                added = deliver_runs(self.source, self.db, self.since)
                log.info("Pipeline DB: %d run summary row(s) added to twitter_pipeline_runs.", added)
        except Exception as exc:  # noqa: BLE001
            log.error("Pipeline DB %s: run summary not delivered (%s: %s); delivered next run",
                      self.db.describe(), type(exc).__name__, exc)
        finally:
            self.db.close()


def fill_json_from_sqlite(stores: list[Store]) -> None:
    """Copy into the JSON output what it missed (a skipped run, a failed save), so those posts are not
    fetched from X again (storage/sync.py). Needs both outputs open this run. A failure is reported and
    never stops the run: the posts JSON still lacks are then fetched again as before."""
    sqlite = next((s for s in stores if isinstance(s, SQLiteStore)), None)
    json_store = next((s for s in stores if isinstance(s, JsonStore)), None)
    if sqlite is None or json_store is None:
        return
    try:
        copied = copy_missing_to_json(sqlite, json_store)
    except Exception as exc:  # noqa: BLE001 - the run goes on; the refetch still covers JSON
        log.error("Could not fill in the JSON output from SQLite (%s: %s); its missing posts are fetched again",
                  type(exc).__name__, exc)
        return
    if copied.accounts:
        log.info("JSON: copied %d missing post(s) for %d account(s) from SQLite (no API cost).",
                 copied.posts, copied.accounts)
    if copied.left_to_refetch:
        log.warning("JSON and SQLite differ in a way that cannot be filled in locally for %s; their missing posts "
                    "are fetched again (paid)", ", ".join(f"@{n}" for n in copied.left_to_refetch))


def merge_states(stores: list[Store]) -> dict[str, UserState]:
    """Combine what every output knows. The cursor is the LOWEST across outputs so that an
    output that missed a run gets backfilled; the others just dedup the repeated posts."""
    per_store = [s.load_users() for s in stores]
    merged: dict[str, UserState] = {}
    for user_id in set().union(*per_store) if per_store else set():
        states = [ps.get(user_id) for ps in per_store]
        known = [s for s in states if s]
        # Status and config username come only from outputs that track them (not PostgreSQL)
        tracked = [ps[user_id] for st, ps in zip(stores, per_store) if st.tracks_status and user_id in ps]
        latest = max(tracked or known,
                     key=lambda s: s.last_profile_refresh_at.timestamp() if s.last_profile_refresh_at else 0)
        since_ids = [s.since_id if s else None for s in states]
        refresh_times = [s.last_profile_refresh_at for s in known if s.last_profile_refresh_at]
        added_times = [s.added_at for s in (tracked or known) if s.added_at]
        attempt_times = [s.last_attempt_at for s in known if s.last_attempt_at]
        merged[user_id] = UserState(
            user_id=user_id,
            config_username=latest.config_username,
            username=latest.username,
            status=latest.status,
            active=latest.active,
            since_id=None if any(x is None for x in since_ids) else min(since_ids, key=int),
            last_profile_refresh_at=max(refresh_times) if refresh_times else None,
            added_at=min(added_times) if added_times else None,
            gaps=merge_gaps([s.gaps for s in known]),   # every output's open gaps get filled
            last_attempt_at=max(attempt_times) if attempt_times else None,
        )
    return merged


# --------------------------------------------------------------------------- run
class DailyRun:
    def __init__(self, settings: Settings, client: XClient, stores: list[Store], problems: RunProblems,
                 run_id: str | None = None):
        self.s = settings
        self.client = client
        self.stores = stores
        self.problems = problems         # every warning/error logged during the run (src/problems.py)
        self.started_at = utc_now()
        self.run_id = run_id or new_run_id()
        self.user_reads = 0
        self.post_reads = 0              # every post X returned (billed): own posts + referenced tweets
        self.posts_new = 0               # the monitored accounts' own posts only
        self.referenced_reads = 0        # referenced tweets X returned (includes.tweets)
        self.users_ok = 0
        self.users_failed = 0
        self.users_skipped = 0
        self.fatal = False
        self.time_limited = False
        self._config_names = {n.lower() for n in settings.usernames}
        self._reported_duplicate_links: set[str] = set()
        # Time budget: no account / gap step / request / wait starts after this (time.monotonic()).
        self.deadline = time.monotonic() + settings.run_time_limit_minutes * 60
        self.client.deadline = self.deadline
        self.accounts_to_fetch = 0       # set by fetch_posts (for the time-limit message)
        self.attempted: set[str] = set()
        self.balance_before: CreditBalance | None = None   # X credit balance at start / end of run
        self.balance_after: CreditBalance | None = None

    def check_time(self) -> None:
        if time.monotonic() >= self.deadline:
            raise RunTimeLimitReached(f"run time limit of {self.s.run_time_limit_minutes} min reached")

    def _each_store(self, action: str, fn) -> bool:
        ok = True
        for store in self.stores:
            try:
                fn(store)
            except Exception as exc:  # noqa: BLE001
                ok = False
                log.error("%s: %s failed: %s", store.name, action, exc)
        return ok

    # ---- account identity
    def _link_config_names(self, states: dict[str, UserState]) -> dict[str, UserState]:
        """Map each config name (lower case) to the ONE account it is linked to.

        A config name should only ever be linked to one user_id. If stored data links it to
        several, the account registered first (the original) is kept, the same way on every
        run, and an error is reported once per run. It never alternates between accounts.
        """
        groups: dict[str, list[UserState]] = {}
        for state in states.values():
            if state.config_username:
                groups.setdefault(state.config_username.lower(), []).append(state)
        linked = {}
        for name, group in groups.items():
            group.sort(key=lambda s: (s.added_at is None, s.added_at or self.started_at, int(s.user_id)))
            linked[name] = group[0]
            if len(group) > 1 and name not in self._reported_duplicate_links:
                self._reported_duplicate_links.add(name)
                others = ", ".join(s.user_id for s in group[1:])
                log.error("Config name '%s' is linked to more than one account; following only the original "
                          "(id %s), ignoring id(s) %s", name, group[0].user_id, others, extra=account(name))
        return linked

    def _same_account_as_other_row(self, user_id: str, config_name: str, states: dict[str, UserState]) -> bool:
        """True if this account is already linked to a DIFFERENT row of username.json (two rows,
        one account). The existing link is kept, so the two rows never take turns."""
        existing = states.get(user_id)
        if (existing is not None and existing.config_username
                and existing.config_username.lower() != config_name.lower()
                and existing.config_username.lower() in self._config_names):
            log.warning("'%s' and '%s' in username.json are the same account (id %s); it is collected once, "
                        "under '%s'", config_name, existing.config_username, user_id, existing.config_username,
                        extra=account(config_name))
            return True
        return False

    # ---- step 2: profiles
    def refresh_profiles(self, states: dict[str, UserState], force: bool) -> dict[str, UserState]:
        by_config = self._link_config_names(states)
        cutoff = self.started_at - timedelta(days=self.s.profile_refresh_days)

        lookup: dict[str, str] = {}   # lower(name to look up) -> config username
        for name in self.s.usernames:
            state = by_config.get(name.lower())
            due = (force or state is None or state.last_profile_refresh_at is None
                   or state.last_profile_refresh_at <= cutoff)
            if due:
                lookup[((state.username if state and state.username else name)).lower()] = name

        if not lookup:
            log.info("Profiles: no refresh due (weekly).")
            return states

        log.info("Profiles: looking up %d user(s).", len(lookup))
        result = lookup_by_usernames(self.client, list(lookup), self.s.user_fields)
        self.user_reads += len(result.users)   # billed per user returned, even ones we do not keep
        if result.failed:
            self._report_failed_lookups(result.failed, lookup, by_config)

        # Known accounts whose handle no longer leads to them: resolved by their stored id below.
        check_by_id: list[tuple[str, UserState]] = []

        for raw in result.users:
            config_name = lookup.get(raw["username"].lower())
            if config_name is None:
                continue
            known = by_config.get(config_name.lower())
            if known is not None and known.user_id != raw["id"]:
                # The handle now belongs to a DIFFERENT account. The stored id is the identity:
                # never save or follow the new holder; find the original account by its id.
                log.warning("@%s now belongs to a different account (id %s). Still following the original "
                            "account (id %s) for '%s'.", raw["username"], raw["id"], known.user_id, config_name,
                            extra=account(config_name))
                check_by_id.append((config_name, known))
                continue
            if known is None and self._same_account_as_other_row(raw["id"], config_name, states):
                continue
            self._save_profile(raw, config_name, states)

        for looked_up, reason in result.not_found.items():
            config_name = lookup.get(looked_up)
            if config_name is None:
                continue
            state = by_config.get(config_name.lower())
            if state is None:
                log.warning("@%s: %s (new user, nothing stored)", config_name, reason.replace("_", " "),
                            extra=account(config_name))
                continue
            check_by_id.append((config_name, state))

        for config_name, state in check_by_id:
            self._resolve_by_id(config_name, state, states)
        return states

    def _resolve_by_id(self, config_name: str, state: UserState, states) -> None:
        """Look up a known account by its stored id (its handle no longer leads to it)."""
        try:
            raw, id_reason = lookup_by_id(self.client, state.user_id, self.s.user_fields)
        except FatalApiError:
            raise
        except XApiError as exc:
            # No answer from X: keep the user's status as it is and check again next run.
            log.error("@%s: could not check the account by id %s (%s); status unchanged", config_name,
                      state.user_id, exc, extra=account(config_name))
            return
        if raw:
            self.user_reads += 1
            renamed = (raw.get("username") or "").lower() != (state.username or "").lower()
            if renamed:
                log.warning("@%s was renamed to @%s (user id %s).", state.username, raw["username"], state.user_id,
                            extra=account(config_name))
            self._save_profile(raw, config_name, states, status="renamed" if renamed else None)
        else:
            log.warning("@%s (id %s): %s. Skipping its posts.", config_name, state.user_id, id_reason,
                        extra=account(config_name))
            state.status = id_reason
            self._each_store("set status", lambda st: st.set_status(state.user_id, id_reason, self.started_at))

    def _report_failed_lookups(self, failed: dict[str, str], lookup: dict[str, str],
                               by_config: dict[str, UserState]) -> None:
        """A failed request is not an answer: nothing is marked not_found and stored state is left as
        it is. Known users still get their tweets fetched (their user_id is stored) and, because
        their refresh time did not move, their profile is retried on the next run."""
        config_names = [lookup.get(name, name) for name in failed]
        new_users = [n for n in config_names if n.lower() not in by_config]
        first_error = next(iter(failed.values()))
        not_registered = (f" New user(s) not registered this run (no user id yet), retried next run: "
                          f"{', '.join(new_users)}." if new_users else "")
        log.error("Profile lookup failed for %d user(s): %s. Tweets are still collected for known users; profiles "
                  'are retried next run. If this is a field-name error, override "api.user_fields" in '
                  "config/input.json.%s", len(failed), first_error, not_registered)

    def _save_profile(self, raw: dict, config_name: str, states, status: str | None = None):
        user = normalize_user(raw)
        status = "protected" if user["protected"] else (status or "ok")
        recorded: list[list[dict]] = []
        self._each_store("save profile", lambda st: recorded.append(
            st.save_profile(user, config_name, status, self.started_at)))
        changed = sorted({c["field"] for changes in recorded for c in changes or []})
        if changed:
            log.info("@%s profile changed: %s", user["username"], ", ".join(changed))
        state = states.get(user["user_id"])
        if state is None:
            state = states[user["user_id"]] = UserState(user_id=user["user_id"], config_username=config_name,
                                                        username=user["username"], added_at=self.started_at)
            log.info("New user @%s registered (id %s).", user["username"], user["user_id"])
        state.config_username = config_name
        state.username = user["username"]
        state.status = status
        state.last_profile_refresh_at = self.started_at

    # ---- step 3: posts
    def fetch_posts(self, states: dict[str, UserState]) -> None:
        by_config = self._link_config_names(states)
        active_ids = {by_config[n.lower()].user_id for n in self.s.usernames if n.lower() in by_config}
        self._each_store("mark active users", lambda st: st.set_active_users(active_ids))

        # One account failing never stops the others. But many accounts IN A ROW failing with
        # server/network errors, 403s that are not labelled app-level (those stop the run at once in
        # x_client) or unexpected errors means an outage or an app-wide access problem: stop instead
        # of grinding through every account. A success or a definite error such as 404 shows the
        # API is up and resets the count.
        queue: list[UserState] = []
        for name in self.s.usernames:
            state = by_config.get(name.lower())
            if state is None:
                continue  # lookup failed; already reported
            if state.status not in FETCHABLE_STATUSES:
                log.info("@%s: skipped (status %s).", state.username or name, state.status)
                self.users_skipped += 1
                continue
            queue.append(state)
        # Accounts the last run(s) did not get to (time limit, outage stop) go first; never-tried
        # accounts first of all. Ties keep the username.json order (sort is stable).
        queue.sort(key=lambda s: (s.last_attempt_at is not None, s.last_attempt_at or self.started_at))
        self.accounts_to_fetch = len(queue)

        consecutive = 0
        try:
            for position, state in enumerate(queue, start=1):
                self.check_time()
                consecutive = self._fetch_one(state, consecutive)
                self.attempted.add(state.user_id)
                if consecutive >= self.s.max_consecutive_failures:
                    raise FatalApiError(
                        f"{consecutive} accounts in a row failed with server/network, access (403) or unexpected "
                        f"errors (X outage, connection or app-access problem likely). The remaining "
                        f"{len(queue) - position} account(s) were not checked; they go first in the next run.")
        finally:
            self._each_store("record attempts", lambda st: st.record_attempts(self.attempted, self.started_at))

    def _fetch_one(self, state: UserState, consecutive: int) -> int:
        """Fetch one account; returns the updated count of consecutive outage-type failures."""
        try:
            self._fetch_user(state)
            self.users_ok += 1
            return 0
        except (FatalApiError, RunTimeLimitReached):
            raise
        except TransientApiError as exc:
            self.users_failed += 1
            log.error("@%s: post fetch failed (server/network): %s; retried next run", state.username, exc,
                      extra=account(state.config_username))
            return consecutive + 1
        except ForbiddenApiError as exc:
            self.users_failed += 1
            log.error("@%s: post fetch failed (access denied): %s; retried next run", state.username, exc,
                      extra=account(state.config_username))
            return consecutive + 1      # many 403s in a row = app-wide access problem
        except XApiError as exc:
            self.users_failed += 1
            log.error("@%s: post fetch failed: %s", state.username, exc, extra=account(state.config_username))
            return 0                    # X answered: the API is up
        except Exception as exc:  # noqa: BLE001 - e.g. an unexpected API object; only this user fails
            self.users_failed += 1
            log.exception("@%s: unexpected error: %s: %s; cursor not advanced (retried next run)", state.username,
                          type(exc).__name__, exc, extra=account(state.config_username))
            return consecutive + 1

    def _fetch(self, state: UserState, **bounds):
        result = fetch_new_posts(self.client, state.user_id, tweet_fields=self.s.tweet_fields,
                                 media_fields=self.s.media_fields, expansions=self.s.expansions, **bounds)
        self.posts_new += len(result.posts)
        self.referenced_reads += result.referenced_returned
        self.post_reads += len(result.posts) + result.referenced_returned
        return result

    @staticmethod
    def _normalize(result, fetched_at, username: str) -> list[dict]:
        return [normalize_post(p, result.media_by_key, fetched_at, username, result.referenced_by_id)
                for p in result.posts]

    def _save_posts(self, state: UserState, posts: list[dict], new_since_id, fetched_at, gap_ops: list[dict]):
        self._each_store("save posts", lambda st: st.save_posts(
            state.user_id, state.username, state.config_username, posts, new_since_id, fetched_at, gap_ops))

    def _fetch_user(self, state: UserState) -> None:
        """1) New posts since the cursor. If the page cap stops it early, the range that was NOT
        fetched is recorded as a gap (saved together with the posts and the new cursor).
        2) Leftover pages of this account's budget are used to fill open gaps, oldest data last.
        3) When posts were saved (also if a later step failed or hit the time limit), the account's
        last collection time and number of own posts saved this run are recorded."""
        collected = {"at": None, "posts": 0}     # what this run saved for the account
        try:
            self._collect_user(state, collected)
        finally:
            if collected["at"] is not None:
                self._each_store("record collection", lambda st: st.record_collection(
                    state.user_id, collected["at"], collected["posts"]))

    def _collect_user(self, state: UserState, collected: dict) -> None:
        budget = self.s.max_pages_per_user_per_run
        fetched_at = utc_now()
        start_time = None
        if not state.since_id:
            start_time = self.started_at - timedelta(days=self.s.lookback_days_first_run)

        result = self._fetch(state, since_id=state.since_id, start_time=start_time, max_pages=budget)
        budget -= result.pages
        posts = self._normalize(result, fetched_at, state.username)

        gap_ops: list[dict] = []
        recorded = None
        if result.hit_page_cap and result.oldest_id:
            lower = state.since_id or snowflake_lower_bound(start_time)
            if int(lower) < int(result.oldest_id):
                recorded = new_gap(lower, result.oldest_id, fetched_at)
                gap_ops.append({"add": recorded})
                log.warning("@%s: page cap reached; gap recorded (posts after %s and before %s), "
                            "it is filled in the next runs.", state.username, lower, result.oldest_id,
                            extra=account(state.config_username))

        self._save_posts(state, posts, max_id(p["post_id"] for p in posts), fetched_at, gap_ops)
        collected["at"], collected["posts"] = fetched_at, len(posts)
        log.info("@%s: %d new post(s)%s.", state.username, len(posts),
                 f" since {state.since_id}" if state.since_id else f" since {start_time:%Y-%m-%d %H:%M}Z (first run)")

        for gap in merge_gaps([state.gaps, [recorded] if recorded else []]):
            if budget <= 0:
                break
            self.check_time()   # gap is kept and continued next run
            budget -= self._fill_gap(state, gap, budget, collected)

    def _fill_gap(self, state: UserState, gap: dict, budget: int, collected: dict) -> int:
        """Fetch posts inside one gap (newest first) within the page budget. Returns pages used."""
        fetched_at = utc_now()
        result = self._fetch(state, since_id=gap["since_id"], until_id=gap["until_id"], max_pages=budget)
        posts = self._normalize(result, fetched_at, state.username)
        recovered = gap.get("recovered", 0) + len(posts)

        if result.hit_page_cap and result.oldest_id:
            op = {"key": gap["key"], "until_id": result.oldest_id, "recovered": len(posts)}
            log.info("@%s: filling gap %s: %d post(s) recovered so far, more remain.",
                     state.username, gap["key"], recovered)
        else:
            op = {"key": gap["key"], "close": True}
            if recovered:
                log.info("@%s: gap %s filled: %d post(s) recovered.", state.username, gap["key"], recovered)
            else:
                log.warning("@%s: gap after %s and before %s could not be filled: X returned no posts (they may be "
                            "beyond X's 3,200-post timeline limit)", state.username, gap["since_id"], gap["until_id"],
                            extra=account(state.config_username))
        self._save_posts(state, posts, None, fetched_at, [op])
        collected["posts"] += len(posts)        # recovered posts count as collected in this run
        return result.pages

    # ---- step 4: summary
    def status(self) -> str:
        """success: no errors (warnings allowed); partial_success: some errors, the rest collected;
        failed: the run was stopped."""
        if self.fatal:
            return "failed"
        return "partial_success" if self.problems.errors else "success"

    def summary(self, delivered: Delivered | None = None) -> dict:
        prices = self.s.prices
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": utc_now(),
            "users_ok": self.users_ok,
            "users_failed": self.users_failed,
            "posts_new": self.posts_new,
            "referenced_reads": self.referenced_reads,
            "user_reads": self.user_reads,
            "post_reads": self.post_reads,
            "est_cost_usd": round(self.user_reads * prices["user_read"] + self.post_reads * prices["post_read"], 4),
            "status": self.status(),
            "items_loaded": self.accounts_to_fetch,
            "items_processed": self.users_ok + self.users_failed,
            # Rows added to / skipped in twitter_posts by this run (None: no pipeline output this run).
            "pipeline_inserted": delivered.posts_inserted if delivered else None,
            "pipeline_duplicates": delivered.posts_duplicate if delivered else None,
            "errors": list(self.problems.errors),        # [{time, account, message}, ...]
            "warnings": list(self.problems.warnings),
            # Real figures from X's credit balance (None = not measurable this run).
            "credits_before": self.balance_before.total if self.balance_before else None,
            "credits_after": self.balance_after.total if self.balance_after else None,
            "credits_used": credits_used(self.balance_before, self.balance_after)[0],
        }


def run(settings: Settings, force_profile_refresh: bool = False, client: XClient | None = None,
        run_id: str | None = None) -> int:
    if client is None and not settings.bearer_token:
        log.error("X_BEARER_TOKEN is not set in .env. Aborting.")
        return 2
    problems = RunProblems().attach()       # from here on, every warning/error is part of the run summary
    stores: list[Store] = []
    try:
        stores = open_stores(settings)
        if not stores:
            log.error("No output could be opened. Aborting.")
            return 2
        accounts = load_accounts(settings, sqlite_of(stores), utc_now())
        if accounts is None:
            return 2                         # reported; nothing to collect
        log_entry_warnings(accounts)
        log.info("Accounts: %d from %s.", len(accounts.usernames), accounts.source)
        settings = replace(settings, usernames=accounts.usernames)   # this run's list (settings not changed)
        return _run(settings, stores, problems, force_profile_refresh, client, run_id, accounts)
    finally:
        for store in stores:
            store.close()
        problems.detach()


def _run(settings: Settings, stores: list[Store], problems: RunProblems, force_profile_refresh: bool,
         client: XClient | None, run_id: str | None, accounts: AccountList) -> int:
    client = client or XClient(settings.bearer_token)
    job = DailyRun(settings, client, stores, problems, run_id)
    log.info("Run %s started: %d enabled username(s).", job.run_id, len(settings.usernames))
    job.balance_before = read_credit_balance(client, "start")
    if job.balance_before:
        log.info("Credit balance at start: %s.", job.balance_before.describe())
    fill_postgres_from_sqlite(stores)
    fill_json_from_sqlite(stores)
    try:
        states = merge_states(stores)
        states = job.refresh_profiles(states, force_profile_refresh)
        job.fetch_posts(states)
    except RunTimeLimitReached as exc:
        # Clean stop: every finished account is saved; the rest go first in the next run.
        job.time_limited = True
        done = job.users_ok + job.users_failed
        where = (f"after {done} of {job.accounts_to_fetch} accounts; the other "
                 f"{job.accounts_to_fetch - done} are collected next run (they go first)"
                 if job.accounts_to_fetch else "before tweet collection started; it continues next run")
        log.warning("Time limit reached (%d min: %s) %s", settings.run_time_limit_minutes, exc, where)
    except FatalApiError as exc:
        job.fatal = True
        log.error("Run stopped: %s", exc)
    except Exception as exc:  # noqa: BLE001 - safety net: log it and still write the summary
        job.fatal = True
        log.exception("Run stopped by an unexpected error: %s: %s", type(exc).__name__, exc)

    job.balance_after = read_credit_balance(client, "end")
    if job.balance_after:
        log.info("Credit balance at end: %s.", job.balance_after.describe())
        warn_below = settings.low_credit_warning_usd
        if warn_below and job.balance_after.total < warn_below:
            log.warning("Credit balance $%.3f is below the warning level $%.2f: top up at console.x.com "
                        "before runs start failing with 'credits depleted'.", job.balance_after.total, warn_below)
    used_text = credits_used(job.balance_before, job.balance_after)[1]

    if accounts.rows:                       # read from monitor_targets this run: fill in missing X user ids
        record_target_ids(settings, stores, job, accounts)

    # Pipeline database: posts and profiles first (the summary reports how many rows were added) ...
    pipeline = PipelineDelivery(settings, stores) if settings.outputs.get("pipeline") else None
    delivered = pipeline.deliver_collected(job.started_at) if pipeline else None

    summary = job.summary(delivered)
    job._each_store("log run", lambda st: st.log_run(summary))
    if pipeline:
        pipeline.deliver_runs()             # ... then the run summaries (this one's included)

    balance_text = f"${summary['credits_after']:.3f}" if summary["credits_after"] is not None else "unknown"
    log.info("Run %s finished (%s): users ok=%d failed=%d skipped=%d, posts=%d, referenced tweets=%d, "
             "user reads=%d, est. cost $%.3f, credits used %s, balance %s, errors=%d, warnings=%d",
             job.run_id, summary["status"], job.users_ok, job.users_failed, job.users_skipped, summary["posts_new"],
             summary["referenced_reads"], summary["user_reads"], summary["est_cost_usd"], used_text,
             balance_text, len(problems.errors), len(problems.warnings))
    if job.fatal:
        return 2
    return 1 if problems.errors else 0


def check(settings: Settings) -> int:
    """Validate config, token presence, the account list and outputs without calling the X API (no cost)."""
    problems = RunProblems().attach()
    stores: list[Store] = []
    try:
        log.info("Config OK: outputs=%s, accounts from %s", [k for k, v in settings.outputs.items() if v],
                 "monitor_targets" if settings.usernames_source == "database" else "username.json")
        if not settings.bearer_token:
            log.error("X_BEARER_TOKEN is not set in .env")
        stores = open_stores(settings)
        for store in stores:
            log.info("%s: %d user(s) stored", store.name, len(store.load_users()))
        accounts = load_accounts(settings, sqlite_of(stores), utc_now(), save=False)
        if accounts is not None:
            log_entry_warnings(accounts)
            log.info("Accounts: %d enabled from %s.", len(accounts.usernames), accounts.source)
        if settings.outputs.get("pipeline"):
            check_pipeline(settings)
        return 1 if problems.errors else 0
    finally:
        for store in stores:
            store.close()
        problems.detach()


def check_pipeline(settings: Settings) -> None:
    """Connect to the pipeline database and check that every column this program writes exists."""
    db = PipelineDB(settings.pipeline_db)
    try:
        db.open()
        missing = db.missing_columns()
    except Exception as exc:  # noqa: BLE001
        log.error("Pipeline DB %s unavailable: %s", db.describe(), exc)
        return
    finally:
        db.close()
    if missing:
        log.error("Pipeline DB %s: missing column(s) %s", db.describe(),
                  "; ".join(f"{t}: {', '.join(c)}" for t, c in missing.items()))
    else:
        log.info("Pipeline DB %s: reachable, all columns present.", db.describe())


def log_entry_warnings(accounts: AccountList) -> None:
    """Skipped account entries (invalid, duplicate, empty), e.g. "monitor_targets id 5: 'x y' is not a valid X
    username" or "username.json id 3: ..."."""
    for warning in accounts.warnings:
        log.warning("%s %s", accounts.warning_prefix, warning)


def sqlite_of(stores: list[Store]) -> SQLiteStore | None:
    return next((s for s in stores if isinstance(s, SQLiteStore)), None)


def record_target_ids(settings: Settings, stores: list[Store], job: "DailyRun", accounts: AccountList) -> None:
    """Fill monitor_targets.external_id with each account's X user id where it is still empty. Not critical:
    a failure is a warning, and it is tried again next run."""
    try:
        linked = job._link_config_names(merge_states(stores))        # lower(listed name) -> account
        filled = fill_external_ids(settings, accounts.rows, {name: s.user_id for name, s in linked.items()})
    except Exception as exc:  # noqa: BLE001
        log.warning("Account list: could not write X user ids to monitor_targets.external_id (%s: %s); tried again "
                    "next run", type(exc).__name__, exc)
        return
    if filled:
        log.info("Account list: X user id written to monitor_targets.external_id for %d account(s).", filled)


RUN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _run_id_arg(value: str) -> str:
    if not RUN_ID_RE.match(value):
        raise argparse.ArgumentTypeError("run id may only contain letters, digits, '-' and '_' (max 64)")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily X monitor")
    parser.add_argument("--config", default=None, help="config folder (default: ./config)")
    parser.add_argument("--force-profile-refresh", action="store_true", help="refresh all profiles now")
    parser.add_argument("--check", action="store_true", help="validate setup without calling the X API")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--run-id", type=_run_id_arg, default=None,
                        help="use this run id (the scheduler passes one so scheduler.log and run.log match)")
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        logging.basicConfig()
        log.error("%s", exc)
        return 2
    # Chosen before logging starts, so every line of this process carries it ("check" is not a run).
    run_id = "check" if args.check else (args.run_id or new_run_id())
    setup_logging(settings.log_dir, args.verbose, settings.log_max_mb * MB, settings.log_backups, run_id)
    if settings.usernames_source == "file":
        log.info("username.json: %d entries, %d enabled.", settings.total_usernames, len(settings.usernames))

    try:
        if args.check:
            return check(settings)
        try:
            with RunLock(settings.sqlite_path.parent / ".run.lock"):
                return run(settings, args.force_profile_refresh, run_id=run_id)
        except LockBusyError as exc:
            log.error("%s", exc)
            return 2
    except Exception:  # noqa: BLE001 - never exit without the traceback in the log file
        log.exception("Unexpected error")
        return 2


if __name__ == "__main__":
    sys.exit(main())
