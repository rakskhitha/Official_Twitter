"""JSON output: one profile file and one tweets file for all users.

data/json/profile.json   every user's info: latest profile, status, and a `changes` list that
                         gets an entry whenever a tracked field changes (models.diff_profile).
                         Rewritten (atomically) only when something in it changes.
data/json/tweets.jsonl   every collected tweet, one JSON object per line, oldest first
                         within each batch. New tweets are APPENDED; the file is never
                         rewritten, so each run stays fast however big it grows.
data/json/_state.json    internal bookkeeping: newest stored tweet id per user, ranges not
                         collected yet ("gaps", see models.new_gap) and the size of
                         tweets.jsonl after the last complete write.
data/json/_runs.jsonl    one summary line per run.

Crash safety for tweets.jsonl: lines are appended and flushed first, then _state.json
records the new committed size and cursor. If a run dies in between, the next open()
cuts tweets.jsonl back to the committed size and the cursor was not advanced, so those
tweets are fetched again and written once: no duplicates, no half-written lines.
"""

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from ..models import (apply_gap_ops, diff_profile, in_gap, max_id, normalize_user, parse_time, post_url,
                      utc_now)
from .base import Store, UserState

log = logging.getLogger(__name__)

PROFILE_FILE = "profile.json"
TWEETS_FILE = "tweets.jsonl"
STATE_FILE = "_state.json"
RUNS_FILE = "_runs.jsonl"

POST_KEYS = ["post_id", "author_id", "username", "created_at", "post_url", "post_type", "text", "full_text", "lang",
             "conversation_id", "in_reply_to_user_id", "referenced_post_id", "referenced",
             "referenced_full_text", "referenced_author_id", "referenced_created_at", "possibly_sensitive",
             "retweet_count", "reply_count", "like_count", "quote_count", "bookmark_count", "impression_count",
             "metrics_fetched_at"]


def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _with_post_url(record: dict) -> dict:
    """Copy of an old tweets.jsonl record with post_url inserted right after created_at (the same
    position new lines have)."""
    url = post_url(record.get("username"), record["post_id"])
    result: dict = {}
    for key, value in record.items():
        result[key] = value
        if key == "created_at":
            result["post_url"] = url
    result.setdefault("post_url", url)
    return result


def atomic_write_json(path: Path, data) -> None:
    """Write to <file>.tmp, flush to disk, then swap it in. A crash never leaves a half-written file."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class JsonStore(Store):
    name = "json"

    def __init__(self, directory: Path):
        self.dir = Path(directory)
        self.profiles: dict[str, dict] = {}     # user_id -> entry in profile.json
        self.state: dict = {"tweets_file_size": 0, "cursors": {}, "last_posts_fetch_at": {}, "gaps": {}}

    @property
    def tweets_path(self) -> Path:
        return self.dir / TWEETS_FILE

    # --- open / recovery ---------------------------------------------------
    def open(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        self._load_profiles()
        state_path = self.dir / STATE_FILE
        if not state_path.exists():
            self._rebuild_state_from_tweets(f"no {STATE_FILE} found")
        else:
            try:
                self.state = json.loads(state_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                self._rebuild_state_from_tweets(f"{STATE_FILE} was unreadable")
            else:
                self._finish_interrupted_rewrite()
                self._reconcile_tweets_file()
        self._add_post_urls_to_old_lines()

    # --- one-time upgrade: post_url on lines written before that field existed ---------------
    def _rewrite_tmp_path(self) -> Path:
        return self.tweets_path.with_name(TWEETS_FILE + ".rewrite.tmp")

    def _finish_interrupted_rewrite(self) -> None:
        """A rewrite records {old_size, new_size} in _state.json BEFORE swapping the file in. If it
        was interrupted, the file has one of the two sizes; set the committed size to match, so the
        normal recovery below never cuts a correctly rewritten file."""
        pending = self.state.pop("pending_rewrite", None)
        if not pending:
            return
        actual = self.tweets_path.stat().st_size if self.tweets_path.exists() else 0
        if actual == pending["old_size"]:          # the swap did not happen: old file still in place
            self.state["tweets_file_size"] = pending["old_size"]
            log.warning("JSON store: an interrupted %s rewrite was rolled back; it is redone now.", TWEETS_FILE)
        elif actual == pending["new_size"]:        # the swap happened: new file is complete
            self.state["tweets_file_size"] = pending["new_size"]
        self._rewrite_tmp_path().unlink(missing_ok=True)
        self._write_state()

    def _add_post_urls_to_old_lines(self) -> None:
        """Lines written before post_url existed get it once (the file is rewritten safely).
        Old lines always precede new ones, so checking the first line is enough."""
        if self.state.get("post_urls_added"):
            return
        first = b""
        if self.tweets_path.exists():
            with open(self.tweets_path, "rb") as fh:
                first = fh.readline()
        try:
            needs_rewrite = bool(first.strip()) and "post_url" not in json.loads(first)
        except ValueError:
            needs_rewrite = False                  # unreadable first line: leave the file alone
        if needs_rewrite:
            self._rewrite_with_post_urls()
        self.state["post_urls_added"] = True
        self._write_state()

    def _rewrite_with_post_urls(self) -> None:
        old_size = self.tweets_path.stat().st_size
        tmp = self._rewrite_tmp_path()
        updated = 0
        with open(self.tweets_path, "r", encoding="utf-8", newline="\n") as src, \
                open(tmp, "w", encoding="utf-8", newline="\n") as dst:
            for line in src:
                try:
                    record = json.loads(line)
                except ValueError:
                    dst.write(line)                # keep anything unreadable exactly as it was
                    continue
                if "post_url" not in record:
                    record = _with_post_url(record)
                    updated += 1
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
            dst.flush()
            os.fsync(dst.fileno())
        new_size = tmp.stat().st_size
        # 1) record the plan, 2) swap the file in, 3) clear the plan. See _finish_interrupted_rewrite.
        self.state["pending_rewrite"] = {"old_size": old_size, "new_size": new_size}
        self.state["tweets_file_size"] = new_size
        self._write_state()
        os.replace(tmp, self.tweets_path)
        self.state.pop("pending_rewrite", None)
        self._write_state()
        log.info("JSON store: added post_url to %d existing line(s) of %s (one-time upgrade).", updated, TWEETS_FILE)

    def _load_profiles(self) -> None:
        path = self.dir / PROFILE_FILE
        if not path.exists():
            self.profiles = {}
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            # Never overwrite a file we cannot read: stop this output, keep the file for inspection.
            raise RuntimeError(f"{path} is not valid JSON ({exc}); fix or move it, then run again") from exc
        self.profiles = {u["user_id"]: u for u in data.get("users", [])}

    def _reconcile_tweets_file(self) -> None:
        committed = int(self.state.get("tweets_file_size", 0))
        actual = self.tweets_path.stat().st_size if self.tweets_path.exists() else 0
        if actual > committed:
            # Lines from a run that died before recording them. Their cursor was not advanced,
            # so they will be fetched again; drop them now to avoid duplicates.
            with open(self.tweets_path, "r+b") as fh:
                fh.truncate(committed)
            log.warning("JSON store: removed %d byte(s) of unfinished writes from %s (they will be re-fetched).",
                        actual - committed, TWEETS_FILE)
        elif actual < committed:
            self._rebuild_state_from_tweets(f"{TWEETS_FILE} is smaller than recorded (edited or replaced)")

    def _rebuild_state_from_tweets(self, reason: str) -> None:
        """Recreate cursors from tweets.jsonl (max post id per user). A trailing partial line is removed."""
        cursors: dict[str, str] = {}
        valid_size = 0
        bad_lines = 0
        if self.tweets_path.exists():
            with open(self.tweets_path, "rb") as fh:
                for raw in fh:
                    if not raw.endswith(b"\n"):
                        break  # half-written last line
                    valid_size += len(raw)
                    try:
                        rec = json.loads(raw)
                        uid = rec["author_id"]
                        cursors[uid] = max_id([cursors.get(uid), rec["post_id"]])
                    except (ValueError, KeyError, TypeError):
                        bad_lines += 1
            if valid_size < self.tweets_path.stat().st_size:
                with open(self.tweets_path, "r+b") as fh:
                    fh.truncate(valid_size)
        if self.tweets_path.exists() or self.profiles:
            log.warning("JSON store: rebuilt %s from %s (%s); %d user cursor(s), %d unreadable line(s).",
                        STATE_FILE, TWEETS_FILE, reason, len(cursors), bad_lines)
        # Gap records cannot be rebuilt from tweets.jsonl; any pending ones are refetched via the other
        # outputs' gap lists, but this output will only accept posts newer than its rebuilt cursor.
        self.state = {"tweets_file_size": valid_size, "cursors": cursors, "last_posts_fetch_at": {}, "gaps": {}}
        self._write_state()

    # --- writers -------------------------------------------------------------
    def _write_state(self) -> None:
        atomic_write_json(self.dir / STATE_FILE, self.state)

    def _write_profiles(self) -> None:
        users = sorted(self.profiles.values(), key=lambda u: (u.get("username") or "").lower())
        atomic_write_json(self.dir / PROFILE_FILE, {"updated_at": utc_now().isoformat(), "users": users})

    def _profile_entry(self, user_id: str, config_username: str, username: str) -> dict:
        return self.profiles.setdefault(user_id, {
            "user_id": user_id, "username": username, "config_username": config_username,
            "status": "ok", "active": True, "first_seen_at": utc_now().isoformat(),
            "refreshed_at": None, "last_changed_at": None, "profile": None, "changes": [],
        })

    # --- Store API -------------------------------------------------------------
    def load_users(self) -> dict[str, UserState]:
        cursors = self.state.get("cursors", {})
        users = {}
        for uid in set(self.profiles) | set(cursors):
            entry = self.profiles.get(uid, {})
            users[uid] = UserState(
                user_id=uid, config_username=entry.get("config_username"), username=entry.get("username"),
                status=entry.get("status", "ok"), active=entry.get("active", True), since_id=cursors.get(uid),
                gaps=list(self.state.get("gaps", {}).get(uid, [])),
                last_profile_refresh_at=parse_time(entry.get("refreshed_at")),
                added_at=parse_time(entry.get("first_seen_at")),
                last_attempt_at=parse_time(self.state.get("last_attempt_at", {}).get(uid)))
        return users

    def record_attempts(self, user_ids: set[str], attempted_at: datetime) -> None:
        if not user_ids:
            return
        attempts = self.state.setdefault("last_attempt_at", {})
        for user_id in user_ids:
            attempts[user_id] = attempted_at.isoformat()
        self._write_state()

    def save_profile(self, user: dict, config_username: str, status: str, refreshed_at: datetime) -> list[dict]:
        entry = self._profile_entry(user["user_id"], config_username, user["username"])
        # Compare with the stored latest profile (none yet on a first save -> no changes).
        old = {**normalize_user(entry["profile"]), "status": entry.get("status")} if entry.get("profile") else None
        new_changes = diff_profile(old, {**user, "status": status}, refreshed_at)
        entry.setdefault("changes", []).extend(new_changes)
        if new_changes:
            entry["last_changed_at"] = refreshed_at.isoformat()
        entry.update({"username": user["username"], "config_username": config_username, "status": status,
                      "active": True, "refreshed_at": refreshed_at.isoformat(), "profile": user["raw"]})
        self._write_profiles()
        return new_changes

    def set_status(self, user_id: str, status: str, changed_at: datetime) -> None:
        entry = self.profiles.get(user_id)
        if entry is None or entry.get("status") == status:
            return
        if entry.get("profile"):  # only log changes once a profile is stored (same rule as the databases)
            new_changes = diff_profile({"status": entry.get("status")}, {"status": status}, changed_at)
            entry.setdefault("changes", []).extend(new_changes)
            if new_changes:
                entry["last_changed_at"] = changed_at.isoformat()
        entry["status"] = status
        self._write_profiles()

    def set_active_users(self, active_user_ids: set[str]) -> None:
        changed = False
        for user_id, entry in self.profiles.items():
            active = user_id in active_user_ids
            if entry.get("active") != active:
                entry["active"] = active
                changed = True
        if changed:
            self._write_profiles()

    def save_posts(self, user_id: str, username: str, config_username: str, posts: list[dict],
                   new_since_id: str | None, fetched_at: datetime, gap_ops: list[dict] | None = None) -> None:
        cursors = self.state.setdefault("cursors", {})
        cursor = cursors.get(user_id)
        # Everything up to this output's own cursor is already in tweets.jsonl (another output
        # may have been behind, so the fetch can include tweets we already have).
        # Posts older than the cursor are only new to this file if they fall inside one of THIS
        # output's own gaps (ranges it is known not to have). Its own list is exact because it is
        # updated in the same state write as the lines it covers, so this never duplicates.
        own_gaps = self.state.setdefault("gaps", {}).get(user_id, [])
        new = [p for p in posts
               if cursor is None or int(p["post_id"]) > int(cursor)
               or any(in_gap(p["post_id"], g) for g in own_gaps)]
        new.sort(key=lambda p: int(p["post_id"]))

        size = self._append([self._line(p, user_id, fetched_at) for p in new])

        # Commit: only now do the cursor, the gaps and the committed size move forward.
        cursors[user_id] = max_id([cursor, new_since_id])
        gaps = apply_gap_ops(own_gaps, gap_ops or [])
        if gaps:
            self.state["gaps"][user_id] = gaps
        else:
            self.state["gaps"].pop(user_id, None)
        self.state.setdefault("last_posts_fetch_at", {})[user_id] = fetched_at.isoformat()
        self.state["tweets_file_size"] = size
        self._write_state()

        if user_id not in self.profiles:  # e.g. this output missed the profile save
            self._profile_entry(user_id, config_username, username)
            self._write_profiles()

    @staticmethod
    def _line(p: dict, user_id: str, first_seen_at: datetime) -> str:
        """One tweets.jsonl line for a post (normalize_post fields)."""
        record = _jsonable({k: p.get(k) for k in POST_KEYS})
        record["author_id"] = p.get("author_id") or user_id
        record["entities"] = (p.get("raw") or {}).get("entities")
        record["media"] = _jsonable([{k: v for k, v in m.items() if k != "post_id"} for m in p["media"]])
        record["first_seen_at"] = first_seen_at.isoformat()
        return json.dumps(record, ensure_ascii=False) + "\n"

    def _append(self, lines: list[str]) -> int:
        """Append lines to tweets.jsonl and flush them to disk. Returns the new file size. Nothing counts as
        written until the caller records that size in _state.json (see the module docstring)."""
        size = int(self.state.get("tweets_file_size", 0))
        if lines:
            with open(self.tweets_path, "ab") as fh:
                fh.write("".join(lines).encode("utf-8"))
                fh.flush()
                os.fsync(fh.fileno())
                size = fh.tell()
        return size

    def fill_in(self, user_id: str, username: str, config_username: str, posts: list[dict], cursor: str,
                gaps: list[dict]) -> None:
        """Take over posts this output missed, copied from SQLite (storage/sync.py), and set this
        account's cursor and gaps to the given ones. Each post keeps its original first_seen_at, so the
        lines are exactly what a normal save would have written. Same commit order as save_posts."""
        posts = sorted(posts, key=lambda p: int(p["post_id"]))
        size = self._append([self._line(p, user_id, p["first_seen_at"]) for p in posts])
        self.state.setdefault("cursors", {})[user_id] = cursor
        if gaps:
            self.state.setdefault("gaps", {})[user_id] = [dict(g) for g in gaps]
        else:
            self.state.setdefault("gaps", {}).pop(user_id, None)
        self.state["tweets_file_size"] = size
        self._write_state()
        if user_id not in self.profiles:
            self._profile_entry(user_id, config_username, username)
            self._write_profiles()

    def log_run(self, summary: dict) -> None:
        with open(self.dir / RUNS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(_jsonable(summary), ensure_ascii=False) + "\n")
