"""Output to the shared pipeline database: twitter_posts, twitter_profiles, twitter_pipeline_runs.

These tables already exist and are read by downstream processes, so this module only writes rows in
the formats the previous pipeline used (and the columns added for this program); it creates no tables.

Data is copied from SQLite (the source) at the end of each run, never written during collection:
    posts     every post first seen since delivery to this database began (no initial load), not
              delivered yet. Insert only: a tweet_id already in twitter_posts is skipped, never changed.
    profiles  every profile whose row differs from the one last delivered (compared by content, so a
              change is never missed whatever its timing); all of them the first time. twitter_profiles
              holds each account's latest state: rows are updated in place (matched by user_id, else by
              username ignoring case).
    runs      the summary of every run since delivery began, not delivered yet (one row per run).
What was delivered is recorded in SQLite (pipeline_sent_*), so if this database is unreachable, the next
run delivers everything missed, at no API cost.

Not written (downstream fills them): embedding, text_search (generated), entities, translated_text,
detected_language.
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from ..models import post_url, utc_now
from ..problems import account
from .sql_store import COPY_BATCH, POSTGRES_CONNECT_OPTIONS, SQLiteStore, profile_freshness

log = logging.getLogger(__name__)

PIPELINE_NAME = "twitter"
ITEM_TYPE = "posts"
INT_MAX = 2_147_483_647                     # the count columns are `integer`

CONTENT_TYPES = {"original": "tweet", "retweet": "retweet", "quote": "quote", "reply": "reply"}

POST_COLUMNS = ["tweet_id", "post_url", "keyword", "content_type", "author_id", "author_username", "author_name",
                "author_url", "author_profile_picture", "is_verified", "text", "language", "hashtags", "mentions",
                "external_links", "attachments", "is_video", "like_count", "retweet_count", "reply_count",
                "impression_count", "bookmark_count", "quote_count", "is_reply", "parent_tweet_id", "is_ad",
                "posted_at", "scraped_at", "referenced_tweet_id", "referenced_full_text", "referenced_author_id",
                "conversation_id", "raw_json"]
PROFILE_COLUMNS = ["username", "display_name", "verified", "followers_count", "following_count", "bio", "location",
                   "join_date", "last_updated", "ingested_at", "user_id", "profile_image_url", "status",
                   "last_scraped_at", "posts_last_run"]
RUN_COLUMNS = ["run_id", "pipeline_name", "item_type", "started_at", "finished_at", "duration_seconds", "status",
               "items_loaded", "items_processed", "rows_collected", "rows_inserted", "duplicates_skipped", "warnings",
               "errors", "error_messages", "warning_messages", "credits_used", "credits_balance"]
JSON_COLUMNS = {"hashtags", "mentions", "external_links", "attachments", "raw_json", "error_messages",
                "warning_messages"}
REQUIRED = {"twitter_posts": POST_COLUMNS, "twitter_profiles": PROFILE_COLUMNS, "twitter_pipeline_runs": RUN_COLUMNS}

# A link X adds for the post's own photo/video (e.g. https://x.com/user/status/123/photo/1): not external.
OWN_MEDIA_LINK = re.compile(r"^https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/[^/]+/status/\d+/(?:photo|video)/\d+")
RETWEET_PREFIX = re.compile(r"^RT @(\w{1,15}):")
_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# --------------------------------------------------------------------------- field mapping
def _unique(values) -> list:
    seen, result = set(), []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _count(value, field: str, post_id: str) -> int | None:
    """Counts go into `integer` columns; a bigger value would make the whole insert fail."""
    if value is not None and value > INT_MAX:
        log.warning("Post %s: %s %d is above the column's limit; stored as %d", post_id, field, value, INT_MAX)
        return INT_MAX
    return value


def attachment(media: dict) -> dict | None:
    """{"url", "type"} like the previous pipeline: a photo's image URL; for a video or GIF its mp4 file
    (the highest bit rate), else its preview image."""
    kind = media.get("type")
    url = media.get("url")
    if kind in ("video", "animated_gif"):
        mp4 = [v for v in media.get("variants") or [] if v.get("content_type") == "video/mp4" and v.get("url")]
        url = max(mp4, key=lambda v: v.get("bit_rate") or 0)["url"] if mp4 else url
    url = url or media.get("preview_image_url")
    return {"url": url, "type": kind} if url else None


def post_text(post: dict) -> str | None:
    """The full text. A retweet's own text is cut by X, so it is rebuilt as "RT @author: " + the complete
    original; if X could not return the original (deleted, protected), X's own text is kept."""
    if post["post_type"] == "retweet" and post.get("referenced_full_text"):
        match = RETWEET_PREFIX.match(post.get("text") or "")
        if match:
            return f"RT @{match.group(1)}: {post['referenced_full_text']}"
    return post.get("full_text") or post.get("text")


def post_row(post: dict, profile: dict | None, keyword: str | None) -> dict:
    """A twitter_posts row from a post exported by SQLiteStore.export_posts, its author's stored profile
    and the account's name in username.json (keyword)."""
    raw = post.get("raw") or {}
    entities = raw.get("entities") or {}
    links = [u.get("expanded_url") or u.get("url") for u in entities.get("urls") or [] if not u.get("media_key")]
    username = post.get("username")
    pid = post["post_id"]
    return {
        "tweet_id": pid,
        "post_url": post.get("post_url") or post_url(username, pid),
        "keyword": keyword or username,
        "content_type": CONTENT_TYPES.get(post["post_type"], post["post_type"]),
        "author_id": post.get("author_id"),
        "author_username": username,
        "author_name": (profile or {}).get("name"),
        "author_url": f"https://x.com/{username}" if username else None,
        "author_profile_picture": (profile or {}).get("profile_image_url"),
        "is_verified": (profile or {}).get("verified"),
        "text": post_text(post),
        "language": post.get("lang"),
        "hashtags": _unique(h.get("tag") for h in entities.get("hashtags") or []),
        "mentions": _unique(m.get("username") for m in entities.get("mentions") or []),
        "external_links": _unique(u for u in links if u and not OWN_MEDIA_LINK.match(u)),
        "attachments": [a for a in (attachment(m) for m in post.get("media") or []) if a],
        "is_video": any(m.get("type") in ("video", "animated_gif") for m in post.get("media") or []),
        "like_count": _count(post.get("like_count"), "like_count", pid),
        "retweet_count": _count(post.get("retweet_count"), "retweet_count", pid),
        "reply_count": _count(post.get("reply_count"), "reply_count", pid),
        "impression_count": _count(post.get("impression_count"), "impression_count", pid),
        "bookmark_count": _count(post.get("bookmark_count"), "bookmark_count", pid),
        "quote_count": _count(post.get("quote_count"), "quote_count", pid),
        "is_reply": post["post_type"] == "reply",
        "parent_tweet_id": post.get("referenced_post_id") if post["post_type"] == "reply" else None,
        "is_ad": False,
        "posted_at": post.get("created_at"),
        "scraped_at": post.get("first_seen_at"),
        "referenced_tweet_id": post.get("referenced_post_id"),
        "referenced_full_text": post.get("referenced_full_text"),
        "referenced_author_id": post.get("referenced_author_id"),
        "conversation_id": post.get("conversation_id"),
        "raw_json": raw or None,
    }


def join_date(created_at: datetime | None) -> str | None:
    """X's classic date text, as in the previous pipeline's rows: "Fri Jun 28 20:09:40 +0000 2013"
    (English names whatever the computer's language)."""
    if created_at is None:
        return None
    t = created_at.astimezone(timezone.utc)
    return f"{_DAYS[t.weekday()]} {_MONTHS[t.month - 1]} {t.day:02d} {t:%H:%M:%S} +0000 {t.year}"


def profile_row(profile: dict, written_at: datetime) -> dict:
    """A twitter_profiles row from a profile exported by SQLiteStore.export_profiles."""
    return {
        "username": profile["username"],
        "display_name": profile.get("name"),
        "verified": profile.get("verified"),
        "followers_count": profile.get("followers_count"),
        "following_count": profile.get("following_count"),
        "bio": profile.get("description"),
        "location": profile.get("location"),
        "join_date": join_date(profile.get("created_at")),
        "last_updated": profile.get("refreshed_at"),
        "ingested_at": written_at,
        "user_id": profile["user_id"],
        "profile_image_url": profile.get("profile_image_url"),
        "status": profile.get("status"),
        # The account's last collection: when its posts were saved, and how many of its own posts (no
        # referenced tweets) that run saved. Both None until it has been collected once.
        "last_scraped_at": profile.get("last_scraped_at"),
        "posts_last_run": profile.get("posts_last_run"),
    }


def _entries(messages: list) -> list[dict]:
    """Run messages as {time, account, message} (runs logged by older versions stored plain texts)."""
    return [m if isinstance(m, dict) else {"time": None, "account": None, "message": str(m)} for m in messages]


def run_row(run: dict) -> dict:
    """A twitter_pipeline_runs row from a run exported by SQLiteStore.pipeline_pending_runs."""
    started, finished = run.get("started_at"), run.get("finished_at")
    errors, warnings = _entries(run.get("errors") or []), _entries(run.get("warnings") or [])
    return {
        "run_id": run["run_id"],
        "pipeline_name": PIPELINE_NAME,
        "item_type": ITEM_TYPE,
        "started_at": started,
        "finished_at": finished,
        "duration_seconds": round((finished - started).total_seconds(), 3) if started and finished else None,
        "status": run.get("status"),
        "items_loaded": run.get("items_loaded"),
        "items_processed": run.get("items_processed"),
        "rows_collected": run.get("posts_new"),
        "rows_inserted": run.get("pipeline_inserted"),
        "duplicates_skipped": run.get("pipeline_duplicates"),
        "warnings": len(warnings),
        "errors": len(errors),
        "error_messages": errors,
        "warning_messages": warnings,
        "credits_used": run.get("credits_used"),
        "credits_balance": run.get("credits_after"),
    }


# --------------------------------------------------------------------------- database
class PipelineDB:
    """Connection to the pipeline database (settings from DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD)."""

    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    @property
    def target(self) -> str:
        """Identifies the database in SQLite's delivery record."""
        return f"{self.config['host']}:{self.config['port']}/{self.config['dbname']}"

    def describe(self) -> str:
        return f"{self.config['dbname']} on {self.config['host']}:{self.config['port']}"

    def open(self) -> None:
        import psycopg
        self.conn = psycopg.connect(host=self.config["host"], port=self.config["port"], dbname=self.config["dbname"],
                                    user=self.config["user"], password=self.config["password"], autocommit=True,
                                    **POSTGRES_CONNECT_OPTIONS)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def missing_columns(self) -> dict[str, list[str]]:
        """Columns this program writes that the tables lack (e.g. the added columns not created yet)."""
        missing = {}
        for table, columns in REQUIRED.items():
            present = {r[0] for r in self.conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() "
                "AND table_name = %s", (table,)).fetchall()}
            lacking = [c for c in columns if c not in present]
            if lacking:
                missing[table] = lacking if present else ["(table missing)"]
        return missing

    @staticmethod
    def _values(row: dict, columns: list[str]) -> tuple:
        from psycopg.types.json import Jsonb
        return tuple(Jsonb(row[c]) if c in JSON_COLUMNS and row[c] is not None else row[c] for c in columns)

    def insert_posts(self, rows: list[dict]) -> int:
        """Insert in one transaction; a tweet_id already there is skipped. Returns the number added."""
        sql = (f"INSERT INTO twitter_posts ({', '.join(POST_COLUMNS)}) VALUES ({', '.join(['%s'] * len(POST_COLUMNS))}) "
               f"ON CONFLICT (tweet_id) DO NOTHING")
        added = 0
        with self.conn.transaction():
            for row in rows:
                added += self.conn.execute(sql, self._values(row, POST_COLUMNS)).rowcount
        return added

    def save_profile(self, row: dict, config_name: str | None = None) -> str:
        """Insert or update the account's row, in one transaction. Returns "inserted", "updated" or "skipped".

        The row is found by user_id, else by username ignoring letter case (a row the previous pipeline
        wrote, which has no user_id). Other rows with the same username in another letter case and no
        user_id are stale duplicates of this account and are removed, so the unique username stays
        free. A row with the same username that belongs to a DIFFERENT user_id is never touched:
        the profile is then skipped with a warning."""
        with self.conn.transaction():
            by_id = self.conn.execute("SELECT id FROM twitter_profiles WHERE user_id = %s",
                                      (row["user_id"],)).fetchone()
            same_name = self.conn.execute("SELECT id, user_id FROM twitter_profiles WHERE lower(username) = lower(%s) "
                                          "ORDER BY id", (row["username"],)).fetchall()
            target = by_id[0] if by_id else next((rid for rid, uid in same_name if uid is None), None)
            others = [(rid, uid) for rid, uid in same_name if rid != target]
            foreign = [uid for _, uid in others if uid is not None and uid != row["user_id"]]
            if foreign:
                log.warning("Pipeline DB: @%s not updated in twitter_profiles: that username is stored for another "
                            "account (user id %s)", row["username"], foreign[0], extra=account(config_name))
                return "skipped"
            for rid, _ in others:
                self.conn.execute("DELETE FROM twitter_profiles WHERE id = %s", (rid,))
            if others:
                log.info("Pipeline DB: removed %d older duplicate row(s) of @%s from twitter_profiles.",
                         len(others), row["username"])
            values = self._values(row, PROFILE_COLUMNS)
            if target is None:
                self.conn.execute(f"INSERT INTO twitter_profiles ({', '.join(PROFILE_COLUMNS)}) "
                                  f"VALUES ({', '.join(['%s'] * len(PROFILE_COLUMNS))})", values)
                return "inserted"
            self.conn.execute(f"UPDATE twitter_profiles SET {', '.join(f'{c} = %s' for c in PROFILE_COLUMNS)} "
                              f"WHERE id = %s", (*values, target))
            return "updated"

    def insert_run(self, row: dict) -> bool:
        """Add the run's row (a run_id already there is skipped). Returns True if added."""
        return self.conn.execute(
            f"INSERT INTO twitter_pipeline_runs ({', '.join(RUN_COLUMNS)}) VALUES ({', '.join(['%s'] * len(RUN_COLUMNS))}) "
            f"ON CONFLICT (run_id) DO NOTHING", self._values(row, RUN_COLUMNS)).rowcount == 1


# --------------------------------------------------------------------------- delivery
@dataclass
class Delivered:
    posts_inserted: int = 0
    posts_duplicate: int = 0
    profiles: int = 0


def deliver_posts_and_profiles(source: SQLiteStore, db: PipelineDB, since: datetime,
                               done: Delivered | None = None) -> Delivered:
    """Copy the posts and profiles not delivered yet, counting into `done` (so the counts so far survive
    an error part-way). Each batch is recorded as delivered only after the database committed it, so an
    interruption never loses anything (at worst a batch is offered again and its posts are skipped as
    already present)."""
    done = done if done is not None else Delivered()
    names = source.config_names()
    profiles = {p["user_id"]: p for p in source.export_profiles()}

    pending = source.pipeline_pending_posts(db.target, since)
    for start in range(0, len(pending), COPY_BATCH):
        chunk = pending[start:start + COPY_BATCH]
        rows = [post_row(p, profiles.get(p["author_id"]), names.get(p["author_id"]))
                for p in source.export_posts(chunk)]
        added = db.insert_posts(rows)
        source.pipeline_mark_posts(db.target, chunk, utc_now())
        done.posts_inserted += added
        done.posts_duplicate += len(rows) - added

    sent = source.pipeline_sent_profiles(db.target)
    for user_id, profile in profiles.items():
        row = profile_row(profile, utc_now())
        digest = profile_digest(row)
        if sent.get(user_id) == digest:
            continue                                   # this exact version is already in twitter_profiles
        outcome = db.save_profile(row, names.get(user_id))
        if outcome != "skipped":
            done.profiles += 1
        source.pipeline_mark_profile(db.target, user_id, profile_freshness(profile["refreshed_at"],
                                                                           profile["last_changed_at"]), digest, utc_now())
    return done


def profile_digest(row: dict) -> str:
    """Fingerprint of a twitter_profiles row's content (not ingested_at, which is just the write time)."""
    content = {k: v for k, v in row.items() if k != "ingested_at"}
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def deliver_runs(source: SQLiteStore, db: PipelineDB, since: datetime) -> int:
    """Add the summaries of runs not delivered yet (including the current one). Returns rows added."""
    added = 0
    for run in source.pipeline_pending_runs(db.target, since):
        added += db.insert_run(run_row(run))
        source.pipeline_mark_run(db.target, run["run_id"], utc_now())
    return added
