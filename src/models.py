"""Turn raw API objects into flat records.

The docs use two naming schemes (tweet_* in guides, post_* in the OpenAPI spec),
so every lookup accepts both. Raw objects are kept alongside the flat fields.
"""

from datetime import datetime, timezone


def _first(obj: dict, *keys, default=None):
    for key in keys:
        if obj.get(key) is not None:
            return obj[key]
    return default


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def max_id(ids) -> str | None:
    """Post IDs are numeric strings; compare numerically, not lexically."""
    ids = [i for i in ids if i]
    return max(ids, key=int) if ids else None


def min_id(ids) -> str | None:
    ids = [i for i in ids if i]
    return min(ids, key=int) if ids else None


# --- post ID <-> time ----------------------------------------------------------------
# X post IDs are "snowflakes": the top bits are milliseconds since this epoch.
SNOWFLAKE_EPOCH_MS = 1288834974657


def snowflake_lower_bound(moment: datetime) -> str:
    """A since_id meaning "posts created at or after `moment`" (since_id is exclusive)."""
    ms = int(moment.timestamp() * 1000) - SNOWFLAKE_EPOCH_MS
    return str(max((ms << 22) - 1, 0))


# --- fetch gaps ----------------------------------------------------------------------
# A gap is a range of an account's posts that is known NOT to have been collected because the
# page cap was reached: posts with since_id < id < until_id. Gaps are filled in later runs.

def new_gap(since_id: str, until_id: str, found_at: datetime) -> dict:
    return {"key": f"{since_id}-{until_id}", "since_id": since_id, "until_id": until_id,
            "found_at": found_at.isoformat(), "recovered": 0}


def in_gap(post_id: str, gap: dict) -> bool:
    return int(gap["since_id"]) < int(post_id) < int(gap["until_id"])


def apply_gap_ops(gaps: list[dict], ops: list[dict]) -> list[dict]:
    """Return a new gap list with the operations applied. Operations:
        {"add": gap}                                  record a new gap
        {"key": k, "until_id": id, "recovered": n}    shrink gap k (its newer part was collected)
        {"key": k, "close": True}                     gap k is finished
    An operation on a key this list does not have is ignored (another output's gap)."""
    result = [dict(g) for g in gaps or []]
    for op in ops or []:
        if "add" in op:
            if not any(g["key"] == op["add"]["key"] for g in result):
                result.append(dict(op["add"]))
            continue
        for gap in result:
            if gap["key"] != op["key"]:
                continue
            if op.get("close"):
                result.remove(gap)
            else:
                gap["until_id"] = op["until_id"]
                gap["recovered"] = gap.get("recovered", 0) + op.get("recovered", 0)
            break
    return result


def merge_gaps(gap_lists: list[list[dict]]) -> list[dict]:
    """Union of several outputs' gap lists for FETCHING: one entry per key, keeping the widest
    range (largest until_id) so no output misses anything. Newest gaps first."""
    merged: dict[str, dict] = {}
    for gaps in gap_lists:
        for gap in gaps or []:
            known = merged.get(gap["key"])
            if known is None or int(gap["until_id"]) > int(known["until_id"]):
                merged[gap["key"]] = dict(gap)
    return sorted(merged.values(), key=lambda g: int(g["until_id"]), reverse=True)


def normalize_user(raw: dict) -> dict:
    metrics = raw.get("public_metrics") or {}
    return {
        "user_id": raw["id"],
        "username": raw.get("username"),
        "name": raw.get("name"),
        "description": raw.get("description"),
        "location": raw.get("location"),
        "url": raw.get("url"),
        "created_at": parse_time(raw.get("created_at")),
        "verified": raw.get("verified"),
        "verified_type": raw.get("verified_type"),
        "protected": raw.get("protected"),
        "followers_count": metrics.get("followers_count"),
        "following_count": metrics.get("following_count"),
        "tweet_count": _first(metrics, "tweet_count", "post_count"),
        "listed_count": metrics.get("listed_count"),
        "profile_image_url": raw.get("profile_image_url"),
        "pinned_tweet_id": _first(raw, "pinned_tweet_id", "pinned_post_id"),
        "raw": raw,
    }


# Profile fields whose changes are recorded. Counts (followers etc.) are left out on purpose:
# they change at every refresh and would bury the meaningful changes.
TRACKED_PROFILE_FIELDS = ["username", "name", "description", "location", "url", "verified", "verified_type",
                          "protected", "profile_image_url", "pinned_tweet_id", "status"]
_BOOLEAN_FIELDS = {"verified", "protected"}


def _comparable(field: str, value):
    """Normalise a value so the same data read back from any output compares equal:
    None and "" are the same, text is trimmed, SQLite's 0/1 become False/True."""
    if value is None:
        return None
    if field in _BOOLEAN_FIELDS:
        return bool(value)
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def diff_profile(old: dict | None, new: dict, changed_at: datetime) -> list[dict]:
    """List the tracked fields that differ between the stored profile `old` and the fresh `new`.

    Returns [] for a first save (old is None). Each entry: changed_at, field, old, new.
    """
    if old is None:
        return []
    changes = []
    for field in TRACKED_PROFILE_FIELDS:
        if field not in new:
            continue
        before, after = _comparable(field, old.get(field)), _comparable(field, new.get(field))
        if before != after:
            changes.append({"changed_at": changed_at.isoformat(), "field": field, "old": before, "new": after})
    return changes


_POST_TYPE_BY_REF = {"retweeted": "retweet", "replied_to": "reply", "quoted": "quote"}


def classify_post(refs: list[dict]) -> tuple[str, str | None]:
    """Return (post_type, referenced_post_id). A retweet wins over a reply, a reply over a quote."""
    by_type = {r.get("type"): r.get("id") for r in refs or []}
    for ref_type in ("retweeted", "replied_to", "quoted"):
        if ref_type in by_type:
            return _POST_TYPE_BY_REF[ref_type], by_type[ref_type]
    return "original", None


def normalize_media(raw: dict, post_id: str) -> dict:
    return {
        "media_key": raw.get("media_key"),
        "post_id": post_id,
        "type": raw.get("type"),
        "url": raw.get("url"),
        "preview_image_url": raw.get("preview_image_url"),
        "duration_ms": raw.get("duration_ms"),
        "width": raw.get("width"),
        "height": raw.get("height"),
        "alt_text": raw.get("alt_text"),
        "variants": raw.get("variants"),
    }


def post_url(username: str | None, post_id: str) -> str:
    """Link to a post, e.g. https://x.com/timesofindia/status/2107712527866188005. X finds the post
    by its id, so the link keeps working after a rename; without a username the id-only form is used."""
    if username:
        return f"https://x.com/{username}/status/{post_id}"
    return f"https://x.com/i/status/{post_id}"


def full_text_of(raw: dict) -> str | None:
    """The complete text of a post: the long-form text (note_tweet) when there is one, else text."""
    note = _first(raw, "note_tweet", "note_post") or {}
    return note.get("text") or raw.get("text")


def normalize_post(raw: dict, media_by_key: dict[str, dict], fetched_at: datetime, username: str,
                   referenced_by_id: dict[str, dict] | None = None) -> dict:
    """`referenced_by_id` holds the tweets X returned in includes.tweets. The one this post refers to
    (see classify_post) fills the referenced_* fields; they stay None when X did not return it
    (deleted, protected or suspended) or the post refers to nothing."""
    refs = _first(raw, "referenced_tweets", "referenced_posts", default=[])
    post_type, referenced_id = classify_post(refs)
    metrics = raw.get("public_metrics") or {}
    media_keys = (raw.get("attachments") or {}).get("media_keys") or []
    referenced = (referenced_by_id or {}).get(referenced_id) if referenced_id else None

    return {
        "post_id": raw["id"],
        "author_id": raw.get("author_id"),
        "username": username,
        "created_at": parse_time(raw.get("created_at")),
        "post_url": post_url(username, raw["id"]),
        "text": raw.get("text"),
        "full_text": full_text_of(raw),
        "lang": raw.get("lang"),
        "conversation_id": raw.get("conversation_id"),
        "in_reply_to_user_id": raw.get("in_reply_to_user_id"),
        "post_type": post_type,
        "referenced_post_id": referenced_id,
        "referenced": refs or [],
        # The tweet referenced_post_id points to: a retweet's original, a reply's parent, a quoted tweet.
        "referenced_full_text": full_text_of(referenced) if referenced else None,
        "referenced_author_id": referenced.get("author_id") if referenced else None,
        "referenced_created_at": parse_time(referenced.get("created_at")) if referenced else None,
        "possibly_sensitive": raw.get("possibly_sensitive"),
        "retweet_count": _first(metrics, "retweet_count", "repost_count"),
        "reply_count": metrics.get("reply_count"),
        "like_count": metrics.get("like_count"),
        "quote_count": metrics.get("quote_count"),
        "bookmark_count": metrics.get("bookmark_count"),
        "impression_count": metrics.get("impression_count"),
        "metrics_fetched_at": fetched_at,
        "media": [normalize_media(media_by_key[k], raw["id"]) for k in media_keys if k in media_by_key],
        "raw": raw,
    }
