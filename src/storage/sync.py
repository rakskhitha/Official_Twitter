"""Fill other outputs in from SQLite: whatever they missed (unreachable, a failed save).

Runs at the start of every run, before the cursors are merged. The run continues from the LOWEST cursor
across the outputs, so without this an output that missed posts would make the run fetch those posts
from X again and pay for them twice. Copying is local and free; anything it cannot copy safely is still
covered by that refetch.

PostgreSQL (copy_missing_to_postgres):

Posts:    every post_id SQLite has and PostgreSQL does not (gap-recovered posts too, which are older
          than the cursor), oldest first, in batches. Posts already in PostgreSQL are never changed.
Profiles: rows where SQLite is newer (a refresh or status change PostgreSQL missed), or that PostgreSQL
          does not have. The `changes` histories of both are combined, so no entry is lost.

JSON (copy_missing_to_json): per account, JSON is missing exactly the posts after its own cursor and
inside its own gaps (its gap list is exact, see json_store). Those of SQLite's posts are appended, and its
cursor and gaps become SQLite's, so the cursors agree and nothing is fetched again. This is only done
when it is provably exact; otherwise that account is left as it is (and refetched as before):
    - JSON is AHEAD of SQLite (SQLite missed a save): JSON already has more; SQLite catches up by refetch.
    - one of SQLite's gaps only partly overlaps what JSON is missing: the resulting gaps would not be exact.
"""

import logging
from dataclasses import dataclass, field

from .json_store import JsonStore
from .sql_store import COPY_BATCH, PostgresStore, SQLiteStore, profile_freshness

log = logging.getLogger(__name__)


@dataclass
class CopyResult:
    posts: int = 0
    profiles: int = 0
    skipped_posts: int = 0      # no author_id, so no valid PostgreSQL row (left to the refetch from X)
    accounts: int = 0           # JSON: accounts filled in
    left_to_refetch: list[str] = field(default_factory=list)   # JSON: accounts that could not be filled safely


def _inside(gap: dict, low: int, high: int | None) -> bool:
    """True if every id of `gap` (since_id < id < until_id) lies in low < id < high (high None: no limit)."""
    return int(gap["since_id"]) >= low and (high is None or int(gap["until_id"]) <= high)


def _disjoint(gap: dict, low: int, high: int | None) -> bool:
    """True if no id of `gap` lies in low < id < high (high None: no limit)."""
    first, last = int(gap["since_id"]) + 1, int(gap["until_id"]) - 1   # the gap's ids
    return last < first or last <= low or (high is not None and first >= high)


def copy_missing_to_json(source: SQLiteStore, target: JsonStore) -> CopyResult:
    result = CopyResult()
    if source.posts_without_author():
        # Such posts cannot be assigned to an account, so JSON could be moved past posts it lacks.
        result.left_to_refetch.append("all accounts (SQLite has posts without author_id)")
        return result
    in_json = target.load_users()
    for user_id, sq in source.load_users().items():
        js = in_json.get(user_id)
        if sq.since_id is None:
            continue                                             # SQLite has no posts for this account
        json_cursor = int(js.since_id) if js and js.since_id else -1   # -1: JSON has none of its posts
        json_gaps = list(js.gaps) if js else []
        if json_cursor > int(sq.since_id):
            result.left_to_refetch.append(sq.username or user_id)          # JSON ahead: SQLite lags
            continue
        if json_cursor == int(sq.since_id) and sorted(g["key"] for g in json_gaps) == sorted(
                g["key"] for g in sq.gaps):
            continue                                             # already the same: the normal case

        # What JSON is missing: after its cursor, and inside its own gaps.
        missing = [(json_cursor, None)] + [(int(g["since_id"]), int(g["until_id"])) for g in json_gaps]
        new_gaps, exact = [], True
        for gap in sq.gaps:
            if any(_inside(gap, low, high) for low, high in missing):
                new_gaps.append(gap)                             # JSON lacks these ids too: keep the gap
            elif not all(_disjoint(gap, low, high) for low, high in missing):
                exact = False                                    # partly overlapping: not exact, leave it
                break
            # else: JSON already has these ids (it collected them while SQLite did not): no gap for JSON
        if not exact:
            result.left_to_refetch.append(sq.username or user_id)
            continue

        wanted = [pid for pid in source.post_ids_of(user_id)
                  if int(pid) > json_cursor or any(low < int(pid) < high for low, high in missing[1:])]
        posts = []
        for start in range(0, len(wanted), COPY_BATCH):
            posts.extend(source.export_posts(sorted(wanted, key=int)[start:start + COPY_BATCH]))
        target.fill_in(user_id, sq.username, sq.config_username, posts, sq.since_id, new_gaps)
        result.posts += len(posts)
        result.accounts += 1
    return result


def copy_missing_to_postgres(source: SQLiteStore, target: PostgresStore) -> CopyResult:
    result = CopyResult()

    missing = sorted(source.post_ids() - target.post_ids(), key=int)
    # Oldest first: if copying stops part-way, PostgreSQL's cursor only reaches the last copied post,
    # so the posts still missing are newer than it and the normal fetch covers them.
    for start in range(0, len(missing), COPY_BATCH):
        posts = source.export_posts(missing[start:start + COPY_BATCH])
        usable = [p for p in posts if p["author_id"]]
        result.skipped_posts += len(posts) - len(usable)
        result.posts += target.insert_missing_posts(usable)

    known = target.profile_freshness()
    for profile in source.export_profiles():
        fresh = profile_freshness(profile["refreshed_at"], profile["last_changed_at"])
        if profile["user_id"] not in known or (fresh is not None and (known[profile["user_id"]] is None
                                                                      or fresh > known[profile["user_id"]])):
            target.merge_profile(profile)
            result.profiles += 1
    return result
