"""Call B: posts of one user (GET /2/users/:id/tweets) with pagination."""

import logging
from dataclasses import dataclass, field
from datetime import datetime

from .models import min_id
from .x_client import XClient

log = logging.getLogger(__name__)


@dataclass
class TimelineResult:
    posts: list[dict] = field(default_factory=list)             # raw post objects, newest first
    media_by_key: dict[str, dict] = field(default_factory=dict)
    # Tweets the posts refer to (retweet originals, reply parents, quoted tweets), from
    # includes.tweets. Kept apart from `posts`: they never move the cursor or the gaps.
    referenced_by_id: dict[str, dict] = field(default_factory=dict)
    referenced_returned: int = 0  # referenced tweets X returned, over all pages (each one is billed)
    referenced_unavailable: int = 0   # referenced tweets X could not return (deleted, protected, ...)
    pages: int = 0
    hit_page_cap: bool = False    # True: X had more (older) posts in range that were NOT fetched

    @property
    def oldest_id(self) -> str | None:
        return min_id(p.get("id") for p in self.posts)


def _is_referenced_error(error: dict) -> bool:
    """An error about a referenced tweet (e.g. a retweeted original that was deleted). It is not a
    problem with the account's own posts, which X still returned in full."""
    return str(error.get("parameter") or "").startswith(("referenced_tweets", "referenced_posts"))


def _log_unavailable(user_id: str, result: TimelineResult) -> None:
    """One line per fetch instead of one warning per deleted/protected original."""
    if result.referenced_unavailable:
        log.info("User %s: %d referenced tweet(s) not available from X (deleted, protected or suspended); "
                 "their referenced_* fields are left empty.", user_id, result.referenced_unavailable)


def fetch_new_posts(client: XClient, user_id: str, *, since_id: str | None = None,
                    start_time: datetime | None = None, until_id: str | None = None, max_pages: int = 10,
                    tweet_fields: list[str], media_fields: list[str], expansions: list[str]) -> TimelineResult:
    """Fetch posts newer than `since_id` (or newer than `start_time` on a first run), and older
    than `until_id` when given (used to fill a gap). since_id and start_time are never sent
    together. Results are newest first; we follow meta.next_token until it is absent or the
    page cap is reached.
    """
    params: dict = {
        "max_results": 100,
        "tweet.fields": tweet_fields,
        "expansions": expansions,
        "media.fields": media_fields,
    }
    if since_id:
        params["since_id"] = since_id
    elif start_time:
        params["start_time"] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    if until_id:
        params["until_id"] = until_id

    result = TimelineResult()
    next_token = None
    while result.pages < max_pages:
        if next_token:
            params["pagination_token"] = next_token
        body = client.get(f"/users/{user_id}/tweets", params)
        result.pages += 1

        result.posts.extend(body.get("data") or [])
        includes = body.get("includes") or {}
        for media in includes.get("media") or []:
            if media.get("media_key"):
                result.media_by_key[media["media_key"]] = media
        for tweet in includes.get("tweets") or includes.get("posts") or []:
            if tweet.get("id"):
                result.referenced_by_id[tweet["id"]] = tweet
                result.referenced_returned += 1
        for error in body.get("errors") or []:
            if _is_referenced_error(error):
                result.referenced_unavailable += 1
                log.debug("User %s: referenced tweet not available: %s", user_id, error.get("detail") or error)
            else:
                log.warning("Partial error for user %s: %s", user_id, error.get("detail") or error)

        next_token = (body.get("meta") or {}).get("next_token")
        if not next_token:
            _log_unavailable(user_id, result)
            return result
    _log_unavailable(user_id, result)
    result.hit_page_cap = True
    # Info only: the run reports the page cap once, as a warning, when it records the gap.
    log.info("User %s: stopped at page cap (%d pages, %d posts); older posts in range not fetched yet.",
             user_id, result.pages, len(result.posts))
    return result
