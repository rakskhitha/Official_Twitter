import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone  # noqa: E402

from src.config import Settings  # noqa: E402
from src.models import snowflake_lower_bound  # noqa: E402


class FakeClient:
    """Stands in for XClient. Serves a fake X account database and records every call."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.users: dict[str, dict] = {}        # user_id -> raw user
        self.posts: dict[str, list[dict]] = {}  # user_id -> raw posts (any order)
        self.referenced: dict[str, dict] = {}   # post_id -> raw tweet served via referenced_tweets.id
        self.page_size = 100
        self.filter_start_time = False          # True: honour start_time (needs snowflake-style ids)
        self.credit_balance = 100.0             # USD, charged like X: per user / post returned
        self.credit_calls = 0

    def add_user(self, user_id, username, **extra):
        self.users[user_id] = {"id": user_id, "username": username, "name": username.title(),
                               "protected": False,
                               "public_metrics": {"followers_count": 10, "following_count": 2,
                                                  "tweet_count": 5, "listed_count": 0}, **extra}
        self.posts.setdefault(user_id, [])

    def add_post(self, user_id, post_id, text="hello", **extra):
        self.posts[user_id].append({"id": post_id, "text": text, "author_id": user_id,
                                    "created_at": "2026-10-04T10:00:00.000Z",
                                    "public_metrics": {"retweet_count": 1, "reply_count": 0, "like_count": 3,
                                                       "quote_count": 0, "bookmark_count": 0,
                                                       "impression_count": 50}, **extra})

    def add_referenced(self, post_id, text, author_id="900", **extra):
        """A tweet other posts refer to (e.g. a retweet's original). Missing ones act as deleted."""
        self.referenced[post_id] = {"id": post_id, "text": text, "author_id": author_id,
                                    "created_at": "2026-10-03T08:00:00.000Z", **extra}

    def get_credit_balance(self):
        """Like GET /2/usage/credits."""
        self.credit_calls += 1
        return {"data": {"total_balance": max(0.0, round(self.credit_balance, 4)),
                         "prepaid_balance": round(self.credit_balance, 4), "free_balance": 0.0, "free_grants": []}}

    def get(self, path, params=None):
        body = self._respond(path, params)
        returned = body.get("data") or []
        count = len(returned) if isinstance(returned, list) else 1
        count += len((body.get("includes") or {}).get("tweets") or [])   # expanded tweets are billed too
        price = 0.005 if path.endswith("/tweets") else 0.010   # posts vs users
        self.credit_balance -= count * price
        return body

    def _respond(self, path, params=None):
        params = dict(params or {})
        self.calls.append((path, params))
        if path == "/users/by":
            names = params["usernames"]
            names = names if isinstance(names, list) else names.split(",")
            data, errors = [], []
            for name in names:
                match = next((u for u in self.users.values() if u["username"].lower() == name.lower()), None)
                if match:
                    data.append(match)
                else:
                    errors.append({"value": name, "detail": f"Could not find user with usernames: [{name}].",
                                   "title": "Not Found Error", "resource_type": "user", "parameter": "usernames",
                                   "type": "https://api.x.com/2/problems/resource-not-found"})
            body = {"data": data} if data else {}
            if errors:
                body["errors"] = errors
            return body
        if path.startswith("/users/") and path.endswith("/tweets"):
            user_id = path.split("/")[2]
            posts = sorted(self.posts.get(user_id, []), key=lambda p: int(p["id"]), reverse=True)
            if params.get("since_id"):
                posts = [p for p in posts if int(p["id"]) > int(params["since_id"])]
            elif params.get("start_time") and self.filter_start_time:
                bound = snowflake_lower_bound(datetime.strptime(params["start_time"], "%Y-%m-%dT%H:%M:%SZ")
                                              .replace(tzinfo=timezone.utc))
                posts = [p for p in posts if int(p["id"]) > int(bound)]
            if params.get("until_id"):
                posts = [p for p in posts if int(p["id"]) < int(params["until_id"])]
            offset = int(params.get("pagination_token") or 0)
            page = posts[offset:offset + self.page_size]
            meta = {"result_count": len(page)}
            if page:
                meta["newest_id"], meta["oldest_id"] = page[0]["id"], page[-1]["id"]
            if offset + self.page_size < len(posts):
                meta["next_token"] = str(offset + self.page_size)
            body = {"meta": meta}
            if page:
                body["data"] = page
            if "referenced_tweets.id" in (params.get("expansions") or []):
                self._add_referenced(body, page)
            return body
        if path.startswith("/users/"):
            user_id = path.split("/")[2]
            if user_id in self.users:
                return {"data": self.users[user_id]}
            return {"errors": [{"title": "Not Found Error", "detail": "Could not find user",
                                "type": "https://api.x.com/2/problems/resource-not-found"}]}
        raise AssertionError(f"unexpected path {path}")

    def _add_referenced(self, body, page):
        """Like X: each referenced tweet once in includes.tweets; one that cannot be returned
        (deleted, protected) becomes an entry in `errors` instead, and the page itself is still served."""
        found, errors = {}, []
        for post in page:
            for ref in post.get("referenced_tweets") or []:
                if ref["id"] in self.referenced:
                    found[ref["id"]] = self.referenced[ref["id"]]
                elif ref["id"] not in {e["value"] for e in errors}:
                    errors.append({"value": ref["id"], "detail": f"Could not find tweet with "
                                   f"referenced_tweets.id: [{ref['id']}].", "title": "Not Found Error",
                                   "resource_type": "tweet", "parameter": "referenced_tweets.id",
                                   "type": "https://api.x.com/2/problems/resource-not-found"})
        if found:
            body.setdefault("includes", {})["tweets"] = list(found.values())
        if errors:
            body["errors"] = errors

    def timeline_calls(self):
        return [p for path, p in self.calls if path.endswith("/tweets")]


def run_messages(settings, kind: str = "errors") -> list[str]:
    """The message texts of the latest run's errors or warnings (SQLite runs.errors_json / warnings_json)."""
    import json
    import sqlite3
    conn = sqlite3.connect(settings.sqlite_path)
    try:
        value = conn.execute(f"SELECT {kind}_json FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()[0]
    finally:
        conn.close()
    return [e["message"] for e in json.loads(value or "[]")]


@pytest.fixture
def fake_client():
    return FakeClient()


def _reset_test_postgres(dsn):
    import psycopg
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS user_profiles, user_snapshots, posts")


@pytest.fixture
def make_settings(tmp_path):
    if os.environ.get("TEST_POSTGRES_DSN"):
        _reset_test_postgres(os.environ["TEST_POSTGRES_DSN"])

    def _make(usernames, **overrides):
        settings = Settings(
            usernames=usernames,
            outputs={"sqlite": True, "postgres": bool(os.environ.get("TEST_POSTGRES_DSN")), "json": True},
            sqlite_path=tmp_path / "x_monitor.db",
            json_dir=tmp_path / "json",
            log_dir=tmp_path / "logs",
            bearer_token="test-token",
            postgres_dsn=os.environ.get("TEST_POSTGRES_DSN"),
        )
        for key, value in overrides.items():
            setattr(settings, key, value)
        return settings
    return _make
