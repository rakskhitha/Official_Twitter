"""Phase 0 check: make two small real API calls and save the raw responses.

    python -m src.probe XDevelopers

Costs about $0.01 (1 user read) + up to 5 x $0.005 (post reads). Use it once to confirm
the token works and to see which field names (tweet_* vs post_*) the live API returns.
"""

import json
import sys

from .config import PROJECT_ROOT, load_settings
from .x_client import XApiError, XClient


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: python -m src.probe <username>")
        return 2
    settings = load_settings()
    client = XClient(settings.bearer_token)
    out_dir = PROJECT_ROOT / "data" / "probe"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        users = client.get("/users/by", {"usernames": argv[0], "user.fields": settings.user_fields})
    except XApiError as exc:
        print(f"User lookup failed: {exc}\n{json.dumps(exc.body, indent=2)}")
        return 1
    (out_dir / "users_by.json").write_text(json.dumps(users, indent=2, ensure_ascii=False), encoding="utf-8")
    if not users.get("data"):
        print(f"User not found: {json.dumps(users, indent=2)}")
        return 1
    user = users["data"][0]
    print(f"User OK: @{user['username']} id={user['id']} fields={sorted(user)}")

    try:
        posts = client.get(f"/users/{user['id']}/tweets", {
            "max_results": 5, "tweet.fields": settings.tweet_fields,
            "expansions": settings.expansions, "media.fields": settings.media_fields})
    except XApiError as exc:
        print(f"Timeline failed: {exc}\n{json.dumps(exc.body, indent=2)}")
        return 1
    (out_dir / "user_tweets.json").write_text(json.dumps(posts, indent=2, ensure_ascii=False), encoding="utf-8")
    data = posts.get("data") or []
    keys = sorted(set().union(*(p.keys() for p in data))) if data else []
    print(f"Timeline OK: {len(data)} post(s), post fields seen={keys}, meta={posts.get('meta')}")
    print(f"Raw responses saved in {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
