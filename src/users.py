"""Call A: user profile lookup (GET /2/users/by, GET /2/users/:id)."""

import logging
from dataclasses import dataclass, field

from .x_client import FatalApiError, XApiError, XClient

log = logging.getLogger(__name__)

BATCH_SIZE = 100


@dataclass
class LookupResult:
    users: list[dict] = field(default_factory=list)                 # raw user objects
    not_found: dict[str, str] = field(default_factory=dict)         # lower(username) -> reason
    failed: dict[str, str] = field(default_factory=dict)            # lower(username) -> request error;
                                                                    # NOT "not found": X gave no answer


def _reason(error: dict) -> str:
    text = f"{error.get('title', '')} {error.get('detail', '')} {error.get('type', '')}".lower()
    if "suspend" in text:
        return "suspended"
    return "not_found"


def lookup_by_usernames(client: XClient, usernames: list[str], user_fields: list[str]) -> LookupResult:
    """Batch lookup in chunks of 100. Missing/suspended users come back in `errors` with HTTP 200.

    A chunk whose request fails (e.g. HTTP 400) is recorded in `failed` and the other chunks
    still run. FatalApiError (bad token, no credits) is not caught: the run must stop.
    """
    result = LookupResult()
    for start in range(0, len(usernames), BATCH_SIZE):
        chunk = usernames[start:start + BATCH_SIZE]
        try:
            body = client.get("/users/by", {"usernames": chunk, "user.fields": user_fields})
        except FatalApiError:
            raise
        except XApiError as exc:
            for name in chunk:
                result.failed[name.lower()] = str(exc)
            # Info only: the run reports failed lookups once, as an error ("Profile lookup failed ...").
            log.info("User lookup request failed for %d user(s): %s", len(chunk), exc)
            continue
        result.users.extend(body.get("data") or [])
        for error in body.get("errors") or []:
            name = error.get("value") or error.get("resource_id") or ""
            if name:
                result.not_found[str(name).lower()] = _reason(error)
            # Info only: the run reports each account not found once (new user, renamed, suspended ...).
            log.info("User lookup error: %s", error.get("detail") or error)

    found = {u["username"].lower() for u in result.users if u.get("username")}
    for name in usernames:
        key = name.lower()
        if key not in found and key not in result.not_found and key not in result.failed:
            result.not_found[key] = "not_found"
    return result


def lookup_by_id(client: XClient, user_id: str, user_fields: list[str]) -> tuple[dict | None, str | None]:
    """Return (user, None) or (None, reason). Used to detect renames of known users.

    Raises XApiError for request failures other than 404; the caller decides what to do.
    """
    try:
        body = client.get(f"/users/{user_id}", {"user.fields": user_fields})
    except XApiError as exc:
        if exc.status == 404:
            return None, "not_found"
        raise
    if body.get("data"):
        return body["data"], None
    errors = body.get("errors") or [{}]
    return None, _reason(errors[0])
