"""Thin HTTP client for the X API v2 with retry, rate-limit and error handling."""

import logging
import random
import time

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.x.com/2"
MAX_RATE_LIMIT_SLEEP = 16 * 60  # one 15-minute window plus margin
DEFAULT_TIMEOUT = (10.0, 30.0)  # seconds: (connect, each read)
CREDITS_PATH = "/usage/credits"  # GET /2/usage/credits: credit balance in USD


class RunTimeLimitReached(Exception):
    """The run's time budget is used up (or a wait would cross it). Deliberately NOT an
    XApiError, so per-request/per-account handlers do not swallow it: the run stops cleanly."""


class XApiError(Exception):
    """A request failed. Affects only the current user; the run can continue."""

    def __init__(self, message: str, status: int | None = None, body: object = None):
        super().__init__(message)
        self.status = status
        self.body = body


class TransientApiError(XApiError):
    """Server (5xx) or network errors that did not clear after all retries. Only the current
    request/account fails; the run counts these to detect an outage (run_daily)."""


class ForbiddenApiError(XApiError):
    """HTTP 403 that is NOT an app-level problem (e.g. not-authorized-for-resource, or no/unknown
    error type). Only the current request/account fails; the run counts these too, because many in
    a row point to an app-wide access problem that X did not label clearly."""


class FatalApiError(XApiError):
    """Bad token (401), app-level 403, usage cap / no credits, or a rate limit that never clears:
    these affect every account, so the whole run must stop."""


# 403 error types that mean the APP cannot use the endpoint at all (every account is affected).
APP_LEVEL_403_TYPES = ("client-forbidden", "unsupported-authentication", "usage-capped")


def _problem_text(body: object) -> str:
    """Summarize both error shapes: problem-details and the old {"errors":[{code,message}]}."""
    if not isinstance(body, dict):
        return str(body)[:500]
    parts = []
    if body.get("title") or body.get("detail"):
        parts.append(f"{body.get('title', '')}: {body.get('detail', '')}".strip(": "))
    for err in body.get("errors", []) or []:
        if isinstance(err, dict):
            parts.append(err.get("message") or err.get("detail") or err.get("title") or str(err))
    return "; ".join(parts) or str(body)[:500]


def _problem_types(body: object) -> list[str]:
    """Error `type` values from both error shapes, e.g. "https://api.x.com/2/problems/client-forbidden"."""
    if not isinstance(body, dict):
        return []
    types = [body.get("type")] + [e.get("type") for e in body.get("errors", []) or [] if isinstance(e, dict)]
    return [t for t in types if isinstance(t, str) and t]


def _has_problem_type(body: object, *names: str) -> str | None:
    """Return the first of `names` found among the body's error types (matched by name)."""
    for problem_type in _problem_types(body):
        for name in names:
            if problem_type.rstrip("/").endswith("/" + name) or problem_type == name:
                return name
    return None


def _is_usage_capped(body: object) -> bool:
    return _has_problem_type(body, "usage-capped") is not None


class XClient:
    def __init__(self, bearer_token: str, base_url: str = BASE_URL, timeout=DEFAULT_TIMEOUT,
                 max_retries: int = 5, session: requests.Session | None = None, sleep=time.sleep):
        if not bearer_token:
            raise FatalApiError("X_BEARER_TOKEN is not set (add it to .env)")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "x-daily-monitor/1.0",
        })
        self._sleep = sleep
        self.request_count = 0
        # time.monotonic() value after which no new request or wait may start (set by the run).
        self.deadline: float | None = None

    def _check_deadline(self, path: str) -> None:
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise RunTimeLimitReached(f"run time limit reached before requesting {path}")

    def _sleep_within_deadline(self, seconds: float, reason: str, message: str) -> None:
        """Sleep, unless the wait would end after the run's deadline: then stop the run instead
        (the next run starts fresh, e.g. after the rate limit has reset)."""
        if self.deadline is not None and time.monotonic() + seconds >= self.deadline:
            raise RunTimeLimitReached(f"{reason}: waiting {seconds:.0f} s would pass the run's time limit")
        log.warning(message)
        self._sleep(seconds)

    def get_credit_balance(self) -> dict:
        """GET /2/usage/credits: the account's credit balance in USD. One attempt only and never
        held back by the run's time limit, so it can never delay or stop a run (callers treat any
        failure as "balance unknown")."""
        return self.get(CREDITS_PATH, respect_deadline=False, max_attempts=1)

    def get(self, path: str, params: dict | None = None, *, respect_deadline: bool = True,
            max_attempts: int | None = None) -> dict:
        """GET a v2 endpoint. List params are joined with commas. Returns the parsed JSON body."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        query = {k: ",".join(v) if isinstance(v, (list, tuple)) else v
                 for k, v in (params or {}).items() if v is not None and v != []}
        attempts = max_attempts or self.max_retries

        for attempt in range(1, attempts + 1):
            if respect_deadline:
                self._check_deadline(path)
            try:
                resp = self.session.get(url, params=query, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == attempts:
                    raise TransientApiError(f"Network error on {path} after {attempt} attempts: {exc}") from exc
                self._backoff(attempt, f"network error: {exc}")
                continue

            self.request_count += 1
            self._log_rate_limit(path, resp)
            body = self._json(resp)

            if resp.status_code == 200:
                return body if isinstance(body, dict) else {}

            if resp.status_code == 429:
                if _is_usage_capped(body):
                    raise FatalApiError(f"Usage cap / spending limit reached: {_problem_text(body)}",
                                        resp.status_code, body)
                if attempt == attempts:
                    raise FatalApiError(f"Rate limit still exceeded after {attempt} attempts on {path}",
                                        resp.status_code, body)
                self._wait_for_reset(resp, attempt)
                continue

            if resp.status_code in (500, 502, 503, 504):
                if attempt == attempts:
                    raise TransientApiError(f"Server error {resp.status_code} on {path} after {attempt} attempts",
                                            resp.status_code, body)
                self._backoff(attempt, f"HTTP {resp.status_code}")
                continue

            if resp.status_code == 401:
                raise FatalApiError(f"HTTP 401 (check X_BEARER_TOKEN): {_problem_text(body)}",
                                    resp.status_code, body)
            if resp.status_code == 403:
                self._raise_forbidden(path, resp.status_code, body)
            if resp.status_code == 402 or _is_usage_capped(body):
                raise FatalApiError(f"Out of credits / usage capped: {_problem_text(body)}", resp.status_code, body)

            raise XApiError(f"HTTP {resp.status_code} on {path}: {_problem_text(body)}", resp.status_code, body)

        raise TransientApiError(f"Retries exhausted on {path}")  # pragma: no cover

    @staticmethod
    def _raise_forbidden(path: str, status: int, body: object) -> None:
        """403: decide by X's error type whether the whole app or only this request is affected."""
        app_level = _has_problem_type(body, *APP_LEVEL_403_TYPES)
        if app_level == "usage-capped":
            raise FatalApiError(f"Out of credits / usage capped: {_problem_text(body)}", status, body)
        if app_level:
            raise FatalApiError(f"HTTP 403 {app_level}: your app cannot use this endpoint (check the app's "
                                f"project, plan and access in console.x.com): {_problem_text(body)}", status, body)
        kind = ", ".join(t.rsplit("/", 1)[-1] for t in _problem_types(body)) or "no error type"
        raise ForbiddenApiError(f"HTTP 403 on {path} ({kind}): {_problem_text(body)}", status, body)

    @staticmethod
    def _json(resp: requests.Response) -> object:
        try:
            return resp.json()
        except ValueError:
            return resp.text

    @staticmethod
    def _log_rate_limit(path: str, resp: requests.Response) -> None:
        remaining = resp.headers.get("x-rate-limit-remaining")
        if remaining is not None:
            log.debug("rate limit %s: %s/%s left, reset at %s", path, remaining,
                      resp.headers.get("x-rate-limit-limit"), resp.headers.get("x-rate-limit-reset"))

    def _wait_for_reset(self, resp: requests.Response, attempt: int) -> None:
        reset = resp.headers.get("x-rate-limit-reset")
        try:
            wait = int(reset) - time.time() + 2
        except (TypeError, ValueError):
            wait = 60 * attempt
        wait = min(max(wait, 5), MAX_RATE_LIMIT_SLEEP)
        self._sleep_within_deadline(wait, "rate limited (429)",
                                    f"Rate limited (429). Sleeping {wait:.0f} s before retry {attempt + 1}.")

    def _backoff(self, attempt: int, reason: str) -> None:
        wait = min(2 ** attempt, 60) + random.uniform(0, 1)
        self._sleep_within_deadline(wait, f"retry after {reason}",
                                    f"Retrying after {reason} in {wait:.1f} s "
                                    f"(attempt {attempt + 1}/{self.max_retries}).")
