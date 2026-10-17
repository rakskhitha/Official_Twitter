import time

import pytest

from src.x_client import FatalApiError, ForbiddenApiError, TransientApiError, XApiError, XClient


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = str(self._body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.requests = []

    def get(self, url, params=None, timeout=None):
        self.requests.append((url, params))
        return self.responses.pop(0)


def make_client(responses):
    sleeps = []
    session = FakeSession(responses)
    client = XClient("tok", session=session, sleep=sleeps.append, max_retries=3)
    return client, session, sleeps


def test_list_params_are_comma_joined_and_auth_header_set():
    client, session, _ = make_client([FakeResponse(200, {"data": []})])
    client.get("/users/by", {"usernames": ["a", "b"], "since_id": None})
    url, params = session.requests[0]
    assert url == "https://api.x.com/2/users/by"
    assert params == {"usernames": "a,b"}
    assert session.headers["Authorization"] == "Bearer tok"


def test_429_waits_for_reset_then_retries():
    reset = str(int(time.time()) + 30)
    client, _, sleeps = make_client([
        FakeResponse(429, {"errors": [{"code": 88, "message": "Rate limit exceeded"}]},
                     {"x-rate-limit-reset": reset}),
        FakeResponse(200, {"data": [1]}),
    ])
    assert client.get("/x") == {"data": [1]}
    assert 25 <= sleeps[0] <= 40


def test_5xx_retries_then_transient_not_fatal():
    client, _, sleeps = make_client([FakeResponse(503)] * 3)
    with pytest.raises(TransientApiError) as info:
        client.get("/x")
    assert not isinstance(info.value, FatalApiError)          # only this request/account fails
    assert isinstance(info.value, XApiError) and info.value.status == 503
    assert len(sleeps) == 2


def test_network_errors_retry_then_transient():
    import requests

    class BrokenSession(FakeSession):
        def get(self, url, params=None, timeout=None):
            self.requests.append((url, params))
            raise requests.ConnectionError("connection refused")

    session = BrokenSession([])
    sleeps = []
    client = XClient("tok", session=session, sleep=sleeps.append, max_retries=3)
    with pytest.raises(TransientApiError, match="Network error"):
        client.get("/x")
    assert len(session.requests) == 3 and len(sleeps) == 2


def test_5xx_then_success_returns_normally():
    client, _, _ = make_client([FakeResponse(502), FakeResponse(200, {"data": [1]})])
    assert client.get("/x") == {"data": [1]}


def test_rate_limit_that_never_clears_is_fatal():
    client, _, _ = make_client([FakeResponse(429, {}, {"x-rate-limit-reset": "0"})] * 3)
    with pytest.raises(FatalApiError, match="Rate limit"):
        client.get("/x")


def test_401_is_fatal_without_retry():
    client, session, _ = make_client([FakeResponse(401, {"title": "Unauthorized", "detail": "bad token"})])
    with pytest.raises(FatalApiError, match="bad token"):
        client.get("/x")
    assert len(session.requests) == 1


def test_usage_capped_is_fatal():
    client, _, _ = make_client([FakeResponse(429, {"title": "Usage cap exceeded",
                                                   "type": "https://api.x.com/2/problems/usage-capped"})])
    with pytest.raises(FatalApiError, match="Usage cap"):
        client.get("/x")


def test_400_is_per_request_error():
    client, _, _ = make_client([FakeResponse(400, {"title": "Invalid Request", "detail": "bad field"})])
    with pytest.raises(XApiError) as info:
        client.get("/x")
    assert not isinstance(info.value, FatalApiError)
    assert info.value.status == 400


PROBLEM = "https://api.x.com/2/problems/"


@pytest.mark.parametrize("problem", ["client-forbidden", "unsupported-authentication"])
def test_app_level_403_is_fatal_with_a_clear_message(problem):
    client, session, _ = make_client([FakeResponse(403, {"title": "Forbidden", "type": PROBLEM + problem})])
    with pytest.raises(FatalApiError, match=f"403 {problem}: your app cannot use this endpoint"):
        client.get("/users/1/tweets")
    assert len(session.requests) == 1                          # no retries


def test_usage_capped_403_is_fatal():
    client, _, _ = make_client([FakeResponse(403, {"errors": [{"type": PROBLEM + "usage-capped",
                                                               "title": "Usage capped"}]})])
    with pytest.raises(FatalApiError, match="usage capped"):
        client.get("/x")


def test_account_level_403_fails_only_that_request():
    client, _, _ = make_client([FakeResponse(403, {"title": "Authorization Error",
                                                   "type": PROBLEM + "not-authorized-for-resource"})])
    with pytest.raises(ForbiddenApiError, match="not-authorized-for-resource") as info:
        client.get("/users/1/tweets")
    assert not isinstance(info.value, FatalApiError) and info.value.status == 403


@pytest.mark.parametrize("body", [{}, {"errors": [{"code": 200, "message": "Forbidden."}]}, "not json"])
def test_403_without_a_known_type_fails_only_that_request(body):
    client, _, _ = make_client([FakeResponse(403, body)])
    with pytest.raises(ForbiddenApiError) as info:
        client.get("/x")
    assert not isinstance(info.value, FatalApiError)


def test_problem_type_matching_is_exact():
    from src.x_client import _has_problem_type
    assert _has_problem_type({"type": PROBLEM + "client-forbidden"}, "client-forbidden") == "client-forbidden"
    assert _has_problem_type({"type": PROBLEM + "not-client-forbidden"}, "client-forbidden") is None
    assert _has_problem_type({"errors": [{"type": PROBLEM + "usage-capped"}]}, "usage-capped") == "usage-capped"
