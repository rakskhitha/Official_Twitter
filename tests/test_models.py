from datetime import datetime, timezone

from src.models import classify_post, max_id, normalize_post, normalize_user

NOW = datetime(2026, 10, 5, tzinfo=timezone.utc)


def test_max_id_is_numeric_not_lexical():
    assert max_id(["9", "10", None]) == "10"
    assert max_id([]) is None


def test_classify_post():
    assert classify_post([]) == ("original", None)
    assert classify_post([{"type": "retweeted", "id": "1"}]) == ("retweet", "1")
    assert classify_post([{"type": "quoted", "id": "2"}, {"type": "replied_to", "id": "3"}]) == ("reply", "3")
    assert classify_post([{"type": "quoted", "id": "2"}]) == ("quote", "2")


def test_long_form_post_uses_note_tweet():
    raw = {"id": "5", "text": "short…", "note_tweet": {"text": "the full long text"}}
    post = normalize_post(raw, {}, NOW, "alice")
    assert post["text"] == "short…"
    assert post["full_text"] == "the full long text"


def test_post_naming_variants_from_openapi_spec():
    raw = {"id": "6", "text": "t", "note_post": {"text": "full"},
           "referenced_posts": [{"type": "retweeted", "id": "1"}],
           "public_metrics": {"repost_count": 7}}
    post = normalize_post(raw, {}, NOW, "alice")
    assert post["full_text"] == "full"
    assert post["post_type"] == "retweet"
    assert post["retweet_count"] == 7


def test_media_attached_to_post():
    raw = {"id": "7", "text": "pic", "attachments": {"media_keys": ["3_7", "missing"]}}
    media = {"3_7": {"media_key": "3_7", "type": "photo", "url": "https://x/img.jpg"}}
    post = normalize_post(raw, media, NOW, "alice")
    assert [m["media_key"] for m in post["media"]] == ["3_7"]
    assert post["media"][0]["post_id"] == "7"


def test_user_tweet_count_variants():
    user = normalize_user({"id": "1", "username": "a", "public_metrics": {"post_count": 42}})
    assert user["tweet_count"] == 42


def test_diff_profile_first_save_and_no_change():
    from src.models import diff_profile
    user = {"username": "a", "name": "A", "verified": True, "status": "ok", "followers_count": 5}
    assert diff_profile(None, user, NOW) == []
    assert diff_profile(dict(user), dict(user), NOW) == []


def test_diff_profile_reports_tracked_fields_only():
    from src.models import diff_profile
    old = {"username": "a", "description": "old bio", "verified": True, "followers_count": 5, "status": "ok"}
    new = {"username": "a2", "description": "new bio", "verified": False, "followers_count": 999, "status": "ok"}
    changes = diff_profile(old, new, NOW)
    assert [(c["field"], c["old"], c["new"]) for c in changes] == [
        ("username", "a", "a2"), ("description", "old bio", "new bio"), ("verified", True, False)]
    assert all(c["changed_at"] == NOW.isoformat() for c in changes)      # follower count ignored


def test_diff_profile_normalises_equivalent_values():
    from src.models import diff_profile
    old = {"location": "", "description": " bio ", "verified": 1, "protected": 0, "url": None}
    new = {"location": None, "description": "bio", "verified": True, "protected": False, "url": ""}
    assert diff_profile(old, new, NOW) == []
