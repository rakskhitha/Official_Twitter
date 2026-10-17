"""Common interface implemented by every output (SQLite, PostgreSQL, JSON)."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class UserState:
    user_id: str
    config_username: str | None
    username: str | None
    status: str = "ok"                      # ok, renamed, protected, not_found, suspended
    active: bool = True
    since_id: str | None = None             # newest post id stored in this output
    last_profile_refresh_at: datetime | None = None
    added_at: datetime | None = None        # when this account was first registered (original wins)
    gaps: list[dict] = field(default_factory=list)  # ranges not collected yet (models.new_gap)
    last_attempt_at: datetime | None = None # last run that tried this account (ok or failed)


class Store(ABC):
    name: str = "store"
    tracks_status: bool = True   # False: output keeps no user status/config name (PostgreSQL)

    @abstractmethod
    def open(self) -> None:
        """Connect and create tables/folders if missing."""

    @abstractmethod
    def load_users(self) -> dict[str, UserState]:
        """All users this output knows about, keyed by user_id."""

    @abstractmethod
    def save_profile(self, user: dict, config_username: str, status: str, refreshed_at: datetime) -> list[dict]:
        """Register/refresh a user: overwrite its latest profile and append any changes to tracked
        fields (models.diff_profile) to its change log. Returns the changes recorded."""

    @abstractmethod
    def set_status(self, user_id: str, status: str, changed_at: datetime) -> None:
        """Set the user's status; a different status is also recorded in its change log."""

    @abstractmethod
    def set_active_users(self, active_user_ids: set[str]) -> None:
        """Mark users in the set active and every other user inactive (removed from config)."""

    @abstractmethod
    def save_posts(self, user_id: str, username: str, config_username: str, posts: list[dict],
                   new_since_id: str | None, fetched_at: datetime, gap_ops: list[dict] | None = None) -> None:
        """Upsert posts + media, advance this output's cursor and apply `gap_ops`
        (models.apply_gap_ops) to this output's own gap list, all-or-nothing."""

    @abstractmethod
    def log_run(self, summary: dict) -> None:
        ...

    def record_attempts(self, user_ids: set[str], attempted_at: datetime) -> None:
        """Remember which accounts this run actually tried (used to order the next run so that
        accounts a run did not reach go first). Outputs without user state ignore it."""

    def record_collection(self, user_id: str, collected_at: datetime, posts: int) -> None:
        """Remember when the account's posts were last collected and how many of its own posts that run
        saved (delivered as twitter_profiles.last_scraped_at / posts_last_run). Only SQLite keeps it."""

    def close(self) -> None:
        pass
