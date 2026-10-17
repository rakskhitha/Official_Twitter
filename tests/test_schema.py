"""The schema loader must turn both schema files into clean CREATE statements.

Runs without any database, so it can never be skipped (unlike tests/test_postgres.py). It guards
against the bug where a ';' inside an SQL comment cut a statement in half.
"""

import re
import sqlite3

import pytest

from src.storage.sql_store import (POSTGRES_SCHEMA_PATH, SCHEMA_PATH, PostgresStore, SQLiteStore,
                                   schema_statements, split_sql)


def test_semicolon_inside_a_comment_does_not_split():
    script = "-- note; with a semicolon\nCREATE TABLE a (x TEXT); -- trailing; comment\nCREATE TABLE b (y TEXT);"
    assert split_sql(script) == ["CREATE TABLE a (x TEXT)", "CREATE TABLE b (y TEXT)"]


def test_quoted_text_is_left_untouched():
    script = "CREATE TABLE a (x TEXT DEFAULT 'a;b--c', y TEXT DEFAULT 'it''s');"
    assert split_sql(script) == ["CREATE TABLE a (x TEXT DEFAULT 'a;b--c', y TEXT DEFAULT 'it''s')"]


def test_comment_only_and_empty_parts_are_dropped():
    assert split_sql("-- only a comment\n\n;;\n") == []


def test_unterminated_quote_is_reported():
    with pytest.raises(ValueError):
        split_sql("CREATE TABLE a (x TEXT DEFAULT 'oops);")


@pytest.mark.parametrize("path, types, tables", [
    (SCHEMA_PATH, SQLiteStore.types, {"monitored_users", "user_profiles", "posts", "media", "runs",
                                      "pipeline_targets", "pipeline_sent_posts", "pipeline_sent_profiles",
                                      "pipeline_sent_runs", "target_list_cache"}),
    (POSTGRES_SCHEMA_PATH, PostgresStore.types, {"user_profiles", "posts"}),
])
def test_both_schema_files_give_clean_create_statements(path, types, tables):
    statements = schema_statements(path, types)
    for statement in statements:
        assert re.match(r"CREATE (TABLE|INDEX) IF NOT EXISTS ", statement), statement[:80]
        assert "--" not in statement and ";" not in statement
        assert "{" not in statement and "}" not in statement          # every placeholder filled in
        assert statement.count("(") == statement.count(")")
    created = {re.match(r"CREATE TABLE IF NOT EXISTS (\w+)", s).group(1)
               for s in statements if s.startswith("CREATE TABLE")}
    assert created == tables


def test_postgres_schema_parses_as_sql_statements():
    """SQLite's parser accepts these PostgreSQL statements' structure once the PostgreSQL-only
    types/defaults are mapped, which catches broken statements without a PostgreSQL server."""
    conn = sqlite3.connect(":memory:")
    for statement in schema_statements(POSTGRES_SCHEMA_PATH, {}):
        conn.execute(statement.replace("JSONB", "TEXT").replace("TIMESTAMPTZ", "TEXT"))
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert names == {"user_profiles", "posts"}
