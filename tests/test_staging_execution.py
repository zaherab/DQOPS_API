"""Integration test for the staging execution path in run_check.

run_check is "the single source of truth for check execution logic, used by
both the API preview path and the Celery worker path" (its docstring), so
driving it directly exercises the worker's routing + execution without Celery.

Hermetic: both the origin and the staging store are in-process DuckDB files, so
no external database / docker network is needed.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pytest

from dq_platform.checks import staging
from dq_platform.checks.check_runner import run_check
from dq_platform.checks.staging import STAGING_TABLE, materialize_sample, staging_file_path
from dq_platform.models.check import Check, CheckType

CONN_ID = "itest-staging-conn"
SCHEMA, TABLE = "main", "requests"

# latitude is stored as TEXT on the origin (the messy real-world case): two
# valid coords and one unparseable value.
_SAMPLE_COLUMNS = [
    {"name": "unique_key", "logical_type": "integer"},
    {"name": "latitude", "logical_type": "number"},
]
_SAMPLE_ROWS = [
    {"unique_key": "1", "latitude": "40.7"},
    {"unique_key": "2", "latitude": "N/A"},  # unparseable -> TRY_CAST NULL
    {"unique_key": "3", "latitude": "41.0"},
]


def _staging_check(check_type: CheckType, column: str | None) -> MagicMock:
    c = MagicMock(spec=Check)
    c.check_type = MagicMock()
    c.check_type.value = check_type.value
    c.target_schema = SCHEMA
    c.target_table = TABLE
    c.target_column = column
    c.parameters = {}
    c.rule_parameters = None
    c.connection_id = CONN_ID
    # Real namespace (not a MagicMock) so .metadata_.get(...) returns a real dict.
    c.connection = SimpleNamespace(metadata_={"execution_mode": "staging"})
    return c


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """Point STAGING_DIR at a temp dir and materialize the typed sample."""
    monkeypatch.setattr(staging, "STAGING_DIR", str(tmp_path / "staging"))
    fp = staging_file_path(CONN_ID, SCHEMA, TABLE)
    materialize_sample(fp, _SAMPLE_COLUMNS, _SAMPLE_ROWS)
    return fp


async def test_column_check_runs_against_typed_staging_sample(staged) -> None:
    """A numeric range check on a TEXT-stored column errors with
    'operator does not exist: text >= numeric' under push-down. In staging
    mode it must run against the TRY_CAST'd DuckDB copy and produce a real
    result instead."""
    check = _staging_check(CheckType.NUMBER_IN_RANGE_PERCENT, "latitude")
    check.parameters = {"min_value": 40.4, "max_value": 41.0}
    check.rule_parameters = {"warning": {"min_percent": 100.0}, "error": {"min_percent": 99.0}}

    # Origin config is intentionally bogus — the staged column check must NOT
    # touch it. If routing were broken and it hit "origin", connecting would
    # fail loudly rather than silently passing.
    result = await run_check(check, connection_config={"type": "duckdb", "database": "/nonexistent/origin.duckdb"})

    # Ran (no text>=numeric error) against the typed sample:
    assert result.sensor_value == pytest.approx(66.67, abs=0.5)  # 2 of 3 in range, bad value -> NULL
    assert result.passed is False
    assert result.severity == "error"  # 66.67% < error band 99%
    assert STAGING_TABLE in (result.executed_sql or "")  # proves it hit main.data


async def test_table_level_check_stays_on_origin(tmp_path, monkeypatch) -> None:
    """Table-level checks (row_count) must run on origin even in staging mode —
    a 10k sample can't answer table size. Here origin is its own DuckDB file."""
    # Origin DuckDB with the real (whole) table — 5 rows.
    monkeypatch.setattr(staging, "STAGING_DIR", str(tmp_path / "staging"))
    origin = str(tmp_path / "origin.duckdb")
    con = duckdb.connect(origin)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    con.execute(f'CREATE TABLE "{SCHEMA}"."{TABLE}" (unique_key BIGINT)')
    con.executemany(f'INSERT INTO "{SCHEMA}"."{TABLE}" VALUES (?)', [(i,) for i in range(5)])
    con.close()
    # Materialize a SMALLER staging sample (2 rows) to prove row_count ignores it.
    materialize_sample(staging_file_path(CONN_ID, SCHEMA, TABLE), _SAMPLE_COLUMNS, _SAMPLE_ROWS[:2])

    check = _staging_check(CheckType.ROW_COUNT, None)
    check.rule_parameters = {"error": {"min_value": 1}}

    result = await run_check(check, connection_config={"type": "duckdb", "database": origin})

    assert result.sensor_value == 5  # origin row count, NOT the 2-row staging sample
    assert STAGING_TABLE not in (result.executed_sql or "")  # ran on origin "requests", not main.data


async def test_pushdown_mode_ignores_staging(staged) -> None:
    """With execution_mode unset, a column check runs push-down on origin even
    if a staging file happens to exist — backwards compatibility."""
    check = _staging_check(CheckType.NUMBER_IN_RANGE_PERCENT, "latitude")
    check.connection = SimpleNamespace(metadata_={})  # not staging
    check.parameters = {"min_value": 40.4, "max_value": 41.0}
    check.rule_parameters = {"error": {"min_percent": 99.0}}

    # Origin is a typed DuckDB so the push-down check still runs (this asserts
    # routing did NOT divert to the staging copy).
    origin = staged + ".origin"
    con = duckdb.connect(origin)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    con.execute(f'CREATE TABLE "{SCHEMA}"."{TABLE}" (latitude DOUBLE)')
    con.executemany(f'INSERT INTO "{SCHEMA}"."{TABLE}" VALUES (?)', [(40.7,), (40.8,), (40.9,)])
    con.close()

    result = await run_check(check, connection_config={"type": "duckdb", "database": origin})
    assert result.sensor_value == pytest.approx(100.0)  # all 3 origin rows in range
    assert STAGING_TABLE not in (result.executed_sql or "")  # used origin "requests"
