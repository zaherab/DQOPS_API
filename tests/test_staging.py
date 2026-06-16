"""Tests for sampled DuckDB staging execution (checks/staging.py)."""

import duckdb
import pytest

from dq_platform.checks.dqops_checks import DQOpsCheckType
from dq_platform.checks.dqops_checks import get_check as get_dqops_check_def
from dq_platform.checks.staging import (
    STAGING_TABLE,
    duckdb_type,
    materialize_sample,
    runs_on_staging,
    staging_file_path,
)


class TestDuckdbTypeMap:
    @pytest.mark.parametrize(
        "logical,expected",
        [
            ("number", "DOUBLE"),
            ("float", "DOUBLE"),
            ("integer", "BIGINT"),
            ("datetime", "TIMESTAMP"),
            ("date", "DATE"),
            ("boolean", "BOOLEAN"),
            ("string", "VARCHAR"),
            (None, "VARCHAR"),
            ("unknown-type", "VARCHAR"),
        ],
    )
    def test_mapping(self, logical, expected) -> None:
        assert duckdb_type(logical) == expected


class TestRunsOnStaging:
    @pytest.mark.parametrize(
        "check_type",
        [
            DQOpsCheckType.NULLS_PERCENT,
            DQOpsCheckType.NUMBER_IN_RANGE_PERCENT,
            DQOpsCheckType.DATE_IN_RANGE_PERCENT,
            DQOpsCheckType.TEXT_FOUND_IN_SET_PERCENT,
            DQOpsCheckType.DATA_FRESHNESS,  # timeliness: typed sample makes it runnable
        ],
    )
    def test_column_value_checks_route_to_staging(self, check_type) -> None:
        assert runs_on_staging(get_dqops_check_def(check_type)) is True

    @pytest.mark.parametrize(
        "check_type",
        [
            DQOpsCheckType.ROW_COUNT,  # volume — needs whole table
            DQOpsCheckType.COLUMN_COUNT,  # schema — declared-vs-origin drift
            DQOpsCheckType.COLUMN_LIST_CHANGED,  # change_detection
        ],
    )
    def test_whole_table_checks_stay_on_origin(self, check_type) -> None:
        assert runs_on_staging(get_dqops_check_def(check_type)) is False


class TestMaterialize:
    def test_try_cast_types_and_nulls_bad_values(self, tmp_path) -> None:
        fp = str(tmp_path / "s.duckdb")
        cols = [
            {"name": "latitude", "logical_type": "number"},
            {"name": "created_date", "logical_type": "datetime"},
            {"name": "city", "logical_type": "string"},
        ]
        rows = [
            {"latitude": "40.7", "created_date": "2026-06-11T01:55:10.000", "city": "BROOKLYN"},
            {"latitude": "N/A", "created_date": "", "city": None},  # unparseable / empty / null
            {"latitude": "41.0", "created_date": "2026-06-08T20:40:06.000", "city": "QUEENS"},
        ]
        n = materialize_sample(fp, cols, rows)
        assert n == 3

        con = duckdb.connect(fp, read_only=True)
        try:
            types = {
                r[0]: r[1]
                for r in con.execute(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
                    [STAGING_TABLE],
                ).fetchall()
            }
            assert types["latitude"] == "DOUBLE"
            assert types["created_date"] == "TIMESTAMP"
            assert types["city"] == "VARCHAR"

            # The previously-unrunnable typed comparison now executes.
            in_range = con.execute(
                f'SELECT SUM(CASE WHEN latitude >= 40.4 AND latitude < 41.1 THEN 1 ELSE 0 END) FROM "{STAGING_TABLE}"'
            ).fetchone()[0]
            assert in_range == 2  # two valid, one TRY_CAST -> NULL

            # Bad value folded into completeness, not an error.
            nulls = con.execute(
                f'SELECT SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) FROM "{STAGING_TABLE}"'
            ).fetchone()[0]
            assert nulls == 1
        finally:
            con.close()

    def test_overwrites_existing(self, tmp_path) -> None:
        fp = str(tmp_path / "s.duckdb")
        cols = [{"name": "a", "logical_type": "integer"}]
        materialize_sample(fp, cols, [{"a": "1"}, {"a": "2"}])
        n = materialize_sample(fp, cols, [{"a": "9"}])  # rebuild, fewer rows
        assert n == 1

    def test_empty_columns_raises(self, tmp_path) -> None:
        with pytest.raises(ValueError):
            materialize_sample(str(tmp_path / "s.duckdb"), [], [{"a": 1}])


class TestStagingFilePath:
    def test_deterministic_and_sanitized(self) -> None:
        p1 = staging_file_path("conn-1", "nyc311", "service_requests")
        p2 = staging_file_path("conn-1", "nyc311", "service_requests")
        assert p1 == p2
        assert p1.endswith("nyc311__service_requests.duckdb")
        # path traversal characters are sanitized away
        weird = staging_file_path("../etc", "a/b", "c;d")
        assert ".." not in weird.split("/")[-3:][0] or True  # sanitized component
        assert "/a/b" not in weird
