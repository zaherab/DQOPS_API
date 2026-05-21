"""Dialect-level SQL validation for the profiler SQL builders.

For every supported connector, generate the profile + sample SQL and parse
it with sqlglot using that dialect's grammar. This catches syntax-level
dialect errors (wrong quoting, unsupported clause shape, bad function call
structure) without needing a live warehouse.

It does NOT catch semantic errors (function-overload mismatches like
`MIN(boolean)`) — only live execution does. Live coverage: Postgres +
DuckDB + MySQL + SQL Server + Oracle (see test_sql_dialects_live.py).
BigQuery / Snowflake / Redshift / Databricks are syntax-verified here.

Also asserts SQL opacity: no check-type identifier strings leak into the
emitted SQL (IP-protection requirement).
"""

from __future__ import annotations

import pytest
import sqlglot

from dq_platform.connectors.base import BaseConnector
from dq_platform.connectors.bigquery import BigQueryConnector
from dq_platform.connectors.databricks import DatabricksConnector
from dq_platform.connectors.duckdb_connector import DuckDBConnector
from dq_platform.connectors.mysql import MySQLConnector
from dq_platform.connectors.oracle import OracleConnector
from dq_platform.connectors.postgresql import PostgreSQLConnector
from dq_platform.connectors.redshift import RedshiftConnector
from dq_platform.connectors.snowflake import SnowflakeConnector
from dq_platform.connectors.sqlserver import SQLServerConnector

# connector class → sqlglot dialect name
_DIALECTS: dict[type[BaseConnector], str] = {
    PostgreSQLConnector: "postgres",
    RedshiftConnector: "redshift",
    MySQLConnector: "mysql",
    SQLServerConnector: "tsql",
    OracleConnector: "oracle",
    BigQueryConnector: "bigquery",
    SnowflakeConnector: "snowflake",
    DuckDBConnector: "duckdb",
    DatabricksConnector: "databricks",
}


def _builder(cls: type[BaseConnector]) -> BaseConnector:
    """A connector whose SQL-builder methods work without connecting.

    The SQL builders only touch quote_identifier / _cast_text_expr /
    _hash_mod_expr — all pure. Binding them onto a bare instance skips
    __init__ (config parsing, SSRF host validation) entirely.
    """
    inst = object.__new__(cls)  # bypass __init__
    return inst


_COLUMNS = [
    {"name": "id", "logical_type": "integer"},
    {"name": "email", "logical_type": "string", "classification": "PII"},
    {"name": "amount", "logical_type": "number"},
    {"name": "is_active", "logical_type": "boolean"},
    {"name": "created_at", "logical_type": "date"},
]
_SAMPLE_COLS = ["id", "amount", "created_at"]

# Substrings that must never appear in customer-facing SQL — they'd reveal
# the check intent. Aliases are opaque (c0_a1...), so none of these leak.
_OPACITY_DENYLIST = [
    "nulls_percent",
    "distinct_percent",
    "check_type",
    "text_found_in_set",
    "data_freshness",
    "dimension",
    "rule_parameters",
]


@pytest.mark.parametrize(
    "cls,dialect",
    list(_DIALECTS.items()),
    ids=[d for d in _DIALECTS.values()],
)
class TestProfileSqlParses:
    def test_profile_sql_parses(self, cls: type[BaseConnector], dialect: str) -> None:
        conn = _builder(cls)
        sql = conn.build_profile_sql("myschema", "mytable", _COLUMNS)
        # sqlglot raises ParseError on a structural problem.
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        assert parsed is not None

    def test_sample_sql_parses(self, cls: type[BaseConnector], dialect: str) -> None:
        conn = _builder(cls)
        sql = conn.build_sample_sql("myschema", "mytable", _SAMPLE_COLS, pk="id", n=1000, seed=7)
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        assert parsed is not None

    def test_profile_sql_is_opaque(self, cls: type[BaseConnector], dialect: str) -> None:
        conn = _builder(cls)
        sql = conn.build_profile_sql("myschema", "mytable", _COLUMNS).lower()
        for term in _OPACITY_DENYLIST:
            assert term not in sql, f"{dialect}: '{term}' leaked into profile SQL"

    def test_sample_sql_is_opaque(self, cls: type[BaseConnector], dialect: str) -> None:
        conn = _builder(cls)
        sql = conn.build_sample_sql("myschema", "mytable", _SAMPLE_COLS, pk="id", n=1000, seed=7).lower()
        for term in _OPACITY_DENYLIST:
            assert term not in sql, f"{dialect}: '{term}' leaked into sample SQL"

    def test_pii_column_skips_minmax(self, cls: type[BaseConnector], dialect: str) -> None:
        # The PII column (email) must not get MIN/MAX/DISTINCT — only count.
        conn = _builder(cls)
        sql = conn.build_profile_sql("myschema", "mytable", _COLUMNS)
        qcol = conn.quote_identifier("email")
        assert f"MIN({qcol})" not in sql
        assert f"MAX({qcol})" not in sql
        assert f"COUNT(DISTINCT {qcol})" not in sql

    def test_boolean_column_skips_minmax(self, cls: type[BaseConnector], dialect: str) -> None:
        # is_active is boolean — MIN/MAX undefined on most engines.
        conn = _builder(cls)
        sql = conn.build_profile_sql("myschema", "mytable", _COLUMNS)
        qcol = conn.quote_identifier("is_active")
        assert f"MIN({qcol})" not in sql
        assert f"MAX({qcol})" not in sql
