"""Sampled DuckDB staging execution for data-quality checks.

Opt-in per connection (``Connection.metadata_["execution_mode"] == "staging"``).
A bounded, deterministic sample of the declared (Content-Schema) columns is
copied into a typed DuckDB file; column-level *data* checks then run against the
typed copy instead of being pushed down to the origin DB.

Why typed-copy fixes the "unrunnable check" bug: the origin may store a numeric
or date field as TEXT, so a sensor like ``"latitude" >= 40.4`` errors with
``operator does not exist: text >= numeric``. Copying with ``TRY_CAST`` to the
declared logical type makes the column properly typed; genuinely unparseable
values become NULL and are caught by the completeness check instead of erroring
the whole query.

Routing: only column-level *value* checks move to the sample. Table-level checks
(``row_count``, ``column_count``/schema-drift, ``data_freshness``, duplicate
record, availability) and change/anomaly checks stay on origin — they need the
whole table or origin metadata to be truthful, and a 10k sample cannot answer
them. See :func:`runs_on_staging`.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Base directory for staging DuckDB files. The /shared volume is mounted on both
# the api (writer) and worker (reader) containers — see docker-compose.yml.
STAGING_DIR = os.getenv("DQ_STAGING_DIR", "/shared/staging")

# Fixed schema/table the sample is materialized into inside each DuckDB file.
STAGING_SCHEMA = "main"
STAGING_TABLE = "data"

# Declared logical type → DuckDB physical type. Anything unmapped stays VARCHAR
# (safe: text checks still run; a numeric check on a VARCHAR would be a routing
# bug, not a type-map bug).
_LOGICAL_TO_DUCKDB: dict[str, str] = {
    "string": "VARCHAR",
    "text": "VARCHAR",
    "integer": "BIGINT",
    "int": "BIGINT",
    "number": "DOUBLE",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "decimal": "DOUBLE",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMP",
}

# Categories that MUST stay on origin even in staging mode. A 10k sample cannot
# answer these truthfully (whole-table aggregates / origin metadata / history).
_ORIGIN_ONLY_CATEGORIES: frozenset[str] = frozenset(
    {
        "volume",  # row_count*: sample size, not table size
        "schema",  # column_count / column_list_changed: declared-vs-origin drift
        "datatype",  # detected_datatype_*: the copy's type is forced by TRY_CAST
        "availability",  # table_availability: must hit the real table
        "comparison",  # cross-source: reference side isn't in the sample
        "referential",  # foreign_key_*: missing FK may be outside the sample
        "change",  # change/anomaly baselines must be mode-invariant
        "change_detection",
        "anomaly",
    }
)
# NOTE: "timeliness" (data_freshness/staleness) runs on staging. On an untyped
# origin the freshness column is TEXT and the origin sensor errors
# (`timestamp - text`); the typed sample has a real TIMESTAMP, so it runs there.
# MAX over a uniform sample can only under-estimate the true max, so freshness
# is conservatively biased toward "staler" — it never hides staleness.


def duckdb_type(logical_type: str | None) -> str:
    """Map a declared logical type to a DuckDB column type (default VARCHAR)."""
    if not logical_type:
        return "VARCHAR"
    return _LOGICAL_TO_DUCKDB.get(logical_type.strip().lower(), "VARCHAR")


def runs_on_staging(check_def: Any) -> bool:
    """True if a check should execute against the typed sample, not origin.

    Only column-level *value* checks (nulls, numeric, geographic, datetime,
    text, patterns, accepted_values, uniqueness, statistical, boolean, pii)
    route to staging — exactly the set that (a) a uniform sample estimates and
    (b) benefits from copy-time typing. Table-level / schema / timeliness /
    change checks stay on origin.
    """
    return bool(getattr(check_def, "is_column_level", False)) and (
        getattr(check_def, "category", None) not in _ORIGIN_ONLY_CATEGORIES
    )


def staging_file_path(connection_id: str, schema: str, table: str) -> str:
    """Deterministic DuckDB file path for one (connection, schema, table)."""

    def _safe(part: str) -> str:
        return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(part))

    name = f"{_safe(schema)}__{_safe(table)}.duckdb"
    return os.path.join(STAGING_DIR, _safe(connection_id), name)


def staging_connection_config(file_path: str, *, read_only: bool = True) -> dict[str, Any]:
    """DuckDB connection_config pointing at a materialized staging file."""
    return {
        "type": "duckdb",
        "connection_type": "duckdb",
        "database": file_path,
        "read_only": read_only,
    }


def _to_text(value: Any) -> str | None:
    """Coerce a sample value to text for the all-VARCHAR landing table.

    None stays None (SQL NULL). Everything else is stringified so the
    subsequent TRY_CAST decides typeability uniformly (mirrors how the value
    would arrive from a TEXT origin column).
    """
    if value is None:
        return None
    return str(value)


def materialize_sample(
    file_path: str,
    columns: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> int:
    """Write a typed ``main.data`` table into ``file_path`` from sample rows.

    ``columns`` is ``[{"name": str, "logical_type": str | None}, ...]`` (declared
    order). Rows are loaded into an all-VARCHAR landing table, then projected
    with ``TRY_CAST`` to the declared types — unparseable values become NULL.

    Returns the number of rows materialized. Overwrites any existing file.
    """
    import duckdb

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Fresh file each materialize — the fingerprint cache decides when to call
    # this, so reaching here means "rebuild".
    for stale in (file_path, f"{file_path}.wal"):
        if os.path.exists(stale):
            os.remove(stale)

    names = [c["name"] for c in columns]
    if not names:
        raise ValueError("materialize_sample requires at least one declared column")

    con = duckdb.connect(file_path)
    try:
        raw_cols = ", ".join(f'"{n}" VARCHAR' for n in names)
        con.execute(f"CREATE TABLE _raw ({raw_cols})")

        if rows:
            placeholders = ", ".join("?" for _ in names)
            tuples = [tuple(_to_text(r.get(n)) for n in names) for r in rows]
            con.executemany(f"INSERT INTO _raw VALUES ({placeholders})", tuples)

        typed = ", ".join(
            f'TRY_CAST("{c["name"]}" AS {duckdb_type(c.get("logical_type"))}) AS "{c["name"]}"' for c in columns
        )
        con.execute(f'CREATE TABLE "{STAGING_TABLE}" AS SELECT {typed} FROM _raw')
        con.execute("DROP TABLE _raw")
        count = con.execute(f'SELECT COUNT(*) FROM "{STAGING_TABLE}"').fetchone()
        return int(count[0]) if count else 0
    finally:
        con.close()
