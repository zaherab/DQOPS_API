"""Run aggregate profile queries against a customer DB.

One round-trip per table. Aggregates only — no rows leave the customer
DB except via sample_fetcher (which is a separate, bounded path).

The profile result has opaque numeric aliases at the SQL level
(`c0_a1`, `c0_a2`, ...); this module re-keys them back to column names
locally on the dq-platform server. Customers see only the opaque SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dq_platform.connectors.base import BaseConnector


@dataclass
class FieldProfileSpec:
    """Per-field input to profile_runner."""

    name: str
    logical_type: str | None = None
    classification: str | None = None  # "PII" / "PCI" / "PHI" / "public"


@dataclass
class ColumnProfile:
    """Per-column aggregate profile."""

    count: int = 0
    nulls: int = 0
    distinct: int = 0
    min: Any = None
    max: Any = None
    min_len: int | None = None
    max_len: int | None = None


@dataclass
class TableProfile:
    """Full table profile result."""

    row_count: int = 0
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    schema_fingerprint: str | None = None


async def run_profile(
    connector: BaseConnector,
    schema: str,
    table: str,
    fields: list[FieldProfileSpec],
) -> TableProfile:
    """Profile a table — one round-trip, opaque aliases.

    Args:
        connector: a BaseConnector (already-configured customer connection).
        schema: schema name.
        table: table name.
        fields: per-field specs. Order matters: result aliases are
            position-prefixed (c0_*, c1_*, ...).

    Returns:
        TableProfile with row_count + per-column aggregates +
        schema_fingerprint.
    """
    # Build the column descriptor list for the SQL builder.
    cols = [
        {
            "name": f.name,
            "logical_type": f.logical_type,
            "classification": f.classification,
        }
        for f in fields
    ]
    sql = connector.build_profile_sql(schema, table, cols)
    rows = await connector.execute_sql(sql)
    if not rows:
        return TableProfile()

    row = rows[0]
    profile = TableProfile(row_count=int(row.get("r_a0", 0) or 0))

    for idx, f in enumerate(fields):
        prefix = f"c{idx}"
        is_pii = f.classification == "PII"
        count = _int(row.get(f"{prefix}_a1"))
        col = ColumnProfile(
            count=count,
            nulls=max(0, profile.row_count - count),
        )
        if not is_pii:
            col.distinct = _int(row.get(f"{prefix}_a2"))
            col.min = row.get(f"{prefix}_a3")
            col.max = row.get(f"{prefix}_a4")
            col.min_len = _int_opt(row.get(f"{prefix}_a5"))
            col.max_len = _int_opt(row.get(f"{prefix}_a6"))
        profile.columns[f.name] = col

    # Best-effort fingerprint. If the connector can't introspect schema
    # (network blocked, permission denied), leave as None — the cache
    # layer treats absence as "always refresh".
    try:
        profile.schema_fingerprint = connector.get_schema_fingerprint(schema, table)
    except Exception:
        profile.schema_fingerprint = None

    return profile


def _int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _int_opt(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
