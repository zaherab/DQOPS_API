"""Audit log for customer-DB queries issued by the profiler pipeline.

Logged: query SHA-256, payload size, duration, target connection, kind
        (profile | sample), result row count.

Not logged: raw SQL (could leak business intent), raw row values
        (could leak customer data). Hashes only.

Storage: structured logs via the standard logger. Optional DB persistence
can be wired later if compliance asks for it; the hash + metadata shape
is stable so the schema doesn't need to change.
"""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("dq_platform.profile_audit")


def _sql_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()[:16]


@dataclass
class AuditEntry:
    kind: str  # "profile" | "sample" | "fingerprint"
    org_id: str | None
    connection_id: str | None
    schema_name: str
    table_name: str
    sql_hash: str
    duration_ms: float
    row_count: int
    bytes_estimate: int


def log_query(entry: AuditEntry) -> None:
    """Single structured log line. Downstream log shippers parse JSON."""
    logger.info(
        "profile_query",
        extra={
            "kind": entry.kind,
            "org_id": entry.org_id,
            "connection_id": entry.connection_id,
            "schema": entry.schema_name,
            "table": entry.table_name,
            "sql_hash": entry.sql_hash,
            "duration_ms": round(entry.duration_ms, 2),
            "row_count": entry.row_count,
            "bytes_estimate": entry.bytes_estimate,
        },
    )


@asynccontextmanager
async def audit(
    *,
    kind: str,
    schema: str,
    table: str,
    sql: str,
    org_id: str | None = None,
    connection_id: str | None = None,
) -> Any:
    """Async context manager that records a single customer-DB query.

    Usage:
        async with audit(kind="profile", schema=s, table=t, sql=sql_str,
                         org_id=oid, connection_id=cid) as recorder:
            rows = await connector.execute_sql(sql_str)
            recorder.set_result(rows)
    """

    class _Recorder:
        def __init__(self) -> None:
            self.row_count = 0
            self.bytes_estimate = 0

        def set_result(self, rows: list[dict[str, Any]] | Any) -> None:
            if isinstance(rows, list):
                self.row_count = len(rows)
                # Cheap byte estimate: avg dict repr length × row count.
                if rows:
                    sample = repr(rows[0])
                    self.bytes_estimate = len(sample) * self.row_count
            else:
                self.row_count = 1 if rows is not None else 0

    rec = _Recorder()
    started = time.monotonic()
    try:
        yield rec
    finally:
        log_query(
            AuditEntry(
                kind=kind,
                org_id=org_id,
                connection_id=connection_id,
                schema_name=schema,
                table_name=table,
                sql_hash=_sql_hash(sql),
                duration_ms=(time.monotonic() - started) * 1000,
                row_count=rec.row_count,
                bytes_estimate=rec.bytes_estimate,
            )
        )
