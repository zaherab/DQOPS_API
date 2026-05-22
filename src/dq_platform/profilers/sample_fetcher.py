"""Pull a bounded row sample from a customer DB to the dq-platform server.

The only path that moves raw values across the wire. Aggregates use
profile_runner. Use cases for samples:
    - regex inference (1k rows usually enough at 95% confidence)
    - codelist match (1k)
    - enum detection (5k)
    - length range detection (1k)
    - outlier detection (10k stratified)

A hard ceiling of MAX_SAMPLE_ROWS prevents misconfigured callers from
egress-bombing the engine. PII columns are stripped from projection
entirely — aggregate stats are still available for them via profile_runner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dq_platform.connectors.base import BaseConnector

MAX_SAMPLE_ROWS = 10_000

DEFAULT_MODULUS = 100_000


@dataclass
class SampleSpec:
    """Caller intent for sampling sizing.

    The engine picks modulus to land within `n` rows in expectation.
    n is capped at MAX_SAMPLE_ROWS.
    """

    columns: list[str]
    pk: str
    n: int
    seed: int = 1


async def fetch_sample(
    connector: BaseConnector,
    schema: str,
    table: str,
    spec: SampleSpec,
    pii_columns: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch a deterministic bounded sample.

    Args:
        connector: configured customer connection.
        schema: schema name.
        table: table name.
        spec: SampleSpec (columns, pk, n, seed).
        pii_columns: optional set of column names to omit from projection.

    Returns:
        Up to spec.n rows as a list of dicts. Empty list on driver error.
    """
    n = max(0, min(spec.n, MAX_SAMPLE_ROWS))
    if n == 0 or not spec.columns:
        return []

    pii = pii_columns or set()
    projected = [c for c in spec.columns if c not in pii]
    if not projected:
        return []

    sql = connector.build_sample_sql(
        schema=schema,
        table=table,
        columns=projected,
        pk=spec.pk,
        n=n,
        seed=spec.seed,
        modulus=DEFAULT_MODULUS,
    )
    return await connector.execute_sql(sql)


def rows_to_column_values(
    rows: list[dict[str, Any]],
    column: str,
) -> list[Any]:
    """Extract one column from sample rows. Convenience for inference."""
    return [r.get(column) for r in rows if column in r]
