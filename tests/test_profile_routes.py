"""Tests for the profile route's schema-drift tolerance.

A contentSchema can declare a column that no longer exists in the physical
table (renamed/dropped). The profiler must profile the columns that exist
and report the rest as `missing_columns` — never let one absent column fail
the whole table's profile + sample run.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from dq_platform.api.v1.profile_routes import ProfileRequest, _profile_and_infer


class _FakeColumn:
    def __init__(self, name: str, is_primary_key: bool = False) -> None:
        self.name = name
        self.is_primary_key = is_primary_key


class _FakeConnector:
    """Connector exposing a fixed live column set.

    build_profile_sql/execute_sql are stubbed: execute_sql ignores the SQL
    and returns aggregate rows shaped by the number of columns the runner
    actually asked for, so the test asserts on which columns got profiled.
    build_sample_sql records the pk it was handed, so tests can assert which
    key the sampler resolved.
    """

    def __init__(
        self,
        live: list[str],
        *,
        introspection_fails: bool = False,
        pk_columns: tuple[str, ...] = (),
    ) -> None:
        self._live = live
        self._introspection_fails = introspection_fails
        self._pk_columns = set(pk_columns)
        self._n_cols = 0
        self.sample_pk: str | None = None

    def get_columns(self, schema: str, table: str) -> list[_FakeColumn]:
        if self._introspection_fails:
            raise RuntimeError("permission denied on information_schema")
        return [_FakeColumn(n, n in self._pk_columns) for n in self._live]

    def build_profile_sql(self, schema: str, table: str, cols: list[dict[str, Any]]) -> str:
        self._n_cols = len(cols)
        return "SELECT 1"

    def build_sample_sql(
        self,
        schema: str,
        table: str,
        columns: list[str],
        pk: str,
        n: int,
        seed: int = 1,
        modulus: int = 100_000,
    ) -> str:
        self.sample_pk = pk
        return "SELECT 1"

    async def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        row: dict[str, Any] = {"r_a0": 10}
        for idx in range(self._n_cols):
            row[f"c{idx}_a1"] = 10  # count
            row[f"c{idx}_a2"] = 4  # distinct
            row[f"c{idx}_a3"] = None  # min
            row[f"c{idx}_a4"] = None  # max
            row[f"c{idx}_a5"] = None  # min_len
            row[f"c{idx}_a6"] = None  # max_len
        return [row]

    def get_schema_fingerprint(self, schema: str, table: str) -> str:
        return "fp"


def _request(field_names: list[str], pk: str | None = None) -> ProfileRequest:
    return ProfileRequest(
        org_id="org-1",
        connection_id=uuid4(),
        schema="public",
        table="t",
        pk=pk,
        fields=[{"name": n} for n in field_names],  # type: ignore[list-item]
    )


@pytest.mark.asyncio
async def test_profiles_present_columns_reports_missing() -> None:
    # contentSchema declares 3 fields; only 2 exist in the table.
    connector = _FakeConnector(live=["id", "email"])
    resp = await _profile_and_infer(connector, _request(["id", "email", "ghost_col"]))

    assert resp.missing_columns == ["ghost_col"]
    assert set(resp.columns) == {"id", "email"}
    assert "ghost_col" not in resp.columns
    assert resp.row_count == 10


@pytest.mark.asyncio
async def test_all_columns_missing_does_not_crash() -> None:
    # Every declared field is gone — still returns row_count, no columns.
    connector = _FakeConnector(live=["other"])
    resp = await _profile_and_infer(connector, _request(["a", "b"]))

    assert sorted(resp.missing_columns) == ["a", "b"]
    assert resp.columns == {}
    assert resp.row_count == 10


@pytest.mark.asyncio
async def test_no_missing_columns_when_all_present() -> None:
    connector = _FakeConnector(live=["id", "email"])
    resp = await _profile_and_infer(connector, _request(["id", "email"]))

    assert resp.missing_columns == []
    assert set(resp.columns) == {"id", "email"}


@pytest.mark.asyncio
async def test_case_mismatched_name_is_treated_as_missing() -> None:
    # The declared name is reused verbatim as a quoted identifier in the
    # profile SQL, so a case-mismatched name (declared "ID", physical "id")
    # is unusable. Dropping it is safe; keeping it would fail the query.
    connector = _FakeConnector(live=["id", "email"])
    resp = await _profile_and_infer(connector, _request(["ID", "email"]))

    assert resp.missing_columns == ["ID"]
    assert set(resp.columns) == {"email"}


@pytest.mark.asyncio
async def test_falls_back_to_all_fields_when_introspection_fails() -> None:
    # If get_columns() raises (permission/network), profile every declared
    # field — no regression vs the pre-drift-tolerance behaviour.
    connector = _FakeConnector(live=[], introspection_fails=True)
    resp = await _profile_and_infer(connector, _request(["id", "email"]))

    assert resp.missing_columns == []
    assert set(resp.columns) == {"id", "email"}


@pytest.mark.asyncio
async def test_falls_back_to_table_primary_key_for_sampling() -> None:
    # No PK declared in the request. The connector reports `id` as the
    # table's real primary key — the profiler must use it so sampling
    # (and therefore format/codelist inference) still runs for an
    # ODPS-only contentSchema with no DQ extensions.
    connector = _FakeConnector(live=["id", "country"], pk_columns=("id",))
    await _profile_and_infer(connector, _request(["id", "country"]))

    assert connector.sample_pk == "id"


@pytest.mark.asyncio
async def test_declared_pk_wins_over_table_primary_key() -> None:
    # An explicit pk in the request takes precedence over introspection.
    connector = _FakeConnector(live=["id", "country"], pk_columns=("id",))
    await _profile_and_infer(connector, _request(["id", "country"], pk="country"))

    assert connector.sample_pk == "country"


@pytest.mark.asyncio
async def test_no_sampling_when_no_pk_available() -> None:
    # Neither a declared PK nor a table PK → no sample is attempted.
    connector = _FakeConnector(live=["id", "country"])
    await _profile_and_infer(connector, _request(["id", "country"]))

    assert connector.sample_pk is None
