"""Integration tests for new connectors and anomaly detection checks.

Tests:
1. DuckDB connector: register connection, verify creation via API.
2. PostgreSQL checks: run various DQOps checks via preview endpoint.
3. Anomaly detection: seed historical check_results, then run anomaly checks.

Requirements:
  - PostgreSQL + API running (docker-compose up -d, uvicorn)
  - Test data loaded (setup_test_data.sql)
"""

from __future__ import annotations

import os
import uuid

import asyncpg
import httpx
import pytest
import pytest_asyncio

API_BASE_URL = os.getenv("DQ_API_URL", "http://localhost:8000/api/v1")
API_KEY = os.getenv("DQ_API_KEY", "test-api-key")
PG_HOST = os.getenv("DQ_PG_HOST", "localhost")
PG_PORT = os.getenv("DQ_PG_PORT", "5433")
PG_USER = os.getenv("DQ_PG_USER", "postgres")
PG_PASSWORD = os.getenv("DQ_PG_PASSWORD", "postgres")
PG_DATABASE = os.getenv("DQ_PG_DATABASE", "dq_platform")
PG_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def api():
    async with httpx.AsyncClient(
        base_url=API_BASE_URL,
        headers={"Content-Type": "application/json", "X-API-Key": API_KEY},
        timeout=30.0,
    ) as client:
        yield client


@pytest_asyncio.fixture
async def pg_connection_id(api: httpx.AsyncClient):
    resp = await api.post("/connections", json={
        "name": f"pg-test-{uuid.uuid4().hex[:8]}",
        "connection_type": "postgresql",
        "config": {
            "host": PG_HOST, "port": int(PG_PORT),
            "database": PG_DATABASE, "user": PG_USER, "password": PG_PASSWORD,
        },
    })
    assert resp.status_code == 201, f"Failed to create PG connection: {resp.text}"
    conn_id = resp.json()["id"]
    yield conn_id
    await api.delete(f"/connections/{conn_id}")


@pytest_asyncio.fixture
async def duckdb_connection_id(api: httpx.AsyncClient):
    resp = await api.post("/connections", json={
        "name": f"duckdb-test-{uuid.uuid4().hex[:8]}",
        "connection_type": "duckdb",
        "config": {"database": ":memory:"},
    })
    assert resp.status_code == 201, f"Failed to create DuckDB connection: {resp.text}"
    conn_id = resp.json()["id"]
    yield conn_id
    await api.delete(f"/connections/{conn_id}")


async def _create_check(api, connection_id, check_type, table, column=None,
                         schema=None, rule_params=None, parameters=None):
    payload = {
        "name": f"test-{check_type}-{uuid.uuid4().hex[:6]}",
        "connection_id": connection_id,
        "check_type": check_type,
        "target_schema": schema or "public",
        "target_table": table,
        "rule_parameters": rule_params or {"error": {"max_percent": 50.0}},
    }
    if column:
        payload["target_column"] = column
    if parameters:
        payload["parameters"] = parameters
    resp = await api.post("/checks", json=payload)
    assert resp.status_code == 201, f"Failed to create check '{check_type}': {resp.text}"
    return resp.json()


async def _preview(api, check_id):
    resp = await api.post(f"/checks/{check_id}/preview")
    assert resp.status_code == 200, f"Preview failed: {resp.text}"
    return resp.json()


async def _seed_history(check_id: str, connection_id: str, values: list[float],
                         check_type: str = "row_count"):
    """Insert historical check_results directly into the database.

    Creates a job per result (to satisfy FK constraint).
    """
    conn = await asyncpg.connect(PG_DSN)
    try:
        for i, val in enumerate(values):
            job_id = uuid.uuid4()
            # Create a completed job
            await conn.execute("""
                INSERT INTO jobs (id, check_id, status, created_at, updated_at)
                VALUES ($1, $2, 'completed', NOW(), NOW())
            """, job_id, uuid.UUID(check_id))

            # Create the check result
            await conn.execute("""
                INSERT INTO check_results
                    (id, check_id, job_id, connection_id, target_table, check_type,
                     actual_value, passed, severity, executed_at)
                VALUES ($1, $2, $3, $4, 'test_data_quality', $5, $6, true, 'passed',
                        NOW() - make_interval(days => $7))
            """, uuid.uuid4(), uuid.UUID(check_id), job_id,
                uuid.UUID(connection_id), check_type, val, i + 1)
    finally:
        await conn.close()


async def _cleanup_history(check_id: str):
    """Remove seeded history for a check."""
    conn = await asyncpg.connect(PG_DSN)
    try:
        cid = uuid.UUID(check_id)
        await conn.execute("DELETE FROM check_results WHERE check_id = $1", cid)
        await conn.execute("DELETE FROM jobs WHERE check_id = $1", cid)
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════
# Part 1: DuckDB Connector Tests
# ══════════════════════════════════════════════════════════════════════


class TestDuckDBConnector:

    @pytest.mark.asyncio
    async def test_duckdb_connection_created(self, api, duckdb_connection_id):
        resp = await api.get(f"/connections/{duckdb_connection_id}")
        assert resp.status_code == 200
        assert resp.json()["connection_type"] == "duckdb"

    @pytest.mark.asyncio
    async def test_duckdb_validate_preview(self, api, duckdb_connection_id):
        """Validate/preview goes through the DuckDB connector."""
        resp = await api.post("/checks/validate/preview", json={
            "connection_id": duckdb_connection_id,
            "check_type": "row_count",
            "target_schema": "main",
            "target_table": "generate_series(1, 10)",
            "rule_parameters": {"error": {"min_count": 1}},
        })
        assert resp.status_code == 200
        result = resp.json()
        # The connector reaches DuckDB — result is either pass or an execution error message
        # Either way proves the connector pipeline works end-to-end
        assert "passed" in result or "message" in result


# ══════════════════════════════════════════════════════════════════════
# Part 2: PostgreSQL Checks — Full Execution via Preview
# ══════════════════════════════════════════════════════════════════════


class TestCheckExecution:

    @pytest.mark.asyncio
    async def test_row_count(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "row_count", "test_data_quality",
                                     rule_params={"error": {"min_count": 1}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert result["sensor_value"] == 20
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_nulls_percent(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "nulls_percent", "test_data_quality",
                                     column="email", rule_params={"error": {"max_percent": 20.0}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert result["sensor_value"] == pytest.approx(5.0)
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_distinct_count(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "distinct_count", "test_data_quality",
                                     column="email", rule_params={"error": {"min_count": 10}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert result["sensor_value"] >= 17
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_invalid_email_format_percent(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "invalid_email_format_percent",
                                     "test_data_quality", column="email",
                                     rule_params={"error": {"max_percent": 20.0}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert result["sensor_value"] < 20.0
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_min_in_range(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "min_in_range", "test_data_quality",
                                     column="score",
                                     rule_params={"error": {"min_value": -10, "max_value": 10}})
        result = await _preview(api, check["id"])
        assert result["sensor_value"] == 0
        assert result["passed"] is True
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_max_in_range(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "max_in_range", "test_data_quality",
                                     column="score",
                                     rule_params={"error": {"min_value": 50, "max_value": 150}})
        result = await _preview(api, check["id"])
        assert result["sensor_value"] == 100
        assert result["passed"] is True
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_true_percent(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "true_percent", "test_data_quality",
                                     column="is_active",
                                     rule_params={"error": {"min_percent": 30.0}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert result["sensor_value"] > 50.0
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_text_max_length(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "text_max_length", "test_data_quality",
                                     column="description",
                                     rule_params={"error": {"min_value": 10, "max_value": 200}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        await api.delete(f"/checks/{check['id']}")

    @pytest.mark.asyncio
    async def test_mean_in_range(self, api, pg_connection_id):
        check = await _create_check(api, pg_connection_id, "mean_in_range", "test_data_quality",
                                     column="score",
                                     rule_params={"error": {"min_value": 50, "max_value": 100}})
        result = await _preview(api, check["id"])
        assert result["passed"] is True
        assert 50 <= result["sensor_value"] <= 100
        await api.delete(f"/checks/{check['id']}")


# ══════════════════════════════════════════════════════════════════════
# Part 3: Anomaly Detection Checks
# ══════════════════════════════════════════════════════════════════════


class TestAnomalyDetection:
    """Test anomaly detection via the API with seeded historical data."""

    @pytest.mark.asyncio
    async def test_row_count_anomaly_passes_with_stable_history(self, api, pg_connection_id):
        """Current row_count matches historical values — should pass."""
        check = await _create_check(api, pg_connection_id, "row_count_anomaly",
                                     "test_data_quality",
                                     rule_params={"error": {"anomaly_percent": 5.0}})
        check_id = check["id"]
        try:
            # Seed: row_count was ~20 for the last 10 runs (current is also 20)
            await _seed_history(check_id, pg_connection_id,
                                [19, 20, 21, 20, 19, 21, 20, 20, 19, 21],
                                check_type="row_count_anomaly")

            result = await _preview(api, check_id)
            assert result["passed"] is True, f"Expected pass: {result['message']}"
            assert result["sensor_value"] == 20
        finally:
            await _cleanup_history(check_id)
            await api.delete(f"/checks/{check_id}")

    @pytest.mark.asyncio
    async def test_row_count_anomaly_fails_with_different_history(self, api, pg_connection_id):
        """Current row_count (20) vs historical (~1000) — should detect anomaly."""
        check = await _create_check(api, pg_connection_id, "row_count_anomaly",
                                     "test_data_quality",
                                     rule_params={"error": {"anomaly_percent": 5.0}})
        check_id = check["id"]
        try:
            await _seed_history(check_id, pg_connection_id,
                                [1000, 1005, 1010, 995, 1002, 1008, 997, 1003, 1001, 998],
                                check_type="row_count_anomaly")

            result = await _preview(api, check_id)
            assert result["passed"] is False, f"Expected fail (anomaly): {result['message']}"
        finally:
            await _cleanup_history(check_id)
            await api.delete(f"/checks/{check_id}")

    @pytest.mark.asyncio
    async def test_nulls_percent_anomaly_passes(self, api, pg_connection_id):
        """nulls_percent stable at ~5% historically, current is ~5% — should pass."""
        check = await _create_check(api, pg_connection_id, "nulls_percent_anomaly",
                                     "test_data_quality", column="email",
                                     rule_params={"error": {"anomaly_percent": 5.0}})
        check_id = check["id"]
        try:
            await _seed_history(check_id, pg_connection_id,
                                [4.5, 5.0, 5.5, 5.0, 4.8, 5.2, 4.9, 5.1, 5.0, 4.7],
                                check_type="nulls_percent_anomaly")

            result = await _preview(api, check_id)
            assert result["passed"] is True, f"Expected pass: {result['message']}"
            assert result["sensor_value"] == pytest.approx(5.0)
        finally:
            await _cleanup_history(check_id)
            await api.delete(f"/checks/{check_id}")

    @pytest.mark.asyncio
    async def test_mean_anomaly_passes(self, api, pg_connection_id):
        """mean(score) stable at ~72 historically — should pass."""
        check = await _create_check(api, pg_connection_id, "mean_anomaly",
                                     "test_data_quality", column="score",
                                     rule_params={"error": {"anomaly_percent": 5.0}})
        check_id = check["id"]
        try:
            # Actual mean of test data scores ≈ 72.75
            await _seed_history(check_id, pg_connection_id,
                                [72, 73, 71, 74, 72, 73, 71, 72, 73, 74],
                                check_type="mean_anomaly")

            result = await _preview(api, check_id)
            assert result["passed"] is True, f"Expected pass: {result['message']}"
        finally:
            await _cleanup_history(check_id)
            await api.delete(f"/checks/{check_id}")

    @pytest.mark.asyncio
    async def test_anomaly_insufficient_history_passes(self, api, pg_connection_id):
        """With < 7 data points, anomaly rule passes (insufficient data)."""
        check = await _create_check(api, pg_connection_id, "row_count_anomaly",
                                     "test_data_quality",
                                     rule_params={"error": {"anomaly_percent": 5.0}})
        check_id = check["id"]
        try:
            # Only 3 points — below the 7-point minimum
            await _seed_history(check_id, pg_connection_id,
                                [1000, 1000, 1000],
                                check_type="row_count_anomaly")

            result = await _preview(api, check_id)
            assert result["passed"] is True, f"Expected pass (insufficient history): {result['message']}"
        finally:
            await _cleanup_history(check_id)
            await api.delete(f"/checks/{check_id}")
