"""Pytest configuration and fixtures for integration tests."""

from __future__ import annotations

import os
import subprocess
import uuid
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

# Configuration
API_BASE_URL = os.getenv("DQ_API_URL", "http://localhost:8000/api/v1")
API_KEY = os.getenv("DQ_API_KEY", "test-api-key")
PG_HOST = os.getenv("DQ_PG_HOST", "localhost")
PG_PORT = os.getenv("DQ_PG_PORT", "5433")
PG_USER = os.getenv("DQ_PG_USER", "postgres")
PG_PASSWORD = os.getenv("DQ_PG_PASSWORD", "postgres")
PG_DATABASE = os.getenv("DQ_PG_DATABASE", "dq_platform")


@pytest.fixture(scope="session", autouse=True)
def setup_test_data():
    """Setup test data once per session."""
    sql_file = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures",
        "setup_test_data.sql",
    )
    if not os.path.exists(sql_file):
        pytest.skip(f"Test data SQL file not found: {sql_file}")

    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PASSWORD
    result = subprocess.run(
        ["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", PG_DATABASE, "-f", sql_file],
        capture_output=True,
        env=env,
    )
    if result.returncode != 0:
        pytest.skip(f"Could not setup test data: {result.stderr.decode()}")


@pytest_asyncio.fixture
async def api_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Create async HTTP client."""
    async with httpx.AsyncClient(
        base_url=API_BASE_URL,
        headers={"Content-Type": "application/json", "X-API-Key": API_KEY},
        timeout=30.0,
    ) as client:
        yield client


@pytest_asyncio.fixture
async def connection_id(api_client: httpx.AsyncClient) -> AsyncGenerator[str, None]:
    """Create and cleanup a test connection."""
    conn_data = {
        "name": f"pytest-{uuid.uuid4().hex[:8]}",
        "description": "Integration test connection",
        "connection_type": "postgresql",
        "config": {
            "host": PG_HOST,
            "port": int(PG_PORT),
            "database": PG_DATABASE,
            "user": PG_USER,
            "password": PG_PASSWORD,
        },
    }
    response = await api_client.post("/connections", json=conn_data)
    if response.status_code != 201:
        pytest.fail(f"Failed to create connection: {response.status_code} - {response.text}")

    conn_id = response.json()["id"]
    yield conn_id
    await api_client.delete(f"/connections/{conn_id}")


@pytest_asyncio.fixture
async def check_factory(api_client: httpx.AsyncClient, connection_id: str):
    """Factory for creating checks with cleanup."""
    created_ids: list[str] = []

    async def _create(check_data: dict[str, Any]) -> dict[str, Any]:
        check_data["connection_id"] = connection_id
        response = await api_client.post("/checks", json=check_data)
        if response.status_code != 201:
            pytest.fail(f"Failed to create check: {response.status_code} - {response.text}")
        check = response.json()
        created_ids.append(check["id"])
        return check

    yield _create

    for check_id in created_ids:
        try:
            await api_client.delete(f"/checks/{check_id}")
        except Exception:
            pass


async def run_check_and_wait(client: httpx.AsyncClient, check_id: str, timeout: int = 30) -> dict[str, Any]:
    """Run a check using preview (synchronous execution).

    Uses the preview endpoint for reliable synchronous execution.
    This is preferred for tests as it doesn't depend on Celery infrastructure.
    """
    response = await client.post(f"/checks/{check_id}/preview")
    if response.status_code != 200:
        return {"passed": False, "error_message": f"Preview failed: {response.status_code} - {response.text}"}

    result = response.json()
    return {
        "passed": result.get("passed", False),
        "severity": result.get("severity"),
        "sensor_value": result.get("sensor_value"),
        "expected": result.get("expected"),
        "actual": result.get("actual"),
        "message": result.get("message"),
        "executed_sql": result.get("executed_sql"),
    }
