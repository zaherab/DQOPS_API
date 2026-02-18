"""Tests for schedule API endpoints."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dq_platform.models.check import Check, CheckMode, CheckType
from dq_platform.models.connection import Connection, ConnectionType
from dq_platform.models.schedule import Schedule


class TestScheduleAPI:
    """Test suite for schedule endpoints."""

    @pytest.fixture
    async def connection(self, db_session):
        """Create a test connection."""
        from dq_platform.core.encryption import encrypt_config

        conn = Connection(
            name="test-connection",
            connection_type=ConnectionType.POSTGRESQL,
            config_encrypted=encrypt_config({
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
            }),
        )
        db_session.add(conn)
        await db_session.flush()
        return conn

    @pytest.fixture
    async def check(self, db_session, connection):
        """Create a test check."""
        check = Check(
            name="test-check",
            connection_id=connection.id,
            check_type=CheckType.NULLS_PERCENT,
            check_mode=CheckMode.MONITORING,
            target_table="users",
            target_column="email",
        )
        db_session.add(check)
        await db_session.flush()
        return check

    @pytest.fixture
    async def schedule(self, db_session, check):
        """Create a test schedule."""
        from datetime import datetime, UTC, timedelta

        schedule = Schedule(
            name="test-schedule",
            description="Test schedule",
            check_id=check.id,
            cron_expression="0 */6 * * *",
            timezone="UTC",
            next_run_at=datetime.now(UTC) + timedelta(hours=6),
        )
        db_session.add(schedule)
        await db_session.flush()
        return schedule

    def test_create_schedule_success(self, sync_client: TestClient, check):
        """POST /schedules - Create schedule returns 201."""
        response = sync_client.post(
            "/api/v1/schedules",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "daily-check",
                "description": "Run check daily at midnight",
                "check_id": str(check.id),
                "cron_expression": "0 0 * * *",
                "timezone": "UTC",
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "daily-check"
        assert data["cron_expression"] == "0 0 * * *"
        assert data["timezone"] == "UTC"
        assert data["check_id"] == str(check.id)
        assert "next_run_at" in data

    def test_create_schedule_invalid_cron(self, sync_client: TestClient, check):
        """POST /schedules - Invalid cron expression returns 422."""
        response = sync_client.post(
            "/api/v1/schedules",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "invalid-schedule",
                "check_id": str(check.id),
                "cron_expression": "invalid-cron",
                "timezone": "UTC",
            },
        )

        assert response.status_code == 422

    def test_create_schedule_missing_check(self, sync_client: TestClient):
        """POST /schedules - Non-existent check returns 404."""
        fake_check_id = str(uuid4())
        response = sync_client.post(
            "/api/v1/schedules",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "orphan-schedule",
                "check_id": fake_check_id,
                "cron_expression": "0 0 * * *",
            },
        )

        assert response.status_code == 404

    def test_list_schedules(self, sync_client: TestClient, schedule):
        """GET /schedules - List schedules returns 200."""
        response = sync_client.get(
            "/api/v1/schedules",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_list_schedules_filter_by_check(self, sync_client: TestClient, schedule, check):
        """GET /schedules - Filter by check_id works."""
        check_id = str(check.id)
        response = sync_client.get(
            f"/api/v1/schedules?check_id={check_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["check_id"] == check_id for item in data["items"])

    def test_list_schedules_filter_by_active(self, sync_client: TestClient, schedule):
        """GET /schedules - Filter by is_active works."""
        response = sync_client.get(
            "/api/v1/schedules?is_active=true",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["is_active"] is True for item in data["items"])

    def test_get_schedule_success(self, sync_client: TestClient, schedule):
        """GET /schedules/{id} - Get schedule returns 200."""
        schedule_id = str(schedule.id)
        response = sync_client.get(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == schedule_id
        assert data["name"] == "test-schedule"
        assert data["cron_expression"] == "0 */6 * * *"

    def test_get_schedule_not_found(self, sync_client: TestClient):
        """GET /schedules/{id} - Get non-existent schedule returns 404."""
        fake_id = str(uuid4())
        response = sync_client.get(
            f"/api/v1/schedules/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404

    def test_update_schedule_success(self, sync_client: TestClient, schedule):
        """PUT /schedules/{id} - Update schedule returns 200."""
        schedule_id = str(schedule.id)
        response = sync_client.put(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "updated-schedule",
                "description": "Updated description",
                "cron_expression": "0 */12 * * *",
                "timezone": "America/New_York",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated-schedule"
        assert data["description"] == "Updated description"
        assert data["cron_expression"] == "0 */12 * * *"
        assert data["timezone"] == "America/New_York"

    def test_update_schedule_invalid_cron(self, sync_client: TestClient, schedule):
        """PUT /schedules/{id} - Invalid cron expression returns 422."""
        schedule_id = str(schedule.id)
        response = sync_client.put(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "cron_expression": "invalid-cron",
            },
        )

        assert response.status_code == 422

    def test_update_schedule_not_found(self, sync_client: TestClient):
        """PUT /schedules/{id} - Update non-existent schedule returns 404."""
        fake_id = str(uuid4())
        response = sync_client.put(
            f"/api/v1/schedules/{fake_id}",
            headers={"X-API-Key": "test-key"},
            json={"name": "updated-name"},
        )

        assert response.status_code == 404

    def test_update_schedule_deactivate(self, sync_client: TestClient, schedule):
        """PUT /schedules/{id} - Deactivate schedule works."""
        schedule_id = str(schedule.id)
        response = sync_client.put(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "is_active": False,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False

    def test_delete_schedule_success(self, sync_client: TestClient, schedule):
        """DELETE /schedules/{id} - Delete schedule returns 204."""
        schedule_id = str(schedule.id)
        response = sync_client.delete(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 204

        # Verify it's deleted
        get_response = sync_client.get(
            f"/api/v1/schedules/{schedule_id}",
            headers={"X-API-Key": "test-key"},
        )
        assert get_response.status_code == 404

    def test_delete_schedule_not_found(self, sync_client: TestClient):
        """DELETE /schedules/{id} - Delete non-existent schedule returns 404."""
        fake_id = str(uuid4())
        response = sync_client.delete(
            f"/api/v1/schedules/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404
