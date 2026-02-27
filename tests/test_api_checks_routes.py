"""Tests for check API endpoints."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dq_platform.models.check import Check, CheckMode, CheckType
from dq_platform.models.connection import Connection, ConnectionType


class TestCheckAPI:
    """Test suite for check endpoints."""

    @pytest.fixture
    async def connection(self, db_session):
        """Create a test connection."""
        from dq_platform.core.encryption import encrypt_config

        conn = Connection(
            name="test-connection",
            connection_type=ConnectionType.POSTGRESQL,
            config_encrypted=encrypt_config(
                {
                    "host": "localhost",
                    "port": 5432,
                    "database": "testdb",
                    "user": "testuser",
                    "password": "testpass",
                }
            ),
        )
        db_session.add(conn)
        await db_session.flush()
        return conn

    @pytest.fixture
    async def check(self, db_session, connection):
        """Create a test check."""
        check = Check(
            name="test-check",
            description="Test check",
            connection_id=connection.id,
            check_type=CheckType.NULLS_PERCENT,
            check_mode=CheckMode.MONITORING,
            target_schema="public",
            target_table="users",
            target_column="email",
            parameters={"max_percent": 5.0},
        )
        db_session.add(check)
        await db_session.commit()
        return check

    def test_create_check_success(self, sync_client: TestClient, connection):
        """POST /checks - Create check returns 201."""
        response = sync_client.post(
            "/api/v1/checks",
            json={
                "name": "new-check",
                "description": "A new check",
                "connection_id": str(connection.id),
                "check_type": "nulls_percent",
                "check_mode": "monitoring",
                "target_schema": "public",
                "target_table": "users",
                "target_column": "email",
                "parameters": {"max_percent": 5.0},
                "rule_parameters": {
                    "warning": {"max_percent": 2.0},
                    "error": {"max_percent": 5.0},
                },
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "new-check"
        assert data["check_type"] == "nulls_percent"
        assert data["target_table"] == "users"
        assert data["target_column"] == "email"

    def test_create_check_validation_error(self, sync_client: TestClient, connection):
        """POST /checks - Missing required fields returns 422."""
        response = sync_client.post(
            "/api/v1/checks",
            json={
                "name": "",  # Empty name
                "connection_id": str(connection.id),
                "check_type": "nulls_percent",
                "target_table": "users",
            },
        )

        assert response.status_code == 422

    def test_list_checks(self, sync_client: TestClient, check):
        """GET /checks - List checks returns 200."""
        response = sync_client.get("/api/v1/checks")

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_list_checks_with_filters(self, sync_client: TestClient, check, connection):
        """GET /checks - Filter by connection_id works."""
        response = sync_client.get(f"/api/v1/checks?connection_id={connection.id}")

        assert response.status_code == 200
        data = response.json()
        assert all(item["connection_id"] == str(connection.id) for item in data["items"])

    def test_list_checks_filter_by_type(self, sync_client: TestClient, check):
        """GET /checks - Filter by check_type works."""
        response = sync_client.get("/api/v1/checks?check_type=nulls_percent")

        assert response.status_code == 200
        data = response.json()
        assert all(item["check_type"] == "nulls_percent" for item in data["items"])

    def test_get_check_success(self, sync_client: TestClient, check):
        """GET /checks/{id} - Get existing check returns 200."""
        check_id = str(check.id)
        response = sync_client.get(f"/api/v1/checks/{check_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == check_id
        assert data["name"] == "test-check"

    def test_get_check_not_found(self, sync_client: TestClient):
        """GET /checks/{id} - Get non-existent check returns 404."""
        fake_id = str(uuid4())
        response = sync_client.get(f"/api/v1/checks/{fake_id}")

        assert response.status_code == 404
        data = response.json()
        assert "error" in data

    def test_update_check_success(self, sync_client: TestClient, check):
        """PATCH /checks/{id} - Update check returns 200."""
        check_id = str(check.id)
        response = sync_client.patch(
            f"/api/v1/checks/{check_id}",
            json={
                "name": "updated-check",
                "description": "Updated description",
                "parameters": {"max_percent": 10.0},
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated-check"
        assert data["description"] == "Updated description"

    def test_update_check_not_found(self, sync_client: TestClient):
        """PATCH /checks/{id} - Update non-existent check returns 404."""
        fake_id = str(uuid4())
        response = sync_client.patch(
            f"/api/v1/checks/{fake_id}",
            json={"name": "updated-name"},
        )

        assert response.status_code == 404

    def test_delete_check_success(self, sync_client: TestClient, check):
        """DELETE /checks/{id} - Delete check returns 204."""
        check_id = str(check.id)
        response = sync_client.delete(f"/api/v1/checks/{check_id}")

        assert response.status_code == 204

        # Verify it's soft deleted
        get_response = sync_client.get(f"/api/v1/checks/{check_id}")
        assert get_response.status_code == 404

    def test_delete_check_not_found(self, sync_client: TestClient):
        """DELETE /checks/{id} - Delete non-existent check returns 404."""
        fake_id = str(uuid4())
        response = sync_client.delete(f"/api/v1/checks/{fake_id}")

        assert response.status_code == 404

    def test_run_check(self, sync_client: TestClient, check):
        """POST /checks/{id}/run - Run check returns 202 with job info."""
        check_id = str(check.id)

        with patch("dq_platform.services.execution_service.execute_check") as mock_execute:
            mock_task = MagicMock()
            mock_task.id = "celery-task-id-123"
            mock_execute.delay.return_value = mock_task

            response = sync_client.post(f"/api/v1/checks/{check_id}/run")

        assert response.status_code == 202
        data = response.json()
        assert "job_id" in data
        assert "task_id" in data
        assert data["status"] == "pending"
        assert "message" in data

    def test_preview_check(self, sync_client: TestClient, check):
        """POST /checks/{id}/preview - Preview check returns 200."""
        check_id = str(check.id)

        with patch("dq_platform.services.check_service.CheckService.preview_check") as mock_preview:
            from datetime import UTC, datetime

            from dq_platform.checks.rules import Severity
            from dq_platform.services.check_service import PreviewResult

            mock_preview.return_value = PreviewResult(
                check_type="nulls_percent",
                check_name="test-check",
                severity=Severity.PASSED,
                passed=True,
                sensor_value=0.0,
                expected={"max_percent": 5.0},
                actual=0.0,
                message="Check passed",
                executed_sql="SELECT ...",
                executed_at=datetime.now(UTC),
            )

            response = sync_client.post(f"/api/v1/checks/{check_id}/preview")

        assert response.status_code == 200
        data = response.json()
        assert data["check_id"] == check_id
        assert "severity" in data
        assert "passed" in data

    def test_validate_check_preview(self, sync_client: TestClient, connection):
        """POST /checks/validate/preview - Preview unsaved config returns 200."""
        with patch("dq_platform.services.check_service.CheckService.preview_check_config") as mock_preview:
            from datetime import UTC, datetime

            from dq_platform.checks.rules import Severity
            from dq_platform.services.check_service import PreviewResult

            mock_preview.return_value = PreviewResult(
                check_type="nulls_percent",
                check_name="preview",
                severity=Severity.PASSED,
                passed=True,
                sensor_value=0.0,
                expected={"max_percent": 5.0},
                actual=0.0,
                message="Check passed",
                executed_sql="SELECT ...",
                executed_at=datetime.now(UTC),
            )

            response = sync_client.post(
                "/api/v1/checks/validate/preview",
                json={
                    "connection_id": str(connection.id),
                    "check_type": "nulls_percent",
                    "target_schema": "public",
                    "target_table": "users",
                    "target_column": "email",
                    "parameters": {"max_percent": 5.0},
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert "severity" in data
        assert "passed" in data

    def test_list_check_types(self, sync_client: TestClient):
        """GET /checks/types - List check types returns 200."""
        response = sync_client.get("/api/v1/checks/types")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        # Check structure
        assert "type" in data[0]
        assert "description" in data[0]
        assert "is_column_level" in data[0]

    def test_list_check_types_with_category_filter(self, sync_client: TestClient):
        """GET /checks/types?category=volume - Filter by category works."""
        response = sync_client.get("/api/v1/checks/types?category=volume")

        assert response.status_code == 200
        data = response.json()
        # All returned checks should be in the volume category
        assert all(item.get("category") == "volume" for item in data)

    def test_list_check_categories(self, sync_client: TestClient):
        """GET /checks/categories - List categories returns 200."""
        response = sync_client.get("/api/v1/checks/categories")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Should have some categories
        assert len(data) > 0

    def test_list_check_modes(self, sync_client: TestClient):
        """GET /checks/modes - List check modes returns 200."""
        response = sync_client.get("/api/v1/checks/modes")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "profiling" in data
        assert "monitoring" in data
        assert "partitioned" in data

    def test_list_time_scales(self, sync_client: TestClient):
        """GET /checks/time-scales - List time scales returns 200."""
        response = sync_client.get("/api/v1/checks/time-scales")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "daily" in data
        assert "monthly" in data
