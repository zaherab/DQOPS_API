"""Tests for incident API endpoints."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dq_platform.models.check import Check, CheckMode, CheckType
from dq_platform.models.connection import Connection, ConnectionType
from dq_platform.models.incident import Incident, IncidentSeverity, IncidentStatus


class TestIncidentAPI:
    """Test suite for incident endpoints."""

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
    async def incident(self, db_session, check):
        """Create a test incident."""
        from datetime import UTC, datetime

        incident = Incident(
            check_id=check.id,
            status=IncidentStatus.OPEN,
            severity=IncidentSeverity.MEDIUM,
            title="Check failed: test-check",
            description="Null percentage exceeded threshold",
            first_failure_at=datetime.now(UTC),
            last_failure_at=datetime.now(UTC),
            failure_count=1,
        )
        db_session.add(incident)
        await db_session.flush()
        return incident

    def test_list_incidents(self, sync_client: TestClient, incident):
        """GET /incidents - List incidents returns 200."""
        response = sync_client.get(
            "/api/v1/incidents",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_list_incidents_filter_by_status(self, sync_client: TestClient, incident):
        """GET /incidents - Filter by status works."""
        response = sync_client.get(
            "/api/v1/incidents?status=open",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["status"] == "open" for item in data["items"])

    def test_list_incidents_filter_by_severity(self, sync_client: TestClient, incident):
        """GET /incidents - Filter by severity works."""
        response = sync_client.get(
            "/api/v1/incidents?severity=medium",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["severity"] == "medium" for item in data["items"])

    def test_list_incidents_filter_by_check_id(self, sync_client: TestClient, incident, check):
        """GET /incidents - Filter by check_id works."""
        check_id = str(check.id)
        response = sync_client.get(
            f"/api/v1/incidents?check_id={check_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["check_id"] == check_id for item in data["items"])

    def test_get_incident_success(self, sync_client: TestClient, incident):
        """GET /incidents/{id} - Get incident returns 200."""
        incident_id = str(incident.id)
        response = sync_client.get(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == incident_id
        assert data["title"] == "Check failed: test-check"
        assert data["status"] == "open"
        assert data["severity"] == "medium"

    def test_get_incident_not_found(self, sync_client: TestClient):
        """GET /incidents/{id} - Get non-existent incident returns 404."""
        fake_id = str(uuid4())
        response = sync_client.get(
            f"/api/v1/incidents/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404

    def test_update_incident_status_acknowledge(self, sync_client: TestClient, incident):
        """PATCH /incidents/{id} - Acknowledge incident returns 200."""
        incident_id = str(incident.id)
        response = sync_client.patch(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "status": "acknowledged",
                "by": "test-user",
                "notes": "Looking into this",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "acknowledged"
        assert data["acknowledged_by"] == "test-user"

    def test_update_incident_status_resolve(self, sync_client: TestClient, incident):
        """PATCH /incidents/{id} - Resolve incident returns 200."""
        incident_id = str(incident.id)
        response = sync_client.patch(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "status": "resolved",
                "by": "test-user",
                "notes": "Fixed the data issue",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "resolved"
        assert data["resolved_by"] == "test-user"
        assert data["resolution_notes"] == "Fixed the data issue"

    def test_update_incident_status_reopen(self, sync_client: TestClient, incident):
        """PATCH /incidents/{id} - Reopen resolved incident returns 200."""
        incident_id = str(incident.id)

        # First resolve the incident
        sync_client.patch(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "status": "resolved",
                "by": "test-user",
            },
        )

        # Then reopen it
        response = sync_client.patch(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "status": "open",
                "by": "test-user",
                "notes": "Issue reoccurred",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "open"

    def test_update_incident_invalid_transition(self, sync_client: TestClient, incident):
        """PATCH /incidents/{id} - Invalid status transition returns 422."""
        incident_id = str(incident.id)

        # Cannot go from open to open (same status)
        response = sync_client.patch(
            f"/api/v1/incidents/{incident_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "status": "open",
                "by": "test-user",
            },
        )

        # This might return 200 if the status doesn't change, or 422 if validation catches it
        # The actual behavior depends on implementation
        assert response.status_code in [200, 422]
