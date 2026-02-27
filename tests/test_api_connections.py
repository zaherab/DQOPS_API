"""Tests for connection API endpoints."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dq_platform.models.connection import Connection, ConnectionType


class TestConnectionAPI:
    """Test suite for connection endpoints."""

    @pytest.fixture
    def sample_config(self):
        """Sample connection configuration."""
        return {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        }

    @pytest.fixture
    async def connection(self, db_session, sample_config):
        """Create a test connection."""
        from dq_platform.core.encryption import encrypt_config

        conn = Connection(
            name="test-connection",
            description="Test connection",
            connection_type=ConnectionType.POSTGRESQL,
            config_encrypted=encrypt_config(sample_config),
            metadata_={"env": "test"},
        )
        db_session.add(conn)
        await db_session.flush()
        return conn

    def test_create_connection_success(self, sync_client: TestClient, sample_config):
        """POST /connections - Create connection returns 201."""
        response = sync_client.post(
            "/api/v1/connections",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "new-connection",
                "description": "A new connection",
                "connection_type": "postgresql",
                "config": sample_config,
                "metadata": {"env": "production"},
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "new-connection"
        assert data["connection_type"] == "postgresql"
        assert data["description"] == "A new connection"
        assert data["is_active"] is True
        assert "id" in data
        assert "created_at" in data

    def test_create_connection_validation_error(self, sync_client: TestClient):
        """POST /connections - Validation error returns 422."""
        response = sync_client.post(
            "/api/v1/connections",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "",  # Empty name should fail validation
                "connection_type": "postgresql",
                "config": {},
            },
        )

        assert response.status_code == 422

    def test_create_connection_missing_required_fields(self, sync_client: TestClient):
        """POST /connections - Missing required fields returns 422."""
        response = sync_client.post(
            "/api/v1/connections",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "test-connection",
                # Missing connection_type and config
            },
        )

        assert response.status_code == 422

    def test_list_connections(self, sync_client: TestClient, connection):
        """GET /connections - List connections returns 200."""
        response = sync_client.get(
            "/api/v1/connections",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert "offset" in data
        assert "limit" in data
        assert data["total"] >= 1
        assert len(data["items"]) >= 1

    def test_list_connections_pagination(self, sync_client: TestClient, connection):
        """GET /connections - Pagination works correctly."""
        response = sync_client.get(
            "/api/v1/connections?offset=0&limit=1",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["offset"] == 0
        assert data["limit"] == 1

    def test_list_connections_filter_by_type(self, sync_client: TestClient, connection):
        """GET /connections - Filter by connection type works."""
        response = sync_client.get(
            "/api/v1/connections?connection_type=postgresql",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["connection_type"] == "postgresql" for item in data["items"])

    def test_get_connection_success(self, sync_client: TestClient, connection):
        """GET /connections/{id} - Get existing connection returns 200."""

        conn_id = str(connection.id)
        response = sync_client.get(
            f"/api/v1/connections/{conn_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == conn_id
        assert data["name"] == "test-connection"
        assert data["connection_type"] == "postgresql"

    def test_get_connection_not_found(self, sync_client: TestClient):
        """GET /connections/{id} - Get non-existent connection returns 404."""
        fake_id = str(uuid4())
        response = sync_client.get(
            f"/api/v1/connections/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404
        data = response.json()
        assert "error" in data
        assert "not found" in data["error"]["message"].lower()

    def test_update_connection_success(self, sync_client: TestClient, connection):
        """PUT /connections/{id} - Update connection returns 200."""
        conn_id = str(connection.id)
        response = sync_client.put(
            f"/api/v1/connections/{conn_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "updated-connection",
                "description": "Updated description",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated-connection"
        assert data["description"] == "Updated description"

    def test_update_connection_not_found(self, sync_client: TestClient):
        """PUT /connections/{id} - Update non-existent connection returns 404."""
        fake_id = str(uuid4())
        response = sync_client.put(
            f"/api/v1/connections/{fake_id}",
            headers={"X-API-Key": "test-key"},
            json={"name": "updated-name"},
        )

        assert response.status_code == 404

    def test_update_connection_validation_error(self, sync_client: TestClient, connection):
        """PUT /connections/{id} - Invalid data returns 422."""
        conn_id = str(connection.id)
        response = sync_client.put(
            f"/api/v1/connections/{conn_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "",  # Empty name should fail validation
            },
        )

        assert response.status_code == 422

    def test_delete_connection_success(self, sync_client: TestClient, connection):
        """DELETE /connections/{id} - Delete connection returns 204."""
        conn_id = str(connection.id)
        response = sync_client.delete(
            f"/api/v1/connections/{conn_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 204

        # Verify it's soft deleted by trying to get it
        get_response = sync_client.get(
            f"/api/v1/connections/{conn_id}",
            headers={"X-API-Key": "test-key"},
        )
        assert get_response.status_code == 404

    def test_delete_connection_not_found(self, sync_client: TestClient):
        """DELETE /connections/{id} - Delete non-existent connection returns 404."""
        fake_id = str(uuid4())
        response = sync_client.delete(
            f"/api/v1/connections/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404

    def test_test_connection_success(self, sync_client: TestClient, connection):
        """POST /connections/{id}/test - Test connection returns 200."""
        conn_id = str(connection.id)

        with patch("dq_platform.services.connection_service.get_connector") as mock_get_connector:
            mock_connector = MagicMock()
            mock_connector.test_connection.return_value = True
            mock_get_connector.return_value = mock_connector

            response = sync_client.post(
                f"/api/v1/connections/{conn_id}/test",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "message" in data

    def test_test_connection_failure(self, sync_client: TestClient, connection):
        """POST /connections/{id}/test - Connection failure returns 200 with success=False."""
        conn_id = str(connection.id)

        with patch("dq_platform.services.connection_service.get_connector") as mock_get_connector:
            mock_connector = MagicMock()
            mock_connector.test_connection.side_effect = Exception("Connection refused")
            mock_get_connector.return_value = mock_connector

            response = sync_client.post(
                f"/api/v1/connections/{conn_id}/test",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "connection refused" in data["message"].lower()

    def test_get_schemas(self, sync_client: TestClient, connection):
        """GET /connections/{id}/schemas - Get schemas returns 200."""
        conn_id = str(connection.id)

        with patch("dq_platform.services.connection_service.get_connector") as mock_get_connector:
            mock_connector = MagicMock()
            mock_connector.get_schemas.return_value = ["public", "schema1", "schema2"]
            mock_get_connector.return_value = mock_connector

            response = sync_client.get(
                f"/api/v1/connections/{conn_id}/schemas",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "public" in data

    def test_get_tables(self, sync_client: TestClient, connection):
        """GET /connections/{id}/schemas/{schema}/tables - Get tables returns 200."""
        conn_id = str(connection.id)

        from dq_platform.connectors.base import TableInfo

        with patch("dq_platform.services.connection_service.get_connector") as mock_get_connector:
            mock_connector = MagicMock()
            mock_connector.get_tables.return_value = [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    table_type="BASE TABLE",
                    row_count=1000,
                ),
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    table_type="BASE TABLE",
                    row_count=5000,
                ),
            ]
            mock_get_connector.return_value = mock_connector

            response = sync_client.get(
                f"/api/v1/connections/{conn_id}/schemas/public/tables",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["table_name"] == "users"
        assert data[0]["schema_name"] == "public"

    def test_get_columns(self, sync_client: TestClient, connection):
        """GET /connections/{id}/schemas/{schema}/tables/{table}/columns - Get columns returns 200."""
        conn_id = str(connection.id)

        from dq_platform.connectors.base import ColumnInfo

        with patch("dq_platform.services.connection_service.get_connector") as mock_get_connector:
            mock_connector = MagicMock()
            mock_connector.get_columns.return_value = [
                ColumnInfo(
                    name="id",
                    data_type="integer",
                    is_nullable=False,
                    is_primary_key=True,
                ),
                ColumnInfo(
                    name="email",
                    data_type="varchar",
                    is_nullable=False,
                    is_primary_key=False,
                ),
            ]
            mock_get_connector.return_value = mock_connector

            response = sync_client.get(
                f"/api/v1/connections/{conn_id}/schemas/public/tables/users/columns",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["name"] == "id"
        assert data[0]["data_type"] == "integer"
