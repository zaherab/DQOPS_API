"""Unit tests for ConnectionService."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from dq_platform.api.errors import NotFoundError
from dq_platform.models.connection import Connection, ConnectionType
from dq_platform.services.connection_service import ConnectionService


class TestConnectionService:
    """Test suite for ConnectionService."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """Create a mock database session."""
        db = AsyncMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create a ConnectionService instance."""
        return ConnectionService(mock_db)

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

    async def test_create_encrypts_config(self, service, mock_db, sample_config):
        """Test that create() encrypts the config."""
        with patch(
            "dq_platform.services.connection_service.encrypt_config"
        ) as mock_encrypt:
            mock_encrypt.return_value = {"encrypted": "data"}

            result = await service.create(
                name="test-connection",
                connection_type=ConnectionType.POSTGRESQL,
                config=sample_config,
            )

        mock_encrypt.assert_called_once_with(sample_config)
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()
        assert isinstance(result, Connection)
        assert result.name == "test-connection"
        assert result.connection_type == ConnectionType.POSTGRESQL

    async def test_create_with_optional_fields(self, service, mock_db, sample_config):
        """Test create() with all optional fields."""
        with patch(
            "dq_platform.services.connection_service.encrypt_config"
        ) as mock_encrypt:
            mock_encrypt.return_value = {"encrypted": "data"}

            result = await service.create(
                name="test-connection",
                connection_type=ConnectionType.MYSQL,
                config=sample_config,
                description="Test description",
                metadata_={"env": "production", "team": "data"},
            )

        assert result.description == "Test description"
        assert result.metadata_ == {"env": "production", "team": "data"}

    async def test_get_success(self, service, mock_db):
        """Test get() returns connection when found."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.id = connection_id
        mock_connection.is_active = True

        # Mock the execute chain
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_connection
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get(connection_id)

        assert result == mock_connection
        mock_db.execute.assert_called_once()

    async def test_get_not_found(self, service, mock_db):
        """Test get() raises NotFoundError when connection doesn't exist."""
        connection_id = uuid4()

        # Mock the execute chain
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.get(connection_id)

        assert str(connection_id) in str(exc_info.value)
        assert "Connection" in str(exc_info.value)

    async def test_get_connection_returns_none(self, service, mock_db):
        """Test get_connection() returns None instead of raising."""
        connection_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_connection(connection_id)

        assert result is None

    async def test_list_connections_pagination(self, service, mock_db):
        """Test list_connections() with pagination."""
        mock_connections = [MagicMock(spec=Connection) for _ in range(5)]

        # Mock count query
        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(i,) for i in range(10)]

        # Mock data query
        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_connections

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        connections, total = await service.list_connections(offset=0, limit=5)

        assert connections == mock_connections
        assert total == 10

    async def test_list_connections_filter_by_type(self, service, mock_db):
        """Test list_connections() filtering by connection type."""
        mock_connections = [MagicMock(spec=Connection)]

        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(1,)]

        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_connections

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        connections, total = await service.list_connections(
            connection_type=ConnectionType.POSTGRESQL
        )

        assert total == 1
        assert len(connections) == 1
        # Verify the query was called with filter
        calls = mock_db.execute.call_args_list
        assert len(calls) == 2

    async def test_update_partial(self, service, mock_db):
        """Test update() with partial fields."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.name = "old-name"
        mock_connection.description = "old-description"

        # Mock get() to return the connection
        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.encrypt_config"
            ) as mock_encrypt:
                mock_encrypt.return_value = {"encrypted": "new-config"}

                result = await service.update(
                    connection_id=connection_id,
                    name="new-name",
                    # description not provided - should not change
                    config={"host": "new-host"},
                )

        assert result.name == "new-name"
        assert result.description == "old-description"  # Unchanged
        mock_db.flush.assert_called_once()

    async def test_update_reencrypts_config(self, service, mock_db):
        """Test update() re-encrypts config when provided."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.config_encrypted = {"old": "config"}

        new_config = {"host": "new-host", "password": "new-pass"}

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.encrypt_config"
            ) as mock_encrypt:
                mock_encrypt.return_value = {"encrypted": "new-data"}

                await service.update(
                    connection_id=connection_id,
                    config=new_config,
                )

        mock_encrypt.assert_called_once_with(new_config)
        assert mock_connection.config_encrypted == {"encrypted": "new-data"}

    async def test_delete_soft_delete(self, service, mock_db):
        """Test delete() performs soft delete."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.is_active = True

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            await service.delete(connection_id)

        assert mock_connection.is_active is False
        mock_db.flush.assert_called_once()

    async def test_test_connection_success(self, service, mock_db):
        """Test test_connection() with successful connection."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.connection_type = ConnectionType.POSTGRESQL
        mock_connection.config_encrypted = {"encrypted": "config"}

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.decrypt_config"
            ) as mock_decrypt:
                mock_decrypt.return_value = {"host": "localhost", "port": 5432}

                with patch(
                    "dq_platform.services.connection_service.get_connector"
                ) as mock_get_connector:
                    mock_connector = MagicMock()
                    mock_connector.test_connection.return_value = True
                    mock_get_connector.return_value = mock_connector

                    result = await service.test_connection(connection_id)

        assert result is True
        mock_connector.test_connection.assert_called_once()

    async def test_get_schemas(self, service, mock_db):
        """Test get_schemas() returns schema list."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.connection_type = ConnectionType.POSTGRESQL
        mock_connection.config_encrypted = {"encrypted": "config"}

        expected_schemas = ["public", "schema1", "schema2"]

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.decrypt_config"
            ) as mock_decrypt:
                mock_decrypt.return_value = {"host": "localhost"}

                with patch(
                    "dq_platform.services.connection_service.get_connector"
                ) as mock_get_connector:
                    mock_connector = MagicMock()
                    mock_connector.get_schemas.return_value = expected_schemas
                    mock_get_connector.return_value = mock_connector

                    result = await service.get_schemas(connection_id)

        assert result == expected_schemas

    async def test_get_tables(self, service, mock_db):
        """Test get_tables() returns table list."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.connection_type = ConnectionType.POSTGRESQL
        mock_connection.config_encrypted = {"encrypted": "config"}

        from dq_platform.connectors.base import TableInfo

        expected_tables = [
            TableInfo(schema_name="public", table_name="users", table_type="BASE TABLE"),
            TableInfo(schema_name="public", table_name="orders", table_type="BASE TABLE"),
        ]

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.decrypt_config"
            ) as mock_decrypt:
                mock_decrypt.return_value = {"host": "localhost"}

                with patch(
                    "dq_platform.services.connection_service.get_connector"
                ) as mock_get_connector:
                    mock_connector = MagicMock()
                    mock_connector.get_tables.return_value = expected_tables
                    mock_get_connector.return_value = mock_connector

                    result = await service.get_tables(connection_id, "public")

        assert result == expected_tables
        mock_connector.get_tables.assert_called_once_with("public")

    async def test_get_columns(self, service, mock_db):
        """Test get_columns() returns column list."""
        connection_id = uuid4()
        mock_connection = MagicMock(spec=Connection)
        mock_connection.connection_type = ConnectionType.POSTGRESQL
        mock_connection.config_encrypted = {"encrypted": "config"}

        from dq_platform.connectors.base import ColumnInfo

        expected_columns = [
            ColumnInfo(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            ColumnInfo(name="email", data_type="varchar", is_nullable=False),
        ]

        with patch.object(service, "get", AsyncMock(return_value=mock_connection)):
            with patch(
                "dq_platform.services.connection_service.decrypt_config"
            ) as mock_decrypt:
                mock_decrypt.return_value = {"host": "localhost"}

                with patch(
                    "dq_platform.services.connection_service.get_connector"
                ) as mock_get_connector:
                    mock_connector = MagicMock()
                    mock_connector.get_columns.return_value = expected_columns
                    mock_get_connector.return_value = mock_connector

                    result = await service.get_columns(connection_id, "public", "users")

        assert result == expected_columns
        mock_connector.get_columns.assert_called_once_with("public", "users")
