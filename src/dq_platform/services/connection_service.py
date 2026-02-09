"""Connection service - CRUD operations for data source connections."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError
from dq_platform.connectors.base import ColumnInfo, TableInfo
from dq_platform.connectors.factory import get_connector
from dq_platform.core.encryption import decrypt_config, encrypt_config
from dq_platform.models.connection import Connection, ConnectionType


class ConnectionService:
    """Service for managing data source connections."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(
        self,
        name: str,
        connection_type: ConnectionType,
        config: dict[str, Any],
        description: str | None = None,
        metadata_: dict[str, Any] | None = None,
    ) -> Connection:
        """Create a new connection.

        Args:
            name: Connection name.
            connection_type: Type of database connection.
            config: Connection configuration (will be encrypted).
            description: Optional description.
            metadata: Optional metadata.

        Returns:
            Created connection.
        """
        encrypted_config = encrypt_config(config)

        connection = Connection(
            name=name,
            description=description,
            connection_type=connection_type,
            config_encrypted=encrypted_config,
            metadata_=metadata_ or {},
        )

        self.db.add(connection)
        await self.db.flush()
        return connection

    async def get(self, connection_id: uuid.UUID) -> Connection:
        """Get a connection by ID.

        Args:
            connection_id: Connection UUID.

        Returns:
            Connection instance.

        Raises:
            NotFoundError: If connection not found.
        """
        result = await self.db.execute(
            select(Connection).where(
                Connection.id == connection_id,
                Connection.is_active == True,  # noqa: E712
            )
        )
        connection = result.scalar_one_or_none()

        if not connection:
            raise NotFoundError("Connection", str(connection_id))

        return connection

    async def get_connection(self, connection_id: uuid.UUID) -> Connection | None:
        """Get a connection by ID (alias for get that returns None instead of raising).

        Args:
            connection_id: Connection UUID.

        Returns:
            Connection instance or None if not found.
        """
        result = await self.db.execute(
            select(Connection).where(
                Connection.id == connection_id,
                Connection.is_active == True,  # noqa: E712
            )
        )
        return result.scalar_one_or_none()

    async def list_connections(
        self,
        offset: int = 0,
        limit: int = 100,
        connection_type: ConnectionType | None = None,
    ) -> tuple[list[Connection], int]:
        """List connections with pagination.

        Args:
            offset: Number of records to skip.
            limit: Maximum number of records to return.
            connection_type: Optional filter by connection type.

        Returns:
            Tuple of (connections, total_count).
        """
        query = select(Connection).where(Connection.is_active == True)  # noqa: E712

        if connection_type:
            query = query.where(Connection.connection_type == connection_type)

        # Get total count
        count_result = await self.db.execute(
            select(Connection.id).where(Connection.is_active == True)  # noqa: E712
        )
        total = len(count_result.all())

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(Connection.created_at.desc())
        result = await self.db.execute(query)
        connections = list(result.scalars().all())

        return connections, total

    async def update(
        self,
        connection_id: uuid.UUID,
        name: str | None = None,
        description: str | None = None,
        config: dict[str, Any] | None = None,
        metadata_: dict[str, Any] | None = None,
    ) -> Connection:
        """Update a connection.

        Args:
            connection_id: Connection UUID.
            name: Optional new name.
            description: Optional new description.
            config: Optional new configuration (will be encrypted).
            metadata: Optional new metadata.

        Returns:
            Updated connection.
        """
        connection = await self.get(connection_id)

        if name is not None:
            connection.name = name
        if description is not None:
            connection.description = description
        if config is not None:
            connection.config_encrypted = encrypt_config(config)
        if metadata_ is not None:
            connection.metadata_ = metadata_

        await self.db.flush()
        return connection

    async def delete(self, connection_id: uuid.UUID) -> None:
        """Soft delete a connection.

        Args:
            connection_id: Connection UUID.
        """
        connection = await self.get(connection_id)
        connection.is_active = False
        await self.db.flush()

    async def test_connection(self, connection_id: uuid.UUID) -> bool:
        """Test if a connection is valid.

        Args:
            connection_id: Connection UUID.

        Returns:
            True if connection is successful.

        Raises:
            ConnectionError: If connection test fails.
        """
        connection = await self.get(connection_id)
        decrypted_config = decrypt_config(connection.config_encrypted)
        connector = get_connector(connection.connection_type, decrypted_config)
        return connector.test_connection()

    async def get_schemas(self, connection_id: uuid.UUID) -> list[str]:
        """Get list of schemas in the data source.

        Args:
            connection_id: Connection UUID.

        Returns:
            List of schema names.
        """
        connection = await self.get(connection_id)
        decrypted_config = decrypt_config(connection.config_encrypted)
        connector = get_connector(connection.connection_type, decrypted_config)

        with connector:
            return connector.get_schemas()

    async def get_tables(self, connection_id: uuid.UUID, schema: str) -> list[TableInfo]:
        """Get list of tables in a schema.

        Args:
            connection_id: Connection UUID.
            schema: Schema name.

        Returns:
            List of TableInfo objects.
        """
        connection = await self.get(connection_id)
        decrypted_config = decrypt_config(connection.config_encrypted)
        connector = get_connector(connection.connection_type, decrypted_config)

        with connector:
            return connector.get_tables(schema)

    async def get_columns(self, connection_id: uuid.UUID, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table.

        Args:
            connection_id: Connection UUID.
            schema: Schema name.
            table: Table name.

        Returns:
            List of ColumnInfo objects.
        """
        connection = await self.get(connection_id)
        decrypted_config = decrypt_config(connection.config_encrypted)
        connector = get_connector(connection.connection_type, decrypted_config)

        with connector:
            return connector.get_columns(schema, table)
