"""Base connector interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnInfo:
    """Column metadata."""

    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    default_value: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass
class TableInfo:
    """Table metadata."""

    schema_name: str
    table_name: str
    table_type: str  # 'TABLE' or 'VIEW'
    row_count: int | None = None


class BaseConnector(ABC):
    """Abstract base class for database connectors.

    All connectors must implement this interface to support:
    - Connection lifecycle (connect, disconnect)
    - Query execution
    - Metadata discovery (schemas, tables, columns)
    - Connection testing

    Usage:
        connector = PostgreSQLConnector(config)
        with connector:
            result = connector.execute("SELECT 1")
            schemas = connector.get_schemas()
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize connector with configuration.

        Args:
            config: Connection configuration (host, port, database, user, password, etc.)
        """
        self.config = config
        self._connection: Any = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database.

        Raises:
            ConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def execute(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters (dict or tuple).

        Returns:
            List of result rows as dictionaries.

        Raises:
            ExecutionError: If query execution fails.
        """
        pass

    @abstractmethod
    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters.

        Returns:
            Single value from the first column of the first row.

        Raises:
            ExecutionError: If query execution fails.
        """
        pass

    async def execute_sql(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query asynchronously and return results.

        Default implementation runs the synchronous execute in a thread.
        Subclasses may override for native async support.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters.

        Returns:
            List of result rows as dictionaries.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute, sql, params)

    async def connect_async(self) -> None:
        """Establish connection to the database asynchronously.

        Default implementation calls sync connect in a thread.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.connect)

    async def disconnect_async(self) -> None:
        """Close the database connection asynchronously.

        Default implementation calls sync disconnect in a thread.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.disconnect)

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is valid.

        Returns:
            True if connection is successful.

        Raises:
            ConnectionError: If connection test fails.
        """
        pass

    @abstractmethod
    def get_schemas(self) -> list[str]:
        """Get list of schemas in the database.

        Returns:
            List of schema names.
        """
        pass

    @abstractmethod
    def get_tables(self, schema: str) -> list[TableInfo]:
        """Get list of tables in a schema.

        Args:
            schema: Schema name.

        Returns:
            List of TableInfo objects.
        """
        pass

    @abstractmethod
    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            List of ColumnInfo objects.
        """
        pass

    def __enter__(self) -> "BaseConnector":
        """Context manager entry - connect to database."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - disconnect from database."""
        self.disconnect()

    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier for safe use in SQL.

        Override in subclasses for database-specific quoting.

        Args:
            identifier: Table, column, or schema name.

        Returns:
            Quoted identifier.
        """
        return f'"{identifier}"'
