"""MySQL connector implementation."""

from typing import Any

import pymysql
import pymysql.cursors

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class MySQLConnector(BaseConnector):
    """MySQL database connector using pymysql."""

    def connect(self) -> None:
        """Establish connection to MySQL database."""
        try:
            self._connection = pymysql.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 3306),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
                connect_timeout=self.config.get("connect_timeout", 10),
                cursorclass=pymysql.cursors.DictCursor,
            )
        except pymysql.Error as e:
            raise ConnectionError(f"Failed to connect to MySQL: {e}")

    def disconnect(self) -> None:
        """Close the MySQL connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def execute(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results."""
        if not self._connection:
            raise ExecutionError("Not connected to database")

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                return list(cursor.fetchall())
        except pymysql.Error as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value."""
        if not self._connection:
            raise ExecutionError("Not connected to database")

        try:
            with self._connection.cursor(pymysql.cursors.Cursor) as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return row[0] if row else None
        except pymysql.Error as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        try:
            self.connect()
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")
        finally:
            self.disconnect()

    def get_schemas(self) -> list[str]:
        """Get list of schemas (databases) in MySQL."""
        sql = """
            SELECT SCHEMA_NAME
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY SCHEMA_NAME
        """
        results = self.execute(sql)
        return [row["SCHEMA_NAME"] for row in results]

    def get_tables(self, schema: str) -> list[TableInfo]:
        """Get list of tables in a schema."""
        sql = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """
        results = self.execute(sql, (schema,))
        return [
            TableInfo(
                schema_name=row["TABLE_SCHEMA"],
                table_name=row["TABLE_NAME"],
                table_type="VIEW" if row["TABLE_TYPE"] == "VIEW" else "TABLE",
            )
            for row in results
        ]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table."""
        sql = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COLUMN_KEY
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        results = self.execute(sql, (schema, table))
        return [
            ColumnInfo(
                name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                is_nullable=row["IS_NULLABLE"] == "YES",
                is_primary_key=row["COLUMN_KEY"] == "PRI",
                default_value=row["COLUMN_DEFAULT"],
                character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
                numeric_precision=row["NUMERIC_PRECISION"],
                numeric_scale=row["NUMERIC_SCALE"],
            )
            for row in results
        ]

    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier using MySQL backticks."""
        return f"`{identifier}`"
