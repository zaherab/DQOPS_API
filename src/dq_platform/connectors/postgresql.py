"""PostgreSQL connector implementation."""

from typing import Any

import psycopg2
import psycopg2.extras

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector using psycopg2."""

    def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            self._connection = psycopg2.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5432),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
                connect_timeout=self.config.get("connect_timeout", 10),
            )
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def disconnect(self) -> None:
        """Close the PostgreSQL connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results."""
        if not self._connection:
            raise ExecutionError("Not connected to database")

        try:
            with self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql, params)
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                return []
        except psycopg2.Error as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value."""
        if not self._connection:
            raise ExecutionError("Not connected to database")

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return row[0] if row else None
        except psycopg2.Error as e:
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
        """Get list of schemas in the database."""
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """
        results = self.execute(sql)
        return [row["schema_name"] for row in results]

    def get_tables(self, schema: str) -> list[TableInfo]:
        """Get list of tables in a schema."""
        sql = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        results = self.execute(sql, (schema,))
        return [
            TableInfo(
                schema_name=row["table_schema"],
                table_name=row["table_name"],
                table_type="VIEW" if row["table_type"] == "VIEW" else "TABLE",
            )
            for row in results
        ]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table."""
        sql = """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        results = self.execute(sql, (schema, table, schema, table))
        return [
            ColumnInfo(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=row["is_nullable"] == "YES",
                is_primary_key=row["is_primary_key"],
                default_value=row["column_default"],
                character_maximum_length=row["character_maximum_length"],
                numeric_precision=row["numeric_precision"],
                numeric_scale=row["numeric_scale"],
            )
            for row in results
        ]
