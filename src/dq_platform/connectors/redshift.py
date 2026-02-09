"""Amazon Redshift connector implementation."""

from typing import Any

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class RedshiftConnector(BaseConnector):
    """Amazon Redshift database connector using redshift_connector."""

    def connect(self) -> None:
        try:
            import redshift_connector
            self._connection = redshift_connector.connect(
                host=self.config.get("host"),
                port=int(self.config.get("port", 5439)),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
                timeout=self.config.get("connect_timeout", 10),
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redshift: {e}")

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if not self._connection:
            raise ExecutionError("Not connected to database")
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        if not self._connection:
            raise ExecutionError("Not connected to database")
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def test_connection(self) -> bool:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            return True
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")
        finally:
            self.disconnect()

    def get_schemas(self) -> list[str]:
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
            ORDER BY schema_name
        """
        results = self.execute(sql)
        return [row["schema_name"] for row in results]

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql = """
            SELECT table_schema, table_name, table_type
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
        sql = """
            SELECT column_name, data_type, is_nullable,
                   column_default, character_maximum_length,
                   numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        results = self.execute(sql, (schema, table))
        return [
            ColumnInfo(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=row["is_nullable"] == "YES",
                default_value=row["column_default"],
                character_maximum_length=row.get("character_maximum_length"),
                numeric_precision=row.get("numeric_precision"),
                numeric_scale=row.get("numeric_scale"),
            )
            for row in results
        ]
