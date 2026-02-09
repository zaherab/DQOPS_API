"""Databricks connector implementation."""

from typing import Any

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class DatabricksConnector(BaseConnector):
    """Databricks SQL connector using databricks-sql-connector."""

    def connect(self) -> None:
        try:
            from databricks import sql
            self._connection = sql.connect(
                server_hostname=self.config.get("server_hostname"),
                http_path=self.config.get("http_path"),
                access_token=self.config.get("access_token"),
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Databricks: {e}")

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
        finally:
            cursor.close()

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
        finally:
            cursor.close()

    def test_connection(self) -> bool:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")
        finally:
            self.disconnect()

    def get_schemas(self) -> list[str]:
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            ORDER BY schema_name
        """
        results = self.execute(sql)
        return [row["schema_name"] for row in results]

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = :schema
            ORDER BY table_name
        """
        results = self.execute(sql, {"schema": schema})
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
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """
        results = self.execute(sql, {"schema": schema, "table": table})
        return [
            ColumnInfo(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=row.get("is_nullable") == "YES",
                default_value=row.get("column_default"),
                character_maximum_length=row.get("character_maximum_length"),
                numeric_precision=row.get("numeric_precision"),
                numeric_scale=row.get("numeric_scale"),
            )
            for row in results
        ]

    def quote_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"
