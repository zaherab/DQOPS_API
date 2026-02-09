"""Oracle database connector implementation."""

from typing import Any

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class OracleConnector(BaseConnector):
    """Oracle database connector using oracledb (thin mode, no client needed)."""

    def connect(self) -> None:
        try:
            import oracledb

            service_name: str = self.config.get("service_name") or "XEPDB1"
            dsn = oracledb.makedsn(
                host=self.config.get("host", "localhost"),
                port=int(self.config.get("port", 1521)),
                service_name=service_name,
            )
            self._connection = oracledb.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                dsn=dsn,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Oracle: {e}")

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def execute(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        if not self._connection:
            raise ExecutionError("Not connected to database")
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql, params or {})
            if cursor.description:
                columns = [desc[0].lower() for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        if not self._connection:
            raise ExecutionError("Not connected to database")
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql, params or {})
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def test_connection(self) -> bool:
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            return True
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")
        finally:
            self.disconnect()

    def get_schemas(self) -> list[str]:
        sql = """
            SELECT username AS schema_name
            FROM all_users
            ORDER BY username
        """
        results = self.execute(sql)
        return [row["schema_name"] for row in results]

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql = """
            SELECT owner AS table_schema, table_name, 'TABLE' AS table_type
            FROM all_tables
            WHERE owner = :schema
            UNION ALL
            SELECT owner AS table_schema, view_name AS table_name, 'VIEW' AS table_type
            FROM all_views
            WHERE owner = :schema
            ORDER BY 2
        """
        results = self.execute(sql, {"schema": schema.upper()})
        return [
            TableInfo(
                schema_name=row["table_schema"],
                table_name=row["table_name"],
                table_type=row["table_type"],
            )
            for row in results
        ]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        sql = """
            SELECT column_name, data_type, nullable AS is_nullable,
                   data_default AS column_default, char_length AS character_maximum_length,
                   data_precision AS numeric_precision, data_scale AS numeric_scale
            FROM all_tab_columns
            WHERE owner = :schema AND table_name = :table
            ORDER BY column_id
        """
        results = self.execute(sql, {"schema": schema.upper(), "table": table.upper()})
        return [
            ColumnInfo(
                name=row["column_name"].lower(),
                data_type=row["data_type"],
                is_nullable=row["is_nullable"] == "Y",
                default_value=row.get("column_default"),
                character_maximum_length=row.get("character_maximum_length"),
                numeric_precision=row.get("numeric_precision"),
                numeric_scale=row.get("numeric_scale"),
            )
            for row in results
        ]

    def quote_identifier(self, identifier: str) -> str:
        return f'"{identifier.upper()}"'
