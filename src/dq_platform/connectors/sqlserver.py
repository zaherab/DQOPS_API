"""SQL Server connector implementation."""

from typing import Any

import pyodbc

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class SQLServerConnector(BaseConnector):
    """SQL Server database connector using pyodbc."""

    def connect(self) -> None:
        """Establish connection to SQL Server database."""
        try:
            driver = self.config.get("driver", "ODBC Driver 18 for SQL Server")
            host = self.config.get("host", "localhost")
            port = self.config.get("port", 1433)
            database = self.config.get("database")
            user = self.config.get("user")
            password = self.config.get("password")
            trust_cert = self.config.get("trust_server_certificate", "yes")

            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"TrustServerCertificate={trust_cert}"
            )

            self._connection = pyodbc.connect(
                connection_string,
                timeout=self.config.get("connect_timeout", 10),
            )
        except pyodbc.Error as e:
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")

    def disconnect(self) -> None:
        """Close the SQL Server connection."""
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
            cursor = self._connection.cursor()
            if params:
                cursor.execute(sql, list(params.values()))
            else:
                cursor.execute(sql)

            if cursor.description:
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
        except pyodbc.Error as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value."""
        if not self._connection:
            raise ExecutionError("Not connected to database")

        try:
            cursor = self._connection.cursor()
            if params:
                cursor.execute(sql, list(params.values()))
            else:
                cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else None
        except pyodbc.Error as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def test_connection(self) -> bool:
        """Test if the connection is valid."""
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
        """Get list of schemas in the database."""
        sql = """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
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
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
        """
        results = self.execute(sql, {"schema": schema})
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
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = ?
                    AND tc.TABLE_NAME = ?
                    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """
        results = self.execute(sql, {"s1": schema, "t1": table, "s2": schema, "t2": table})
        return [
            ColumnInfo(
                name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                is_nullable=row["IS_NULLABLE"] == "YES",
                is_primary_key=bool(row["IS_PRIMARY_KEY"]),
                default_value=row["COLUMN_DEFAULT"],
                character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
                numeric_precision=row["NUMERIC_PRECISION"],
                numeric_scale=row["NUMERIC_SCALE"],
            )
            for row in results
        ]

    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier using SQL Server brackets."""
        return f"[{identifier}]"
