"""BigQuery connector implementation."""

from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

from dq_platform.api.errors import ConnectionError, ExecutionError
from dq_platform.connectors.base import BaseConnector, ColumnInfo, TableInfo


class BigQueryConnector(BaseConnector):
    """Google BigQuery connector using google-cloud-bigquery."""

    def connect(self) -> None:
        """Establish connection to BigQuery."""
        try:
            project_id = self.config.get("project_id")
            credentials_json = self.config.get("credentials_json")

            if credentials_json:
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_json
                )
                self._connection = bigquery.Client(
                    project=project_id,
                    credentials=credentials,
                )
            else:
                # Use default credentials (e.g., from environment)
                self._connection = bigquery.Client(project=project_id)

        except Exception as e:
            raise ConnectionError(f"Failed to connect to BigQuery: {e}")

    def disconnect(self) -> None:
        """Close the BigQuery connection."""
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
            raise ExecutionError("Not connected to BigQuery")

        try:
            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
                ]

            query_job = self._connection.query(sql, job_config=job_config)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {e}")

    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value."""
        results = self.execute(sql, params)
        if results and len(results) > 0:
            first_row = results[0]
            if first_row:
                return list(first_row.values())[0]
        return None

    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        try:
            self.connect()
            self.execute("SELECT 1")
            return True
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")
        finally:
            self.disconnect()

    def get_schemas(self) -> list[str]:
        """Get list of datasets (schemas) in BigQuery project."""
        if not self._connection:
            raise ExecutionError("Not connected to BigQuery")

        try:
            datasets = list(self._connection.list_datasets())
            return sorted([ds.dataset_id for ds in datasets])
        except Exception as e:
            raise ExecutionError(f"Failed to list datasets: {e}")

    def get_tables(self, schema: str) -> list[TableInfo]:
        """Get list of tables in a dataset."""
        if not self._connection:
            raise ExecutionError("Not connected to BigQuery")

        try:
            tables = list(self._connection.list_tables(schema))
            return [
                TableInfo(
                    schema_name=schema,
                    table_name=table.table_id,
                    table_type="VIEW" if table.table_type == "VIEW" else "TABLE",
                )
                for table in tables
            ]
        except Exception as e:
            raise ExecutionError(f"Failed to list tables: {e}")

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table."""
        if not self._connection:
            raise ExecutionError("Not connected to BigQuery")

        try:
            table_ref = self._connection.get_table(f"{schema}.{table}")
            return [
                ColumnInfo(
                    name=field.name,
                    data_type=field.field_type,
                    is_nullable=field.mode != "REQUIRED",
                    is_primary_key=False,  # BigQuery doesn't have traditional PKs
                )
                for field in table_ref.schema
            ]
        except Exception as e:
            raise ExecutionError(f"Failed to get columns: {e}")

    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier using BigQuery backticks."""
        return f"`{identifier}`"
