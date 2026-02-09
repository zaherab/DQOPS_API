"""Great Expectations-based check executor."""

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.datasource.fluent import SQLDatasource

from dq_platform.checks.gx_registry import build_expectation, is_column_level_check
from dq_platform.core.encryption import decrypt_config
from dq_platform.models.check import Check, CheckType
from dq_platform.models.connection import Connection, ConnectionType


@dataclass
class ExecutionResult:
    """Result of check execution."""

    check_id: uuid.UUID
    job_id: uuid.UUID
    connection_id: uuid.UUID
    target_table: str
    target_column: str | None
    check_type: str
    executed_at: datetime
    actual_value: float | None
    expected_value: float | None
    passed: bool
    execution_time_ms: int
    rows_scanned: int | None
    result_details: dict[str, Any]
    error_message: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "check_id": self.check_id,
            "job_id": self.job_id,
            "connection_id": self.connection_id,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "check_type": self.check_type,
            "executed_at": self.executed_at,
            "actual_value": self.actual_value,
            "expected_value": self.expected_value,
            "passed": self.passed,
            "execution_time_ms": self.execution_time_ms,
            "rows_scanned": self.rows_scanned,
            "result_details": self.result_details,
            "error_message": self.error_message,
        }


class GXCheckExecutor:
    """Orchestrates check execution using Great Expectations.

    The executor:
    1. Creates ephemeral GX context (no filesystem state)
    2. Creates datasource from connection config
    3. Builds expectation from check type
    4. Runs validation and captures result
    5. Returns execution result
    """

    def __init__(self) -> None:
        """Initialize executor with ephemeral GX context."""
        self.context = gx.get_context(mode="ephemeral")

    def execute(
        self,
        check: Check,
        connection: Connection,
        job_id: uuid.UUID,
    ) -> ExecutionResult:
        """Execute a check against a data source using Great Expectations.

        Args:
            check: Check definition to execute.
            connection: Connection configuration.
            job_id: ID of the job tracking this execution.

        Returns:
            ExecutionResult with check outcome.
        """
        start_time = time.time()
        executed_at = datetime.now(timezone.utc)

        try:
            # Decrypt connection config
            decrypted_config = decrypt_config(connection.config_encrypted)

            # Create datasource from connection
            datasource = self._create_datasource(connection, decrypted_config)

            # Determine schema
            schema_name = check.target_schema or "public"

            # Add table asset and get batch
            asset_name = f"asset_{check.id}_{job_id}"
            asset = datasource.add_table_asset(
                name=asset_name,
                table_name=check.target_table,
                schema_name=schema_name if connection.connection_type != ConnectionType.BIGQUERY else None,
            )

            batch_def_name = f"batch_{check.id}_{job_id}"
            batch_def = asset.add_batch_definition_whole_table(batch_def_name)

            # Build expectation from check type
            expectation = build_expectation(
                check_type=check.check_type,
                parameters=check.parameters,
                column=check.target_column,
            )

            # Create suite and add to context
            suite_name = f"suite_{check.id}_{job_id}"
            suite = ExpectationSuite(name=suite_name)
            suite.add_expectation(expectation)
            suite = self.context.suites.add(suite)

            # Create validation definition and add to context
            validation_def = gx.ValidationDefinition(
                name=f"validation_{job_id}",
                data=batch_def,
                suite=suite,
            )
            validation_def = self.context.validation_definitions.add(validation_def)

            # Run validation
            result = validation_def.run()

            # Parse result
            execution_time_ms = int((time.time() - start_time) * 1000)
            return self._parse_result(result, check, job_id, executed_at, execution_time_ms)

        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)

            return ExecutionResult(
                check_id=check.id,
                job_id=job_id,
                connection_id=connection.id,
                target_table=check.target_table,
                target_column=check.target_column,
                check_type=check.check_type.value,
                executed_at=executed_at,
                actual_value=None,
                expected_value=None,
                passed=False,
                execution_time_ms=execution_time_ms,
                rows_scanned=None,
                result_details={"error": str(e)},
                error_message=str(e),
            )

    def _create_datasource(
        self,
        connection: Connection,
        config: dict[str, Any],
    ) -> SQLDatasource:
        """Create a GX datasource from connection configuration.

        Args:
            connection: Connection model instance.
            config: Decrypted connection configuration.

        Returns:
            GX SQLDatasource instance.

        Raises:
            ValueError: If connection type is not supported.
        """
        datasource_name = f"ds_{connection.id}"

        connection_string = self._build_connection_string(
            connection.connection_type,
            config,
        )

        # Use appropriate method based on connection type
        if connection.connection_type == ConnectionType.POSTGRESQL:
            return self.context.data_sources.add_postgres(
                name=datasource_name,
                connection_string=connection_string,
            )
        elif connection.connection_type == ConnectionType.SNOWFLAKE:
            return self.context.data_sources.add_snowflake(
                name=datasource_name,
                connection_string=connection_string,
            )
        else:
            # Generic SQL datasource for MySQL, SQL Server, BigQuery
            return self.context.data_sources.add_sql(
                name=datasource_name,
                connection_string=connection_string,
            )

    def _build_connection_string(
        self,
        connection_type: ConnectionType,
        config: dict[str, Any],
    ) -> str:
        """Build SQLAlchemy connection string from config.

        Args:
            connection_type: Database type.
            config: Connection configuration dict.

        Returns:
            SQLAlchemy connection string.

        Raises:
            ValueError: If connection type is not supported.
        """
        if connection_type == ConnectionType.POSTGRESQL:
            return (
                f"postgresql+psycopg2://{config['user']}:{config['password']}"
                f"@{config['host']}:{config.get('port', 5432)}/{config['database']}"
            )

        elif connection_type == ConnectionType.MYSQL:
            return (
                f"mysql+pymysql://{config['user']}:{config['password']}"
                f"@{config['host']}:{config.get('port', 3306)}/{config['database']}"
            )

        elif connection_type == ConnectionType.SQLSERVER:
            driver = config.get("driver", "ODBC+Driver+18+for+SQL+Server")
            return (
                f"mssql+pyodbc://{config['user']}:{config['password']}"
                f"@{config['host']}:{config.get('port', 1433)}/{config['database']}"
                f"?driver={driver}"
            )

        elif connection_type == ConnectionType.BIGQUERY:
            project_id = config["project_id"]
            dataset = config.get("dataset", "")
            return f"bigquery://{project_id}/{dataset}" if dataset else f"bigquery://{project_id}"

        elif connection_type == ConnectionType.SNOWFLAKE:
            account = config["account"]
            user = config["user"]
            password = config["password"]
            database = config["database"]
            warehouse = config.get("warehouse", "")
            schema = config.get("schema", "PUBLIC")
            role = config.get("role", "")

            conn_str = (
                f"snowflake://{user}:{password}@{account}/{database}/{schema}"
            )
            params = []
            if warehouse:
                params.append(f"warehouse={warehouse}")
            if role:
                params.append(f"role={role}")
            if params:
                conn_str += "?" + "&".join(params)
            return conn_str

        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    def _parse_result(
        self,
        validation_result: Any,
        check: Check,
        job_id: uuid.UUID,
        executed_at: datetime,
        execution_time_ms: int,
    ) -> ExecutionResult:
        """Parse GX validation result into ExecutionResult.

        Args:
            validation_result: GX validation result object.
            check: Check definition.
            job_id: Job UUID.
            executed_at: Execution timestamp.
            execution_time_ms: Execution time in milliseconds.

        Returns:
            ExecutionResult instance.
        """
        # Extract results from validation
        results = validation_result.results
        passed = validation_result.success

        # Extract details from first expectation result
        actual_value = None
        expected_value = None
        result_details: dict[str, Any] = {}

        if results:
            exp_result = results[0]
            result_dict = exp_result.to_json_dict() if hasattr(exp_result, "to_json_dict") else {}

            # Try to extract observed value
            if hasattr(exp_result, "result"):
                res = exp_result.result
                if isinstance(res, dict):
                    actual_value = res.get("observed_value")
                    # For row count checks
                    if "element_count" in res:
                        actual_value = res.get("element_count")

            # Convert actual_value to float for storage in check_results.actual_value column
            # Store non-numeric values in result_details instead
            actual_value = self._convert_to_float(actual_value, result_details)

            # Build result details
            result_details = {
                "expectation_type": exp_result.expectation_config.type if hasattr(exp_result, "expectation_config") else check.check_type.value,
                "success": exp_result.success if hasattr(exp_result, "success") else passed,
                "gx_result": result_dict,
            }

            # Add message based on pass/fail
            if passed:
                result_details["message"] = f"Check passed: {check.check_type.value}"
            else:
                result_details["message"] = f"Check failed: {check.check_type.value}"
                if hasattr(exp_result, "exception_info") and exp_result.exception_info:
                    result_details["exception"] = str(exp_result.exception_info)

        return ExecutionResult(
            check_id=check.id,
            job_id=job_id,
            connection_id=check.connection_id,
            target_table=check.target_table,
            target_column=check.target_column,
            check_type=check.check_type.value,
            executed_at=executed_at,
            actual_value=actual_value,
            expected_value=expected_value,
            passed=passed,
            execution_time_ms=execution_time_ms,
            rows_scanned=None,
            result_details=result_details,
            error_message=None,
        )

    def _convert_to_float(
        self,
        value: Any,
        result_details: dict[str, Any],
    ) -> float | None:
        """Convert actual value to float for database storage.

        Non-numeric values are stored in result_details['observed_value'].

        Args:
            value: The observed value from GX.
            result_details: Dict to store non-numeric values.

        Returns:
            Float value or None if not convertible.
        """
        if value is None:
            return None

        # Handle datetime - convert to epoch timestamp
        if isinstance(value, datetime):
            return value.timestamp()

        # Handle numeric types
        if isinstance(value, (int, float)):
            return float(value)

        # Handle boolean
        if isinstance(value, bool):
            return 1.0 if value else 0.0

        # Handle lists/arrays - store in result_details and return count
        if isinstance(value, (list, tuple, set)):
            result_details["observed_value"] = list(value) if not isinstance(value, list) else value
            return float(len(value)) if value else None

        # Handle dicts - store in result_details
        if isinstance(value, dict):
            result_details["observed_value"] = value
            return None

        # Try to convert string to float
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                result_details["observed_value"] = value
                return None

        # Unknown type - try conversion, store if fails
        try:
            return float(value)
        except (ValueError, TypeError):
            result_details["observed_value"] = str(value)
            return None


# Alias for backwards compatibility
GreatExpectationsExecutor = GXCheckExecutor


async def run_gx_check(
    check_type: CheckType,
    connection_config: dict[str, Any],
    schema_name: str | None,
    table_name: str,
    column_name: str | None,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Run a Great Expectations check and return the result.

    This is a simplified async wrapper for GX check execution.
    In production, this would use the GXCheckExecutor with proper connection handling.

    Args:
        check_type: Type of check to run.
        connection_config: Connection configuration.
        schema_name: Schema name.
        table_name: Table name.
        column_name: Column name (for column-level checks).
        parameters: Check parameters.

    Returns:
        Check result dictionary.
    """
    # This is a placeholder implementation
    # In production, this would execute the actual GX check
    return {
        "success": True,
        "observed_value": None,
        "result": {"comment": "GX check execution placeholder"},
    }
