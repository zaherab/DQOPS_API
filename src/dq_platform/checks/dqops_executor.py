"""DQOps check executor - combines sensors and rules to run checks."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.checks.dqops_checks import DQOpsCheckType, get_check
from dq_platform.checks.rules import Severity, evaluate_rule
from dq_platform.checks.sensors import get_sensor
from dq_platform.connectors.base import BaseConnector
from dq_platform.connectors.factory import ConnectorFactory


@dataclass
class CheckExecutionResult:
    """Result of executing a DQOps check."""

    check_type: str
    passed: bool
    severity: Severity
    sensor_value: float | None
    expected: Any
    actual: Any
    message: str
    executed_sql: str


class DQOpsExecutor:
    """Executor for DQOps-style checks."""

    def __init__(self, connector: BaseConnector | None = None):
        """Initialize the executor.

        Args:
            connector: Optional connector to use for executing SQL.
                       If not provided, one will be created per check.
        """
        self.connector = connector

    async def execute_check(
        self,
        check_type: DQOpsCheckType,
        connection_config: dict[str, Any],
        schema_name: str,
        table_name: str,
        column_name: str | None = None,
        rule_params: dict[str, Any] | None = None,
        partition_filter: str | None = None,
    ) -> CheckExecutionResult:
        """Execute a DQOps check.

        Args:
            check_type: The type of check to execute.
            connection_config: Connection configuration for the data source.
            schema_name: Schema/database name.
            table_name: Table name.
            column_name: Column name (for column-level checks).
            rule_params: Parameters for the rule (thresholds, etc.).
            partition_filter: Optional partition filter for partitioned checks.

        Returns:
            The check execution result.
        """
        # Get check definition
        check = get_check(check_type)

        # Get sensor
        sensor = get_sensor(check.sensor_type)

        # Build sensor parameters
        sensor_params = {
            "schema_name": schema_name,
            "table_name": table_name,
        }

        if sensor.is_column_level and column_name:
            sensor_params["column_name"] = column_name

        if partition_filter:
            sensor_params["partition_filter"] = partition_filter

        # Add default sensor params
        if sensor.default_params:
            sensor_params.update(sensor.default_params)

        # Add rule params to sensor params (for params like regex_pattern)
        if rule_params:
            sensor_params.update(rule_params)

        # Render SQL
        sql = sensor.render(sensor_params)

        # Execute sensor SQL
        sensor_value = await self._execute_sensor_sql(
            connection_config=connection_config,
            sql=sql,
        )

        # Build rule parameters
        final_rule_params = {}
        if check.default_params:
            final_rule_params.update(check.default_params)
        if rule_params:
            final_rule_params.update(rule_params)

        # Evaluate rule
        rule_result = evaluate_rule(check.rule_type, sensor_value, final_rule_params)

        return CheckExecutionResult(
            check_type=check_type.value,
            passed=rule_result.passed,
            severity=rule_result.severity,
            sensor_value=sensor_value,
            expected=rule_result.expected,
            actual=rule_result.actual,
            message=rule_result.message,
            executed_sql=sql,
        )

    async def _execute_sensor_sql(
        self,
        connection_config: dict[str, Any],
        sql: str,
    ) -> float | None:
        """Execute sensor SQL and return the value.

        Args:
            connection_config: Connection configuration.
            sql: SQL to execute.

        Returns:
            The sensor value, or None if execution failed.
        """
        connector = self.connector
        own_connector = False

        try:
            if connector is None:
                # Create connector from config
                connector = ConnectorFactory.create_connector(connection_config)
                own_connector = True

            # Connect and execute
            await connector.connect_async()
            result = await connector.execute_sql(sql.strip())

            # Extract value
            if result and len(result) > 0:
                row = result[0]
                value = row.get("sensor_value")
                if value is not None:
                    return float(value)

            return None

        except Exception:
            return None

        finally:
            if own_connector and connector:
                await connector.disconnect_async()


class DQOpsLocalExecutor:
    """Executor for DQOps checks using local database session.

    This executor is used when checks are run against the local results database
    rather than external data sources (e.g., for historical comparison).
    """

    def __init__(self, db: AsyncSession):
        """Initialize with database session.

        Args:
            db: Async database session.
        """
        self.db = db

    async def execute_sensor_sql(
        self,
        sql: str,
    ) -> float | None:
        """Execute sensor SQL against local database.

        Args:
            sql: SQL to execute.

        Returns:
            The sensor value, or None if execution failed.
        """
        try:
            result = await self.db.execute(text(sql.strip()))
            row = result.fetchone()

            if row and hasattr(row, "_mapping"):
                value = row._mapping.get("sensor_value")
                if value is not None:
                    return float(value)

            return None

        except Exception:
            return None


async def run_dqops_check(
    check_type: DQOpsCheckType,
    connection_config: dict[str, Any],
    schema_name: str,
    table_name: str,
    column_name: str | None = None,
    rule_params: dict[str, Any] | None = None,
    partition_filter: str | None = None,
    connector: BaseConnector | None = None,
) -> CheckExecutionResult:
    """Convenience function to run a DQOps check.

    Args:
        check_type: The type of check to execute.
        connection_config: Connection configuration for the data source.
        schema_name: Schema/database name.
        table_name: Table name.
        column_name: Column name (for column-level checks).
        rule_params: Parameters for the rule (thresholds, etc.).
        partition_filter: Optional partition filter.
        connector: Optional connector to use.

    Returns:
        The check execution result.
    """
    executor = DQOpsExecutor(connector)
    return await executor.execute_check(
        check_type=check_type,
        connection_config=connection_config,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        rule_params=rule_params,
        partition_filter=partition_filter,
    )
