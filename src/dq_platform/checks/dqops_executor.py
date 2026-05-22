"""DQOps check executor - combines sensors and rules to run checks."""

import logging
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.checks.dqops_checks import DQOpsCheckType, get_check
from dq_platform.checks.rules import Severity, evaluate_rule
from dq_platform.checks.sensors import QUOTE_CHARS, get_sensor
from dq_platform.connectors.base import BaseConnector
from dq_platform.connectors.factory import ConnectorFactory
from dq_platform.core.metrics import (
    SENSOR_TRANSPILE_FAILURES,
    SENSOR_TRANSPILE_OK,
    SENSOR_UNSUPPORTED_SKIPS,
    metrics,
)

logger = logging.getLogger(__name__)


class SensorUnsupportedError(Exception):
    """A sensor cannot run on the connection's dialect.

    Raised for genuine engine limitations (e.g. regex on SQL Server, which
    has no native T-SQL regex). The worker treats this as a clean skip —
    the check is recorded not_assessed, never retried.
    """

    def __init__(self, sensor_name: str, dialect: str) -> None:
        self.sensor_name = sensor_name
        self.dialect = dialect
        super().__init__(f"sensor '{sensor_name}' is not supported on dialect '{dialect}'")


# Sensor SQL templates are authored in PostgreSQL syntax (`::FLOAT` casts,
# `STRING_AGG`, etc.). For non-PG engines the rendered SQL is transpiled
# with sqlglot to the target dialect just before execution.
#
# Postgres and DuckDB are NOT transpiled — DuckDB is PG-syntax-compatible
# for the sensor subset, and PG is the source dialect. Both are verified
# to run sensor SQL directly.
_SQLGLOT_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "redshift": "redshift",
    "mysql": "mysql",
    "sqlserver": "tsql",
    "oracle": "oracle",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "databricks": "databricks",
    "duckdb": "duckdb",
}

# Engines whose sensor SQL runs without transpilation.
_NO_TRANSPILE = {"postgresql", "duckdb", ""}


def _transpile_sensor_sql(sql: str, conn_type: str) -> str:
    """Transpile Postgres-authored sensor SQL to the connection's dialect.

    Returns the SQL unchanged for Postgres/DuckDB, on unknown dialects, or
    if sqlglot can't parse the statement — transpilation only ever helps,
    never breaks the engines that already work.
    """
    if conn_type in _NO_TRANSPILE:
        return sql
    target = _SQLGLOT_DIALECT.get(conn_type)
    if not target:
        return sql
    try:
        out = sqlglot.transpile(sql, read="postgres", write=target)
        metrics.incr(SENSOR_TRANSPILE_OK)
        return out[0] if out else sql
    except Exception as err:  # noqa: BLE001 - fall back to raw SQL
        # Silent fallback would mask a portability regression. Count it so
        # /metrics surfaces a rising failure rate, and log loud + structured.
        metrics.incr(SENSOR_TRANSPILE_FAILURES)
        logger.warning(
            "sensor SQL transpile failed (%s): %s | SQL: %s",
            conn_type,
            err,
            sql[:200].replace("\n", " "),
        )
        return sql


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
    rows_scanned: int | None = None


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
        quote_char: str | None = None,
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

        # Determine the connection's dialect.
        conn_type = connection_config.get("type", "") or connection_config.get("connection_type", "")

        # Genuine engine limitation (e.g. regex on SQL Server) → clean skip.
        if not sensor.supports(conn_type):
            metrics.incr(SENSOR_UNSUPPORTED_SKIPS)
            raise SensorUnsupportedError(str(sensor.name), conn_type)

        # SQL rendering, three paths:
        #  - dialect_templates override → already native, render with the
        #    native quote char, no transpilation.
        #  - PG / DuckDB              → base template, native quote, no transpile.
        #  - everything else         → base template rendered with Postgres
        #    quotes, then transpiled (incl. quoting) to the target dialect.
        has_dialect_template = conn_type in sensor.dialect_templates
        needs_transpile = conn_type not in _NO_TRANSPILE and not has_dialect_template
        if quote_char is None:
            quote_char = '"' if needs_transpile else QUOTE_CHARS.get(conn_type, '"')

        sql = sensor.render(sensor_params, quote_char=quote_char, dialect=conn_type)

        # Transpile Postgres-authored sensor SQL to the target dialect.
        if needs_transpile:
            sql = _transpile_sensor_sql(sql, conn_type)

        # Execute sensor SQL
        sensor_value = await self._execute_sensor_sql(
            connection_config=connection_config,
            sql=sql,
        )

        # Capture rows_scanned by running a paired COUNT(*) on the target
        # table with the same partition filter. Cheap for most engines
        # (one aggregate query), and gives a consistent count-of-rows
        # denominator across every sensor type.
        rows_scanned = await self._count_rows(
            connection_config=connection_config,
            schema_name=schema_name,
            table_name=table_name,
            partition_filter=partition_filter,
            quote_char=quote_char,
            conn_type=conn_type,
        )

        # Build rule parameters
        final_rule_params = {}
        if check.default_params:
            final_rule_params.update(check.default_params)
        if rule_params:
            final_rule_params.update(rule_params)

        # Evaluate rule
        rule_result = evaluate_rule(
            check.rule_type,
            sensor_value,
            final_rule_params,
            category=check.category,
            description=check.description,
            sensor_type=check.sensor_type.value,
        )

        return CheckExecutionResult(
            check_type=check_type.value,
            passed=rule_result.passed,
            severity=rule_result.severity,
            sensor_value=sensor_value,
            expected=rule_result.expected,
            actual=rule_result.actual,
            message=rule_result.message,
            executed_sql=sql,
            rows_scanned=rows_scanned,
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
            logger.error(
                "Sensor SQL execution failed | SQL: %s",
                sql.replace("\n", " "),
                exc_info=True,
            )
            raise

        finally:
            if own_connector and connector:
                await connector.disconnect_async()

    async def _count_rows(
        self,
        connection_config: dict[str, Any],
        schema_name: str,
        table_name: str,
        partition_filter: str | None,
        quote_char: str,
        conn_type: str = "",
    ) -> int | None:
        """Return row count of the target table (with partition filter if any).

        Best-effort: returns None on any failure so a COUNT(*) hiccup never
        fails the check itself. Used to populate rows_scanned on results.
        """
        q = quote_char
        where = f" WHERE {partition_filter}" if partition_filter else ""
        sql = f"SELECT COUNT(*) AS sensor_value FROM {q}{schema_name}{q}.{q}{table_name}{q}{where}"
        # Match the dialect handling of the sensor SQL — a "-quoted count
        # query must be transpiled for engines like MySQL.
        if conn_type not in _NO_TRANSPILE:
            sql = _transpile_sensor_sql(sql, conn_type)
        try:
            value = await self._execute_sensor_sql(
                connection_config=connection_config,
                sql=sql,
            )
            return int(value) if value is not None else None
        except Exception:
            logger.warning("rows_scanned count query failed", extra={"sql": sql})
            return None


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
            logger.error("Local sensor SQL execution failed", extra={"sql": sql}, exc_info=True)
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
    quote_char: str | None = None,
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
        quote_char=quote_char,
    )
