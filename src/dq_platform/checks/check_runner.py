"""Unified check execution logic shared between API preview and Celery worker paths."""

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.checks import Severity, run_dqops_check
from dq_platform.checks.dqops_checks import DQOpsCheckType
from dq_platform.checks.dqops_checks import get_check as get_dqops_check_def
from dq_platform.checks.dqops_executor import DQOpsExecutor
from dq_platform.checks.gx_executor import run_gx_check
from dq_platform.checks.rules import RuleType, evaluate_rule
from dq_platform.checks.sensors import QUOTE_CHARS, get_sensor
from dq_platform.models.check import Check
from dq_platform.models.result import CheckResult


@dataclass
class CheckRunResult:
    """Result of running a check."""

    passed: bool
    severity: str
    sensor_value: float | None
    expected: Any
    actual: Any
    message: str
    executed_sql: str | None
    executed_at: datetime
    execution_time_ms: int | None = None
    rows_scanned: int | None = None


async def run_check(
    check: Check,
    connection_config: dict[str, Any],
    db: AsyncSession | None = None,
) -> CheckRunResult:
    """Run a data quality check using DQOps or Great Expectations.

    This is the single source of truth for check execution logic,
    used by both the API preview path and the Celery worker path.

    Args:
        check: Check definition to execute.
        connection_config: Connection configuration (includes 'type' key).
        db: Optional database session for historical value lookups (anomaly checks).

    Returns:
        Check execution result.
    """
    executed_at = datetime.now(UTC)
    t0 = time.perf_counter()

    # Resolve the check definition. Only an unknown check type falls back to
    # GX — any error beyond this point (sensor render, SQL execution, rule
    # evaluation) is a real execution error and must propagate so the job's
    # error_message reflects the actual cause, not a misleading "GX not
    # implemented" from the fallback path.
    try:
        dqops_check_type = DQOpsCheckType(check.check_type.value)
        dqops_check_def = get_dqops_check_def(dqops_check_type)
    except (ValueError, KeyError):
        return await _run_gx_fallback(check, connection_config, executed_at, t0)

    # Build rule parameters
    rule_params = _build_rule_params(check)

    # Anomaly: inject historical values
    if dqops_check_def.rule_type == RuleType.ANOMALY_PERCENTILE and db is not None:
        rule_params["_historical_values"] = await _get_historical_values(db, check)

    # Cross-source: dual-connection execution
    if "reference_connection_id" in (check.parameters or {}):
        return await _run_cross_source(check, connection_config, dqops_check_def, rule_params, executed_at, db, t0)

    # Execute DQOps check. quote_char is left None so the executor picks
    # the right quoting per dialect — it renders sensor SQL in Postgres
    # form and transpiles (incl. quoting) to the target engine. Passing a
    # pre-computed quote char here would bake in dialect quoting that
    # sqlglot then can't read back as Postgres.
    result = await run_dqops_check(
        check_type=dqops_check_type,
        connection_config=connection_config,
        schema_name=check.target_schema or "public",
        table_name=check.target_table,
        column_name=check.target_column,
        rule_params=rule_params,
    )

    return CheckRunResult(
        passed=result.passed,
        severity=result.severity.value,
        sensor_value=result.sensor_value,
        expected=result.expected,
        actual=result.actual,
        message=result.message,
        executed_sql=result.executed_sql,
        executed_at=executed_at,
        execution_time_ms=int((time.perf_counter() - t0) * 1000),
        rows_scanned=result.rows_scanned,
    )


async def _run_gx_fallback(
    check: Check,
    connection_config: dict[str, Any],
    executed_at: datetime,
    t0: float,
) -> CheckRunResult:
    """Fallback execution path for check types not in the DQOps registry."""
    gx_result = await run_gx_check(
        check_type=check.check_type,
        connection_config=connection_config,
        schema_name=check.target_schema,
        table_name=check.target_table,
        column_name=check.target_column,
        parameters=check.parameters,
    )

    severity = Severity.PASSED if gx_result["success"] else Severity.ERROR

    return CheckRunResult(
        passed=gx_result["success"],
        severity=severity.value,
        sensor_value=gx_result.get("observed_value"),
        expected=check.parameters,
        actual=gx_result.get("observed_value"),
        message=gx_result.get("result", {}).get("comment", "Check executed"),
        executed_sql=None,
        executed_at=executed_at,
        execution_time_ms=int((time.perf_counter() - t0) * 1000),
    )


def _build_rule_params(check: Check) -> dict[str, Any]:
    """Build rule parameters from check configuration."""
    rule_params: dict[str, Any] = {}
    if check.rule_parameters:
        for severity in ["fatal", "error", "warning"]:
            if severity in check.rule_parameters and check.rule_parameters[severity]:
                rule_params.update(check.rule_parameters[severity])
                rule_params["severity"] = severity
                break
    if check.parameters:
        rule_params.update(check.parameters)
    return rule_params


async def _get_historical_values(db: AsyncSession, check: Check, days: int = 90) -> list[float]:
    """Get historical sensor values for anomaly detection."""
    cutoff = datetime.now(UTC) - timedelta(days=days)
    result = await db.execute(
        select(CheckResult.actual_value)
        .where(
            CheckResult.check_id == check.id,
            CheckResult.executed_at >= cutoff,
            CheckResult.actual_value.isnot(None),
        )
        .order_by(CheckResult.executed_at.desc())
        .limit(1000)
    )
    return [row[0] for row in result.all()]


async def _run_cross_source(
    check: Check,
    connection_config: dict[str, Any],
    dqops_check_def: Any,
    rule_params: dict[str, Any],
    executed_at: datetime,
    db: AsyncSession | None,
    t0: float,
) -> CheckRunResult:
    """Run cross-source comparison check."""
    from uuid import UUID

    from dq_platform.services.connection_service import ConnectionService

    params = check.parameters or {}
    ref_conn_id = params.get("reference_connection_id")

    if not db:
        return CheckRunResult(
            passed=False,
            severity=Severity.ERROR.value,
            sensor_value=None,
            expected="database session required for cross-source checks",
            actual=None,
            message="Cross-source check requires a database session",
            executed_sql=None,
            executed_at=executed_at,
            execution_time_ms=int((time.perf_counter() - t0) * 1000),
        )

    conn_service = ConnectionService(db)
    ref_connection = await conn_service.get_connection(UUID(ref_conn_id))
    if not ref_connection:
        return CheckRunResult(
            passed=False,
            severity=Severity.ERROR.value,
            sensor_value=None,
            expected="valid reference connection",
            actual=None,
            message=f"Reference connection {ref_conn_id} not found",
            executed_sql=None,
            executed_at=executed_at,
            execution_time_ms=int((time.perf_counter() - t0) * 1000),
        )

    ref_config = ref_connection.decrypted_config
    sensor = get_sensor(dqops_check_def.sensor_type)

    # Determine quote chars for source and reference connections
    source_qc = QUOTE_CHARS.get(connection_config.get("type", ""), '"')
    ref_qc = QUOTE_CHARS.get(ref_config.get("type", ""), '"')

    source_params: dict[str, Any] = {
        "schema_name": check.target_schema or "public",
        "table_name": check.target_table,
    }
    if sensor.is_column_level and check.target_column:
        source_params["column_name"] = check.target_column

    ref_params: dict[str, Any] = {
        "schema_name": params.get("reference_schema", check.target_schema or "public"),
        "table_name": params.get("reference_table", check.target_table),
    }
    if sensor.is_column_level:
        ref_params["column_name"] = params.get("reference_column", check.target_column)

    if sensor.default_params:
        source_params.update(sensor.default_params)
        ref_params.update(sensor.default_params)

    source_sql = sensor.render(source_params, quote_char=source_qc)
    ref_sql = sensor.render(ref_params, quote_char=ref_qc)

    executor = DQOpsExecutor()
    source_value = await executor._execute_sensor_sql(connection_config, source_sql)
    ref_value = await executor._execute_sensor_sql(ref_config, ref_sql)

    if source_value is None or ref_value is None:
        match_percent = None
    elif source_value == 0 and ref_value == 0:
        match_percent = 100.0
    elif max(abs(source_value), abs(ref_value)) == 0:
        match_percent = 0.0
    else:
        match_percent = min(abs(source_value), abs(ref_value)) / max(abs(source_value), abs(ref_value)) * 100.0

    rule_result = evaluate_rule(dqops_check_def.rule_type, match_percent, rule_params)

    return CheckRunResult(
        passed=rule_result.passed,
        severity=rule_result.severity.value,
        sensor_value=match_percent,
        expected=rule_result.expected,
        actual=rule_result.actual,
        message=f"Source={source_value}, Reference={ref_value}. {rule_result.message}",
        executed_sql=f"-- Source:\n{source_sql}\n-- Reference:\n{ref_sql}",
        executed_at=executed_at,
        execution_time_ms=int((time.perf_counter() - t0) * 1000),
    )
