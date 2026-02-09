"""Celery tasks for background job execution."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dq_platform.checks import Severity
from dq_platform.checks.dqops_checks import DQOpsCheckType
from dq_platform.checks.dqops_checks import get_check as get_dqops_check
from dq_platform.checks.dqops_executor import CheckExecutionResult, DQOpsExecutor, run_dqops_check
from dq_platform.checks.gx_executor import run_gx_check
from dq_platform.checks.rules import RuleType, evaluate_rule
from dq_platform.checks.sensors import get_sensor
from dq_platform.config import get_settings
from dq_platform.models.check import Check
from dq_platform.models.incident import Incident, IncidentStatus
from dq_platform.models.job import Job, JobStatus
from dq_platform.models.result import CheckResult
from dq_platform.services.connection_service import ConnectionService
from dq_platform.services.incident_service import IncidentService
from dq_platform.services.result_service import ResultService
from dq_platform.services.schedule_service import ScheduleService
from dq_platform.workers.celery_app import celery_app


def _create_task_session_factory() -> async_sessionmaker[AsyncSession]:
    """Create a fresh async session factory for task execution.

    This avoids connection pool conflicts when running async code
    inside Celery's synchronous task context with asyncio.run().
    """
    settings = get_settings()
    engine = create_async_engine(
        settings.database_url,
        pool_size=1,
        max_overflow=0,
    )
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


@celery_app.task(bind=True, max_retries=3)  # type: ignore[untyped-decorator]
def execute_check(self: Any, job_id: str) -> dict[str, Any]:
    """Execute a data quality check.

    Args:
        job_id: Job UUID as string.

    Returns:
        Execution result.
    """
    import asyncio

    return asyncio.run(_execute_check_async(self, job_id))


async def _execute_check_async(task: Any, job_id: str) -> dict[str, Any]:
    """Async implementation of check execution."""
    # Create a fresh session factory for this task execution
    session_factory = _create_task_session_factory()
    async with session_factory() as db:
        # Get job
        result = await db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            return {"status": "failed", "error": f"Job {job_id} not found"}

        # Get check
        result = await db.execute(select(Check).where(Check.id == job.check_id))
        check = result.scalar_one_or_none()

        if not check:
            job.status = JobStatus.FAILED
            job.error_message = f"Check {job.check_id} not found"
            await db.commit()
            return {"status": "failed", "error": f"Check {job.check_id} not found"}

        # Update job status
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(UTC)
        await db.commit()

        try:
            # Get connection config (includes type for connector factory)
            connection_config = check.connection.decrypted_config  # type: ignore[attr-defined]

            # Execute check
            execution_result = await _run_check_execution(db, check, connection_config)  # type: ignore[arg-type]

            # Record result
            result_service = ResultService(db)
            check_result = await result_service.create_result(
                check_id=check.id,
                job_id=job.id,
                status="passed" if execution_result["passed"] else "failed",
                severity=execution_result.get("severity", "error"),
                result_value=execution_result.get("sensor_value"),
                expected=execution_result.get("expected"),
                actual=execution_result.get("actual"),
                message=execution_result.get("message"),
                executed_sql=execution_result.get("executed_sql"),
            )

            # Create/update incident if failed
            if not execution_result["passed"]:
                await _handle_failure(db, check, check_result, execution_result)  # type: ignore[arg-type]

            # Update job status
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(UTC)
            await db.commit()

            return {
                "status": "completed",
                "job_id": job_id,
                "check_id": str(check.id),
                "passed": execution_result["passed"],
                "severity": execution_result.get("severity"),
                "result_id": str(check_result.id),
            }

        except Exception as exc:
            import traceback

            tb = traceback.format_exc()
            print(f"Task error: {exc}\n{tb}")

            # Update job status
            job.status = JobStatus.FAILED
            job.error_message = f"{exc}\n{tb}"
            job.completed_at = datetime.now(UTC)
            await db.commit()

            # Retry logic
            if task.request.retries < 3:
                raise task.retry(exc=exc, countdown=60)

            return {
                "status": "failed",
                "job_id": job_id,
                "error": str(exc),
            }


async def _run_check_execution(
    db: AsyncSession,
    check: Check,
    connection_config: dict[str, Any],
) -> dict[str, Any]:
    """Run check execution using DQOps or Great Expectations.

    Args:
        db: Database session.
        check: Check to execute.
        connection_config: Connection configuration.

    Returns:
        Execution result dictionary.
    """
    executed_at = datetime.now(UTC)

    # Try DQOps-style execution first
    try:
        dqops_check_type = DQOpsCheckType(check.check_type.value)
        dqops_check_def = get_dqops_check(dqops_check_type)

        # Build rule parameters
        rule_params = {}
        if check.rule_parameters:
            # Extract highest severity threshold
            for severity in ["fatal", "error", "warning"]:
                if severity in check.rule_parameters and check.rule_parameters[severity]:
                    rule_params.update(check.rule_parameters[severity])
                    rule_params["severity"] = severity
                    break
        if check.parameters:
            rule_params.update(check.parameters)

        # Anomaly: inject historical values
        if dqops_check_def.rule_type == RuleType.ANOMALY_PERCENTILE:
            cutoff = datetime.now(UTC) - timedelta(days=90)
            hist_result = await db.execute(
                select(CheckResult.actual_value)
                .where(
                    CheckResult.check_id == check.id,
                    CheckResult.executed_at >= cutoff,
                    CheckResult.actual_value.isnot(None),
                )
                .order_by(CheckResult.executed_at.desc())
                .limit(1000)
            )
            rule_params["_historical_values"] = [row[0] for row in hist_result.all()]

        # Cross-source: dual-connection execution (early return)
        if "reference_connection_id" in (check.parameters or {}):
            return await _run_cross_source_execution(
                db, check, connection_config, dqops_check_def, rule_params, executed_at
            )

        # Execute DQOps check
        result: CheckExecutionResult = await run_dqops_check(
            check_type=dqops_check_type,
            connection_config=connection_config,
            schema_name=check.target_schema or "public",
            table_name=check.target_table,
            column_name=check.target_column,
            rule_params=rule_params,
        )

        return {
            "passed": result.passed,
            "severity": result.severity.value,
            "sensor_value": result.sensor_value,
            "expected": result.expected,
            "actual": result.actual,
            "message": result.message,
            "executed_sql": result.executed_sql,
            "executed_at": executed_at.isoformat(),
        }

    except (ValueError, KeyError):
        # Fall back to Great Expectations execution
        pass

    # Execute via Great Expectations
    try:
        gx_result = await run_gx_check(
            check_type=check.check_type,
            connection_config=connection_config,
            schema_name=check.target_schema,
            table_name=check.target_table,
            column_name=check.target_column,
            parameters=check.parameters,
        )

        severity = Severity.PASSED if gx_result["success"] else Severity.ERROR

        return {
            "passed": gx_result["success"],
            "severity": severity.value,
            "sensor_value": gx_result.get("observed_value"),
            "expected": check.parameters,
            "actual": gx_result.get("observed_value"),
            "message": gx_result.get("result", {}).get("comment", "Check executed"),
            "executed_sql": None,
            "executed_at": executed_at.isoformat(),
        }

    except Exception as e:
        return {
            "passed": False,
            "severity": Severity.ERROR.value,
            "sensor_value": None,
            "expected": check.parameters,
            "actual": None,
            "message": f"Execution failed: {str(e)}",
            "executed_sql": None,
            "executed_at": executed_at.isoformat(),
        }


async def _run_cross_source_execution(
    db: AsyncSession,
    check: Check,
    connection_config: dict[str, Any],
    dqops_check_def: Any,
    rule_params: dict[str, Any],
    executed_at: datetime,
) -> dict[str, Any]:
    """Run cross-source comparison in the Celery worker path.

    Args:
        db: Database session.
        check: Check to execute.
        connection_config: Source connection config.
        dqops_check_def: Check definition.
        rule_params: Rule parameters.
        executed_at: Execution timestamp.

    Returns:
        Execution result dictionary.
    """
    params = check.parameters or {}
    ref_conn_id = params.get("reference_connection_id")

    conn_service = ConnectionService(db)
    ref_connection = await conn_service.get_connection(UUID(ref_conn_id))
    if not ref_connection:
        return {
            "passed": False,
            "severity": Severity.ERROR.value,
            "sensor_value": None,
            "expected": "valid reference connection",
            "actual": None,
            "message": f"Reference connection {ref_conn_id} not found",
            "executed_sql": None,
            "executed_at": executed_at.isoformat(),
        }

    ref_config = ref_connection.decrypted_config
    sensor = get_sensor(dqops_check_def.sensor_type)

    source_params = {
        "schema_name": check.target_schema or "public",
        "table_name": check.target_table,
    }
    if sensor.is_column_level and check.target_column:
        source_params["column_name"] = check.target_column

    ref_params = {
        "schema_name": params.get("reference_schema", check.target_schema or "public"),
        "table_name": params.get("reference_table", check.target_table),
    }
    if sensor.is_column_level:
        ref_params["column_name"] = params.get("reference_column", check.target_column)

    if sensor.default_params:
        source_params.update(sensor.default_params)
        ref_params.update(sensor.default_params)

    source_sql = sensor.render(source_params)
    ref_sql = sensor.render(ref_params)

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

    return {
        "passed": rule_result.passed,
        "severity": rule_result.severity.value,
        "sensor_value": match_percent,
        "expected": rule_result.expected,
        "actual": rule_result.actual,
        "message": f"Source={source_value}, Reference={ref_value}. {rule_result.message}",
        "executed_sql": f"-- Source:\n{source_sql}\n-- Reference:\n{ref_sql}",
        "executed_at": executed_at.isoformat(),
    }


async def _handle_failure(
    db: AsyncSession,
    check: Check,
    check_result: CheckResult,
    execution_result: dict[str, Any],
) -> None:
    """Handle check failure by creating or updating incident.

    Args:
        db: Database session.
        check: Check that failed.
        check_result: Check result record.
        execution_result: Execution result.
    """
    incident_service = IncidentService(db)

    # Check for existing open incident for this check
    from sqlalchemy import select

    result = await db.execute(
        select(Incident).where(
            Incident.check_id == check.id,
            Incident.status.in_([IncidentStatus.OPEN, IncidentStatus.ACKNOWLEDGED]),
        )
    )
    existing_incident = result.scalar_one_or_none()

    if existing_incident:
        # Update existing incident with new result
        existing_incident.result_id = check_result.id
        await db.flush()
    else:
        # Create new incident
        severity = execution_result.get("severity", "error")
        await incident_service.create_incident(
            check_id=check.id,
            result_id=check_result.id,
            title=f"Data Quality Check Failed: {check.name}",
            description=execution_result.get("message", "Check failed"),
            severity=severity,
        )


@celery_app.task  # type: ignore[untyped-decorator]
def process_scheduled_checks() -> dict[str, Any]:
    """Poll for due schedules and dispatch check execution jobs.

    Called by Celery Beat every 60 seconds. Finds schedules whose
    next_run_at has passed, creates a Job for each, dispatches
    execute_check, and updates the schedule's next run time.
    """
    import asyncio

    return asyncio.run(_process_scheduled_checks_async())


async def _process_scheduled_checks_async() -> dict[str, Any]:
    """Async implementation of schedule processing."""
    session_factory = _create_task_session_factory()
    async with session_factory() as db:
        schedule_service = ScheduleService(db)
        due_schedules = await schedule_service.get_due_schedules()

        dispatched = []
        for schedule in due_schedules:
            job = Job(
                check_id=schedule.check_id,
                status=JobStatus.PENDING,
                metadata_={"triggered_by": "scheduler", "schedule_id": str(schedule.id)},
            )
            db.add(job)
            await db.flush()

            execute_check.delay(str(job.id))
            await schedule_service.mark_executed(schedule.id)
            dispatched.append(str(schedule.id))

        await db.commit()
        return {"dispatched": len(dispatched), "schedule_ids": dispatched}
