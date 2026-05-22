"""Celery tasks for background job execution."""

import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from dq_platform.checks.check_runner import run_check
from dq_platform.checks.dqops_executor import SensorUnsupportedError
from dq_platform.config import get_settings
from dq_platform.models.check import Check
from dq_platform.models.incident import Incident, IncidentStatus
from dq_platform.models.job import Job, JobStatus
from dq_platform.models.result import CheckResult
from dq_platform.services.incident_service import IncidentService
from dq_platform.services.result_service import ResultService
from dq_platform.services.schedule_service import ScheduleService
from dq_platform.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

# NullPool: no connection pooling across asyncio.run() calls.
# Celery prefork workers each call asyncio.run() which creates a new event loop.
# Pooled asyncpg connections are bound to the loop they were created in, causing
# "Future attached to a different loop" errors. NullPool creates a fresh
# connection per session and closes it immediately on return.
_task_session_factory_instance: async_sessionmaker[AsyncSession] | None = None


def _get_task_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return a cached async session factory for task execution."""
    global _task_session_factory_instance
    if _task_session_factory_instance is None:
        settings = get_settings()
        engine = create_async_engine(
            settings.database_url,
            poolclass=NullPool,
        )
        _task_session_factory_instance = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _task_session_factory_instance


@celery_app.task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=3,
    soft_time_limit=270,  # 4.5 minutes (warning)
    time_limit=300,  # 5 minutes (hard kill)
    acks_late=True,
    reject_on_worker_lost=True,
)
def execute_check(self: Any, job_id: str) -> dict[str, Any]:
    """Execute a data quality check.

    Args:
        job_id: Job UUID as string.

    Returns:
        Execution result.
    """
    import asyncio

    try:
        return asyncio.run(_execute_check_async(self, job_id))
    except Exception as exc:
        # Ensure job is marked as failed even if async task fails completely
        import asyncio

        asyncio.run(_mark_job_failed_on_error(job_id, str(exc)))
        raise


async def _mark_job_failed_on_error(job_id: str, error_message: str) -> None:
    """Mark job as failed when execution fails completely."""
    session_factory = _get_task_session_factory()
    async with session_factory() as db:
        from sqlalchemy import select

        result = await db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if job and job.status not in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            job.status = JobStatus.FAILED
            job.error_message = f"Worker error: {error_message}"
            job.completed_at = datetime.now(UTC)
            await db.commit()


async def _execute_check_async(task: Any, job_id: str) -> dict[str, Any]:
    """Async implementation of check execution."""
    # Create a fresh session factory for this task execution
    session_factory = _get_task_session_factory()
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
                execution_time_ms=execution_result.get("execution_time_ms"),
                rows_scanned=execution_result.get("rows_scanned"),
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

        except SensorUnsupportedError as exc:
            # Genuine engine limitation, not a failure — the check can't
            # run on this dialect (e.g. regex on SQL Server). Complete the
            # job with no result row and no retry; the dimension scorer
            # then treats the check as not_assessed.
            logger.info("check skipped — sensor unsupported on dialect: %s", exc)
            job.status = JobStatus.COMPLETED
            job.error_message = f"skipped: {exc}"
            job.completed_at = datetime.now(UTC)
            await db.commit()
            return {
                "status": "skipped",
                "job_id": job_id,
                "check_id": str(check.id),
                "reason": str(exc),
            }

        except Exception as exc:
            logger.error("Task execution error", exc_info=True)

            # Update job status
            job.status = JobStatus.FAILED
            job.error_message = str(exc)
            job.completed_at = datetime.now(UTC)
            await db.commit()

            # Retry with exponential backoff + jitter to avoid thundering herd
            if task.request.retries < 3:
                base = 60 * (2**task.request.retries)
                jitter = random.uniform(0, base * 0.5)  # noqa: S311
                raise task.retry(exc=exc, countdown=int(base + jitter))

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
    """Run check execution using the unified check_runner.

    Args:
        db: Database session.
        check: Check to execute.
        connection_config: Connection configuration.

    Returns:
        Execution result dictionary.
    """
    result = await run_check(check, connection_config, db=db)
    return {
        "passed": result.passed,
        "severity": result.severity,
        "sensor_value": result.sensor_value,
        "expected": result.expected,
        "actual": result.actual,
        "message": result.message,
        "executed_sql": result.executed_sql,
        "executed_at": result.executed_at.isoformat(),
        "execution_time_ms": result.execution_time_ms,
        "rows_scanned": result.rows_scanned,
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

    # Check for existing open incident for this check (serialized via FOR UPDATE)
    result = await db.execute(
        select(Incident)
        .where(
            Incident.check_id == check.id,
            Incident.status.in_([IncidentStatus.OPEN, IncidentStatus.ACKNOWLEDGED]),
        )
        .with_for_update(of=Incident)
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


@celery_app.task  # type: ignore[untyped-decorator]
def recover_orphaned_jobs() -> dict[str, Any]:
    """Mark orphaned running/pending jobs as failed on worker startup.

    When a worker restarts, any jobs marked as 'running' or 'pending'
    in the database are orphaned. This task marks them as failed so
    they don't appear stuck forever.
    """
    import asyncio

    return asyncio.run(_recover_orphaned_jobs_async())


async def _recover_orphaned_jobs_async() -> dict[str, Any]:
    """Async implementation of orphaned job recovery."""
    session_factory = _get_task_session_factory()
    async with session_factory() as db:
        from sqlalchemy import or_, select

        # Find jobs stuck in RUNNING or PENDING state
        # Jobs are considered orphaned if:
        # - RUNNING for more than 10 minutes, OR
        # - PENDING for more than 10 minutes (started_at is null, use created_at)
        cutoff_time = datetime.now(UTC) - timedelta(minutes=10)

        result = await db.execute(
            select(Job).where(
                Job.status.in_([JobStatus.RUNNING, JobStatus.PENDING]),
                or_(
                    # Running jobs: check started_at
                    Job.started_at < cutoff_time,
                    # Pending jobs: check created_at (started_at is null)
                    Job.started_at.is_(None),
                ),
                # Ensure pending jobs are also old enough
                Job.created_at < cutoff_time,
            )
        )
        orphaned_jobs = result.scalars().all()

        recovered = 0
        for job in orphaned_jobs:
            job.status = JobStatus.FAILED
            job.error_message = "Job orphaned - worker restarted or crashed"
            job.completed_at = datetime.now(UTC)
            recovered += 1

        await db.commit()

        return {"recovered": recovered, "job_ids": [str(j.id) for j in orphaned_jobs]}


@celery_app.task  # type: ignore[untyped-decorator]
def cleanup_stuck_jobs() -> dict[str, Any]:
    """Periodically check for and fail jobs that have exceeded timeout.

    Runs every 5 minutes to find jobs running longer than the configured
    timeout and marks them as failed.
    """
    import asyncio

    return asyncio.run(_cleanup_stuck_jobs_async())


async def _cleanup_stuck_jobs_async() -> dict[str, Any]:
    """Async implementation of stuck job cleanup."""
    session_factory = _get_task_session_factory()
    async with session_factory() as db:
        from sqlalchemy import or_, select

        settings = get_settings()
        timeout_minutes = settings.check_execution_timeout // 60

        # Calculate cutoff times
        running_cutoff = datetime.now(UTC) - timedelta(seconds=settings.check_execution_timeout)
        # Pending jobs timeout after 1 hour (they should have been picked up by a worker)
        pending_cutoff = datetime.now(UTC) - timedelta(hours=1)

        # Find jobs stuck in RUNNING or PENDING state
        result = await db.execute(
            select(Job).where(
                Job.status.in_([JobStatus.RUNNING, JobStatus.PENDING]),
                or_(
                    # Running jobs: check started_at
                    Job.started_at < running_cutoff,
                    # Pending jobs: check created_at (started_at is null)
                    Job.started_at.is_(None),
                ),
                # For pending jobs, use pending_cutoff (1 hour)
                Job.created_at < pending_cutoff,
            )
        )
        stuck_jobs = result.scalars().all()

        cleaned = 0
        for job in stuck_jobs:
            original_status = job.status
            job.status = JobStatus.FAILED
            if original_status == JobStatus.RUNNING:
                job.error_message = f"Job exceeded timeout of {timeout_minutes} minutes"
            else:
                job.error_message = "Job stuck in pending - no worker picked it up within 1 hour"
            job.completed_at = datetime.now(UTC)
            cleaned += 1

        await db.commit()

        return {"cleaned": cleaned, "timeout_minutes": timeout_minutes}


async def _process_scheduled_checks_async() -> dict[str, Any]:
    """Async implementation of schedule processing."""
    import redis.asyncio as aioredis

    settings = get_settings()
    redis_client = aioredis.from_url(settings.redis_url)  # type: ignore[no-untyped-call]
    try:
        # Distributed lock prevents duplicate dispatch during rolling deploys
        lock = redis_client.lock("dq_platform:scheduler:lock", timeout=55)
        if not await lock.acquire(blocking=False):
            return {"dispatched": 0, "skipped": "lock held by another instance"}
    except Exception:
        logger.warning("Failed to acquire scheduler lock, proceeding without lock", exc_info=True)
        lock = None

    try:
        session_factory = _get_task_session_factory()
        async with session_factory() as db:
            schedule_service = ScheduleService(db)
            due_schedules = await schedule_service.get_due_schedules(batch_size=settings.schedule_batch_size)

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
    finally:
        if lock is not None:
            try:
                await lock.release()
            except Exception:
                pass
        await redis_client.aclose()
