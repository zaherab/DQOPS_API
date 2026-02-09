"""Execution service - job creation and queue submission."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.check import Check
from dq_platform.models.job import Job, JobStatus


class ExecutionService:
    """Service for managing check execution jobs."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_job(
        self,
        check_id: uuid.UUID,
        triggered_by: str = "api",
        schedule_id: uuid.UUID | None = None,
    ) -> Job:
        """Create a new execution job.

        Args:
            check_id: Check UUID to execute.
            triggered_by: Who/what triggered the job (api, schedule, etc.).
            schedule_id: Optional schedule UUID if triggered by schedule.

        Returns:
            Created job.

        Raises:
            NotFoundError: If check not found.
        """
        # Verify check exists
        result = await self.db.execute(
            select(Check).where(
                Check.id == check_id,
                Check.is_active == True,  # noqa: E712
            )
        )
        check = result.scalar_one_or_none()

        if not check:
            raise NotFoundError("Check", str(check_id))

        job = Job(
            check_id=check_id,
            status=JobStatus.PENDING,
            metadata_={
                "triggered_by": triggered_by,
                "schedule_id": str(schedule_id) if schedule_id else None,
            },
        )

        self.db.add(job)
        await self.db.flush()
        return job

    async def get_job(self, job_id: uuid.UUID) -> Job:
        """Get a job by ID.

        Args:
            job_id: Job UUID.

        Returns:
            Job instance.

        Raises:
            NotFoundError: If job not found.
        """
        result = await self.db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            raise NotFoundError("Job", str(job_id))

        return job

    async def list_jobs(
        self,
        offset: int = 0,
        limit: int = 100,
        check_id: uuid.UUID | None = None,
        status: JobStatus | None = None,
    ) -> tuple[list[Job], int]:
        """List jobs with pagination and filters.

        Args:
            offset: Number of records to skip.
            limit: Maximum number of records to return.
            check_id: Optional filter by check.
            status: Optional filter by status.

        Returns:
            Tuple of (jobs, total_count).
        """
        query = select(Job)

        if check_id:
            query = query.where(Job.check_id == check_id)
        if status:
            query = query.where(Job.status == status)

        # Get total count
        count_query = select(Job.id)
        if check_id:
            count_query = count_query.where(Job.check_id == check_id)
        if status:
            count_query = count_query.where(Job.status == status)

        count_result = await self.db.execute(count_query)
        total = len(count_result.all())

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(Job.created_at.desc())
        result = await self.db.execute(query)
        jobs = list(result.scalars().all())

        return jobs, total

    async def update_job_status(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        celery_task_id: str | None = None,
        error_message: str | None = None,
    ) -> Job:
        """Update job status.

        Args:
            job_id: Job UUID.
            status: New status.
            celery_task_id: Optional Celery task ID.
            error_message: Optional error message.

        Returns:
            Updated job.
        """
        job = await self.get_job(job_id)

        job.status = status
        if celery_task_id:
            job.celery_task_id = celery_task_id
        if error_message:
            job.error_message = error_message

        if status == JobStatus.RUNNING:
            job.started_at = datetime.now(timezone.utc)
        elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            job.completed_at = datetime.now(timezone.utc)

        await self.db.flush()
        return job

    async def cancel_job(self, job_id: uuid.UUID) -> Job:
        """Cancel a pending or running job.

        Args:
            job_id: Job UUID.

        Returns:
            Updated job.

        Raises:
            ValidationError: If job cannot be cancelled.
        """
        job = await self.get_job(job_id)

        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            raise ValidationError(
                f"Cannot cancel job with status '{job.status.value}'"
            )

        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(timezone.utc)

        await self.db.flush()
        return job

    async def submit_job(self, job_id: uuid.UUID) -> str:
        """Submit a job to the Celery queue.

        Args:
            job_id: Job UUID.

        Returns:
            Celery task ID.
        """
        from dq_platform.workers.tasks import execute_check

        # Submit to Celery
        task = execute_check.delay(str(job_id))

        # Update job with task ID
        job = await self.get_job(job_id)
        job.celery_task_id = task.id
        await self.db.flush()

        return task.id
