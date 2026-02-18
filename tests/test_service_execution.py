"""Unit tests for ExecutionService."""

from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from uuid import uuid4

import pytest
import pytest_asyncio

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.check import Check
from dq_platform.models.job import Job, JobStatus
from dq_platform.services.execution_service import ExecutionService


class TestExecutionService:
    """Test suite for ExecutionService."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """Create a mock database session."""
        db = AsyncMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create an ExecutionService instance."""
        return ExecutionService(mock_db)

    async def test_create_job_success(self, service, mock_db):
        """Test create_job() creates a job successfully."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.id = check_id
        mock_check.is_active = True

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_check
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.create_job(
            check_id=check_id,
            triggered_by="api",
        )

        assert isinstance(result, Job)
        assert result.check_id == check_id
        assert result.status == JobStatus.PENDING
        assert result.metadata_["triggered_by"] == "api"
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    async def test_create_job_with_schedule(self, service, mock_db):
        """Test create_job() with schedule_id."""
        check_id = uuid4()
        schedule_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.id = check_id
        mock_check.is_active = True

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_check
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.create_job(
            check_id=check_id,
            triggered_by="schedule",
            schedule_id=schedule_id,
        )

        assert result.metadata_["triggered_by"] == "schedule"
        assert result.metadata_["schedule_id"] == str(schedule_id)

    async def test_create_job_check_not_found(self, service, mock_db):
        """Test create_job() raises NotFoundError when check doesn't exist."""
        check_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.create_job(check_id=check_id)

        assert "Check" in str(exc_info.value)

    async def test_get_job_success(self, service, mock_db):
        """Test get_job() returns job when found."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.id = job_id

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_job
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_job(job_id)

        assert result == mock_job

    async def test_get_job_not_found(self, service, mock_db):
        """Test get_job() raises NotFoundError when job doesn't exist."""
        job_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.get_job(job_id)

        assert "Job" in str(exc_info.value)

    async def test_list_jobs_with_filters(self, service, mock_db):
        """Test list_jobs() with filters."""
        check_id = uuid4()
        mock_jobs = [MagicMock(spec=Job) for _ in range(5)]

        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(i,) for i in range(5)]

        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_jobs

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        jobs, total = await service.list_jobs(
            check_id=check_id,
            status=JobStatus.PENDING,
            offset=0,
            limit=10,
        )

        assert jobs == mock_jobs
        assert total == 5

    async def test_update_job_status_to_running(self, service, mock_db):
        """Test update_job_status() sets started_at when status is RUNNING."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.PENDING
        mock_job.started_at = None

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            result = await service.update_job_status(
                job_id=job_id,
                status=JobStatus.RUNNING,
                celery_task_id="task-123",
            )

        assert result.status == JobStatus.RUNNING
        assert result.celery_task_id == "task-123"
        assert result.started_at is not None
        mock_db.flush.assert_called_once()

    async def test_update_job_status_to_completed(self, service, mock_db):
        """Test update_job_status() sets completed_at when status is terminal."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.RUNNING
        mock_job.completed_at = None

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            result = await service.update_job_status(
                job_id=job_id,
                status=JobStatus.COMPLETED,
            )

        assert result.status == JobStatus.COMPLETED
        assert result.completed_at is not None

    async def test_update_job_status_with_error(self, service, mock_db):
        """Test update_job_status() sets error_message."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.RUNNING

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            result = await service.update_job_status(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message="Connection timeout",
            )

        assert result.status == JobStatus.FAILED
        assert result.error_message == "Connection timeout"

    async def test_cancel_job_success(self, service, mock_db):
        """Test cancel_job() cancels a pending job."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.PENDING

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            result = await service.cancel_job(job_id)

        assert result.status == JobStatus.CANCELLED
        assert result.completed_at is not None

    async def test_cancel_job_running(self, service, mock_db):
        """Test cancel_job() cancels a running job."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.RUNNING

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            result = await service.cancel_job(job_id)

        assert result.status == JobStatus.CANCELLED

    async def test_cancel_job_completed_fails(self, service, mock_db):
        """Test cancel_job() raises error for already completed job."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.status = JobStatus.COMPLETED

        with patch.object(service, "get_job", AsyncMock(return_value=mock_job)):
            with pytest.raises(ValidationError) as exc_info:
                await service.cancel_job(job_id)

        assert "Cannot cancel" in str(exc_info.value)

    async def test_submit_job(self, service, mock_db):
        """Test submit_job() submits to Celery and updates job."""
        job_id = uuid4()
        mock_job = MagicMock(spec=Job)
        mock_job.celery_task_id = None

        # Create a mock async function for get_job
        async def mock_get_job(job_id):
            return mock_job

        service.get_job = mock_get_job

        # Patch where it's imported (inside the function uses workers.tasks)
        with patch(
            "dq_platform.workers.tasks.execute_check"
        ) as mock_execute:
            mock_task = MagicMock()
            mock_task.id = "celery-task-123"
            mock_execute.delay.return_value = mock_task

            result = await service.submit_job(job_id)

        assert result == "celery-task-123"
        assert mock_job.celery_task_id == "celery-task-123"
        mock_db.flush.assert_called_once()
