"""Unit tests for ScheduleService."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.check import Check
from dq_platform.models.schedule import Schedule
from dq_platform.services.schedule_service import ScheduleService


class TestScheduleService:
    """Test suite for ScheduleService."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """Create a mock database session."""
        db = AsyncMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        db.delete = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create a ScheduleService instance."""
        return ScheduleService(mock_db)

    @pytest.fixture
    def mock_check(self):
        """Create a mock check."""
        check = MagicMock(spec=Check)
        check.id = uuid4()
        check.is_active = True
        return check

    async def test_create_success(self, service, mock_db):
        """Test create() creates a schedule successfully."""
        from datetime import datetime, UTC
        check_id = uuid4()

        # Create a real Check object
        real_check = Check(
            id=check_id,
            name="test-check",
            connection_id=uuid4(),
            check_type="nulls_percent",
            target_table="users",
            is_active=True,
        )

        # Mock check lookup
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = real_check
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.create(
            name="daily-check",
            check_id=check_id,
            cron_expression="0 0 * * *",
            timezone_str="UTC",
            description="Run daily at midnight",
        )

        assert isinstance(result, Schedule)
        assert result.name == "daily-check"
        assert result.check_id == check_id
        assert result.cron_expression == "0 0 * * *"
        assert result.timezone == "UTC"
        assert result.description == "Run daily at midnight"
        # is_active defaults to True in the model, but the Schedule object created
        # directly won't have the default until flushed to DB
        assert result.next_run_at is not None
        assert isinstance(result.next_run_at, datetime)
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    async def test_create_invalid_cron(self, service, mock_db):
        """Test create() raises ValidationError for invalid cron."""
        check_id = uuid4()

        with pytest.raises(ValidationError) as exc_info:
            await service.create(
                name="invalid-schedule",
                check_id=check_id,
                cron_expression="invalid-cron-expression",
            )

        assert "Invalid cron" in str(exc_info.value)

    async def test_create_check_not_found(self, service, mock_db):
        """Test create() raises NotFoundError when check doesn't exist."""
        check_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.create(
                name="orphan-schedule",
                check_id=check_id,
                cron_expression="0 0 * * *",
            )

        assert "Check" in str(exc_info.value)

    async def test_get_success(self, service, mock_db):
        """Test get() returns schedule when found."""
        schedule_id = uuid4()
        mock_schedule = MagicMock(spec=Schedule)
        mock_schedule.id = schedule_id

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_schedule
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get(schedule_id)

        assert result == mock_schedule

    async def test_get_not_found(self, service, mock_db):
        """Test get() raises NotFoundError when schedule doesn't exist."""
        schedule_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.get(schedule_id)

        assert "Schedule" in str(exc_info.value)

    async def test_list_schedules_with_filters(self, service, mock_db):
        """Test list_schedules() with filters."""
        check_id = uuid4()
        mock_schedules = [MagicMock(spec=Schedule) for _ in range(5)]

        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(i,) for i in range(5)]

        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_schedules

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        schedules, total = await service.list_schedules(
            check_id=check_id,
            is_active=True,
            offset=0,
            limit=10,
        )

        assert schedules == mock_schedules
        assert total == 5

    async def test_update_success(self, service, mock_db):
        """Test update() updates schedule fields."""
        schedule_id = uuid4()
        mock_schedule = MagicMock(spec=Schedule)
        mock_schedule.id = schedule_id
        mock_schedule.cron_expression = "0 0 * * *"
        mock_schedule.timezone = "UTC"
        mock_schedule.next_run_at = datetime.now(UTC)

        with patch.object(service, "get", AsyncMock(return_value=mock_schedule)):
            result = await service.update(
                schedule_id=schedule_id,
                name="updated-schedule",
                description="Updated description",
                cron_expression="0 */12 * * *",
                timezone_str="America/New_York",
            )

        assert result.name == "updated-schedule"
        assert result.description == "Updated description"
        assert result.cron_expression == "0 */12 * * *"
        assert result.timezone == "America/New_York"
        assert result.next_run_at is not None  # Recalculated
        mock_db.flush.assert_called_once()

    async def test_update_invalid_cron(self, service, mock_db):
        """Test update() raises ValidationError for invalid cron."""
        schedule_id = uuid4()
        mock_schedule = MagicMock(spec=Schedule)
        mock_schedule.cron_expression = "0 0 * * *"

        with patch.object(service, "get", AsyncMock(return_value=mock_schedule)):
            with pytest.raises(ValidationError) as exc_info:
                await service.update(
                    schedule_id=schedule_id,
                    cron_expression="invalid-cron",
                )

        assert "Invalid cron" in str(exc_info.value)

    async def test_delete_success(self, service, mock_db):
        """Test delete() removes schedule."""
        schedule_id = uuid4()
        mock_schedule = MagicMock(spec=Schedule)

        with patch.object(service, "get", AsyncMock(return_value=mock_schedule)):
            await service.delete(schedule_id)

        mock_db.delete.assert_called_once_with(mock_schedule)
        mock_db.flush.assert_called_once()

    async def test_get_due_schedules(self, service, mock_db):
        """Test get_due_schedules() returns schedules past their next_run_at."""
        mock_schedules = [MagicMock(spec=Schedule) for _ in range(3)]

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_schedules
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_due_schedules()

        assert result == mock_schedules

    async def test_mark_executed(self, service, mock_db):
        """Test mark_executed() updates last_run_at and next_run_at."""
        from datetime import datetime, UTC, timedelta

        schedule_id = uuid4()

        # Create a real Schedule object instead of a Mock
        real_schedule = Schedule(
            id=schedule_id,
            name="test-schedule",
            check_id=uuid4(),
            cron_expression="0 0 * * *",
            timezone="UTC",
            next_run_at=datetime.now(UTC) - timedelta(hours=1),
        )

        with patch.object(service, "get", AsyncMock(return_value=real_schedule)):
            result = await service.mark_executed(schedule_id)

        assert result.last_run_at is not None
        assert result.next_run_at is not None
        assert result.last_run_at > datetime.now(UTC) - timedelta(minutes=1)
        mock_db.flush.assert_called_once()

    def test_validate_cron_valid(self, service):
        """Test _validate_cron() returns True for valid expressions."""
        valid_expressions = [
            "0 0 * * *",      # Daily at midnight
            "0 */6 * * *",     # Every 6 hours
            "0 0 * * 0",      # Weekly on Sunday
            "0 0 1 * *",      # Monthly on 1st
            "*/5 * * * *",    # Every 5 minutes
        ]

        for expr in valid_expressions:
            assert service._validate_cron(expr) is True, f"Failed for: {expr}"

    def test_validate_cron_invalid(self, service):
        """Test _validate_cron() returns False for invalid expressions."""
        invalid_expressions = [
            "invalid",
            "",
            "* * * *",        # Too few fields
            "abc def ghi jkl mno",
        ]

        for expr in invalid_expressions:
            assert service._validate_cron(expr) is False, f"Failed for: {expr}"

    def test_calculate_next_run(self, service):
        """Test _calculate_next_run() returns future datetime."""
        now = datetime.now(UTC)

        next_run = service._calculate_next_run("0 0 * * *", "UTC")

        assert isinstance(next_run, datetime)
        assert next_run > now
        assert next_run.hour == 0
        assert next_run.minute == 0
