"""Schedule service - managing cron schedules for checks."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from croniter import croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.check import Check
from dq_platform.models.schedule import Schedule


class ScheduleService:
    """Service for managing check schedules."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(
        self,
        name: str,
        check_id: uuid.UUID,
        cron_expression: str,
        timezone_str: str = "UTC",
        description: str | None = None,
    ) -> Schedule:
        """Create a new schedule.

        Args:
            name: Schedule name.
            check_id: Check UUID to schedule.
            cron_expression: Cron expression (e.g., "0 */6 * * *").
            timezone_str: Timezone for cron evaluation.
            description: Optional description.

        Returns:
            Created schedule.

        Raises:
            ValidationError: If cron expression is invalid.
            NotFoundError: If check not found.
        """
        # Validate cron expression
        if not self._validate_cron(cron_expression):
            raise ValidationError(f"Invalid cron expression: {cron_expression}")

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

        # Calculate next run time
        next_run = self._calculate_next_run(cron_expression, timezone_str)

        schedule = Schedule(
            name=name,
            description=description,
            check_id=check_id,
            cron_expression=cron_expression,
            timezone=timezone_str,
            next_run_at=next_run,
        )

        self.db.add(schedule)
        await self.db.flush()
        return schedule

    async def get(self, schedule_id: uuid.UUID) -> Schedule:
        """Get a schedule by ID.

        Args:
            schedule_id: Schedule UUID.

        Returns:
            Schedule instance.

        Raises:
            NotFoundError: If schedule not found.
        """
        result = await self.db.execute(select(Schedule).where(Schedule.id == schedule_id))
        schedule = result.scalar_one_or_none()

        if not schedule:
            raise NotFoundError("Schedule", str(schedule_id))

        return schedule

    async def list_schedules(
        self,
        offset: int = 0,
        limit: int = 100,
        check_id: uuid.UUID | None = None,
        is_active: bool | None = None,
    ) -> tuple[list[Schedule], int]:
        """List schedules with pagination and filters.

        Args:
            offset: Number of records to skip.
            limit: Maximum number of records to return.
            check_id: Optional filter by check.
            is_active: Optional filter by active status.

        Returns:
            Tuple of (schedules, total_count).
        """
        query = select(Schedule)

        if check_id:
            query = query.where(Schedule.check_id == check_id)
        if is_active is not None:
            query = query.where(Schedule.is_active == is_active)

        # Get total count
        count_query = select(Schedule.id)
        if check_id:
            count_query = count_query.where(Schedule.check_id == check_id)
        if is_active is not None:
            count_query = count_query.where(Schedule.is_active == is_active)

        count_result = await self.db.execute(count_query)
        total = len(count_result.all())

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(Schedule.created_at.desc())
        result = await self.db.execute(query)
        schedules = list(result.scalars().all())

        return schedules, total

    async def update(
        self,
        schedule_id: uuid.UUID,
        name: str | None = None,
        description: str | None = None,
        cron_expression: str | None = None,
        timezone_str: str | None = None,
        is_active: bool | None = None,
    ) -> Schedule:
        """Update a schedule.

        Args:
            schedule_id: Schedule UUID.
            name: Optional new name.
            description: Optional new description.
            cron_expression: Optional new cron expression.
            timezone_str: Optional new timezone.
            is_active: Optional new active status.

        Returns:
            Updated schedule.
        """
        schedule = await self.get(schedule_id)

        if name is not None:
            schedule.name = name
        if description is not None:
            schedule.description = description
        if cron_expression is not None:
            if not self._validate_cron(cron_expression):
                raise ValidationError(f"Invalid cron expression: {cron_expression}")
            schedule.cron_expression = cron_expression
        if timezone_str is not None:
            schedule.timezone = timezone_str
        if is_active is not None:
            schedule.is_active = is_active

        # Recalculate next run time
        schedule.next_run_at = self._calculate_next_run(schedule.cron_expression, schedule.timezone)

        await self.db.flush()
        return schedule

    async def delete(self, schedule_id: uuid.UUID) -> None:
        """Delete a schedule.

        Args:
            schedule_id: Schedule UUID.
        """
        schedule = await self.get(schedule_id)
        await self.db.delete(schedule)
        await self.db.flush()

    async def get_due_schedules(self) -> list[Schedule]:
        """Get schedules that are due for execution.

        Returns:
            List of schedules with next_run_at <= now.
        """
        now = datetime.now(UTC)

        result = await self.db.execute(
            select(Schedule).where(
                Schedule.is_active == True,  # noqa: E712
                Schedule.next_run_at <= now,
            )
        )
        return list(result.scalars().all())

    async def mark_executed(self, schedule_id: uuid.UUID) -> Schedule:
        """Mark a schedule as executed and update next run time.

        Args:
            schedule_id: Schedule UUID.

        Returns:
            Updated schedule.
        """
        schedule = await self.get(schedule_id)

        schedule.last_run_at = datetime.now(UTC)
        schedule.next_run_at = self._calculate_next_run(schedule.cron_expression, schedule.timezone)

        await self.db.flush()
        return schedule

    def _validate_cron(self, expression: str) -> bool:
        """Validate a cron expression.

        Args:
            expression: Cron expression to validate.

        Returns:
            True if valid, False otherwise.
        """
        try:
            croniter(expression)
            return True
        except (ValueError, KeyError):
            return False

    def _calculate_next_run(self, expression: str, tz: str) -> datetime:
        """Calculate the next run time for a cron expression.

        Args:
            expression: Cron expression.
            tz: Timezone string.

        Returns:
            Next run time as UTC datetime.
        """
        # For simplicity, calculate based on UTC
        # In production, you'd want proper timezone handling
        cron = croniter(expression, datetime.now(UTC))
        next_run: datetime = cron.get_next(datetime)
        return next_run
