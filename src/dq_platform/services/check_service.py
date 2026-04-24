"""Check service for managing data quality checks."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.checks import Severity
from dq_platform.checks.check_runner import run_check
from dq_platform.checks.dqops_checks import (
    DQOpsCheckType,
)
from dq_platform.checks.dqops_checks import (
    get_check as get_dqops_check_def,
)
from dq_platform.checks.gx_registry import is_column_level_check
from dq_platform.checks.sensors import get_sensor
from dq_platform.models.check import Check, CheckMode, CheckTimeScale, CheckType
from dq_platform.services.connection_service import ConnectionService


def _dry_run_render(
    check_type: CheckType,
    target_schema: str | None,
    target_table: str,
    target_column: str | None,
    parameters: dict[str, Any] | None,
) -> None:
    """Validate a check config can produce valid SQL before we persist it.

    Attempts to render the sensor's Jinja template with the merged params.
    Surfaces missing required_params (e.g. `reference_table` on cross-table
    sensors) as a ValidationError so the API returns 422 instead of saving
    a check that will fail on every execution.

    GX-only check types (not in the DQOps registry) are skipped — they don't
    have a sensor template to render.
    """
    try:
        dqops_check = get_dqops_check_def(DQOpsCheckType(check_type.value))
    except (ValueError, KeyError):
        return  # GX fallback; no sensor to validate

    sensor = get_sensor(dqops_check.sensor_type)

    # Merge params: sensor defaults → check-def defaults → user-supplied.
    # User-supplied wins so callers can override defaults.
    merged: dict[str, Any] = {}
    if dqops_check.default_params:
        merged.update(dqops_check.default_params)
    if parameters:
        merged.update(parameters)
    # Identifier slots used by the templates.
    merged["schema_name"] = target_schema or "public"
    merged["table_name"] = target_table
    if target_column is not None:
        merged["column_name"] = target_column

    try:
        sensor.render(merged)
    except ValueError as e:
        raise ValidationError(str(e)) from e


@dataclass
class PreviewResult:
    """Result of a check preview."""

    check_type: str
    check_name: str
    severity: Severity
    passed: bool
    sensor_value: float | None
    expected: Any
    actual: Any
    message: str
    executed_sql: str | None
    executed_at: datetime


class CheckService:
    """Service for managing data quality checks."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_check(
        self,
        name: str,
        connection_id: UUID,
        check_type: CheckType,
        target_table: str,
        target_schema: str | None = None,
        target_column: str | None = None,
        description: str | None = None,
        parameters: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        check_mode: CheckMode = CheckMode.MONITORING,
        time_scale: CheckTimeScale | None = None,
        partition_by_column: str | None = None,
        rule_parameters: dict[str, Any] | None = None,
    ) -> Check:
        """Create a new data quality check.

        Args:
            name: Check name.
            connection_id: Connection UUID.
            check_type: Type of check.
            target_table: Target table name.
            target_schema: Target schema name.
            target_column: Target column name (for column-level checks).
            description: Optional description.
            parameters: Check parameters.
            metadata: Optional metadata.
            check_mode: Check mode (profiling, monitoring, partitioned).
            time_scale: Time scale for monitoring checks (daily, monthly).
            partition_by_column: Column to partition by (for partitioned checks).
            rule_parameters: Rule parameters with severity thresholds.

        Returns:
            Created check.

        Raises:
            ValidationError: If validation fails.
        """
        # Validate column is provided for column-level checks
        is_column_level = False
        try:
            dqops_check = get_dqops_check_def(DQOpsCheckType(check_type.value))
            is_column_level = dqops_check.is_column_level
        except (ValueError, KeyError):
            # Not a DQOps check type, try GX registry
            try:
                is_column_level = is_column_level_check(check_type)
            except ValueError:
                # Unknown check type, skip validation
                pass

        if is_column_level and not target_column:
            raise ValidationError(f"Column-level check '{check_type.value}' requires target_column")

        # Validate partition_by_column for partitioned mode
        if check_mode == CheckMode.PARTITIONED and not partition_by_column:
            raise ValidationError("Partitioned checks require partition_by_column")

        # Dry-run render the sensor template so we reject configs that can't
        # produce valid SQL (e.g. cross-table sensors missing reference_table).
        _dry_run_render(check_type, target_schema, target_table, target_column, parameters)

        check = Check(
            name=name,
            description=description,
            connection_id=connection_id,
            check_type=check_type,
            check_mode=check_mode,
            time_scale=time_scale,
            target_schema=target_schema,
            target_table=target_table,
            target_column=target_column,
            partition_by_column=partition_by_column,
            parameters=parameters or {},
            rule_parameters=rule_parameters,
            metadata_=metadata,
        )

        self.db.add(check)
        # Flush (not commit) so the request-level session owner (`get_db`)
        # controls the transaction boundary. Matches ConnectionService /
        # ExecutionService / ScheduleService.
        await self.db.flush()
        return check

    async def get_check(self, check_id: UUID, *, include_inactive: bool = False) -> Check | None:
        """Get a check by ID.

        Args:
            check_id: Check UUID.
            include_inactive: If True, return check even if is_active=False.

        Returns:
            Check instance or None if not found.
        """
        query = select(Check).where(Check.id == check_id)
        if not include_inactive:
            query = query.where(Check.is_active == True)  # noqa: E712
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_checks(
        self,
        connection_id: UUID | None = None,
        check_type: CheckType | None = None,
        check_mode: CheckMode | None = None,
        target_table: str | None = None,
        is_active: bool | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[Check], int]:
        """List checks with optional filtering.

        Args:
            connection_id: Filter by connection.
            check_type: Filter by check type.
            check_mode: Filter by check mode.
            target_table: Filter by target table.
            is_active: Filter by active status.
            offset: Number of records to skip.
            limit: Maximum number of records to return.

        Returns:
            Tuple of (checks, total_count).
        """
        query = select(Check)

        if connection_id:
            query = query.where(Check.connection_id == connection_id)
        if check_type:
            query = query.where(Check.check_type == check_type)
        if check_mode:
            query = query.where(Check.check_mode == check_mode)
        if target_table:
            query = query.where(Check.target_table == target_table)
        if is_active is not None:
            query = query.where(Check.is_active == is_active)

        # Get total count
        count_query = select(func.count(Check.id))
        if connection_id:
            count_query = count_query.where(Check.connection_id == connection_id)
        if check_type:
            count_query = count_query.where(Check.check_type == check_type)
        if check_mode:
            count_query = count_query.where(Check.check_mode == check_mode)
        if target_table:
            count_query = count_query.where(Check.target_table == target_table)
        if is_active is not None:
            count_query = count_query.where(Check.is_active == is_active)

        count_result = await self.db.execute(count_query)
        total = count_result.scalar() or 0

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(Check.created_at.desc())
        result = await self.db.execute(query)
        checks = list(result.scalars().all())

        return checks, total

    async def update_check(
        self,
        check_id: UUID,
        name: str | None = None,
        description: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
        target_column: str | None = None,
        parameters: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        is_active: bool | None = None,
        partition_by_column: str | None = None,
        rule_parameters: dict[str, Any] | None = None,
    ) -> Check | None:
        """Update a check.

        Args:
            check_id: Check UUID.
            name: New name.
            description: New description.
            target_schema: New target schema.
            target_table: New target table.
            target_column: New target column.
            parameters: New parameters.
            metadata: New metadata.
            is_active: New active status.
            partition_by_column: New partition column.
            rule_parameters: New rule parameters.

        Returns:
            Updated check or None if not found.
        """
        check = await self.get_check(check_id, include_inactive=True)
        if not check:
            return None

        if name is not None:
            check.name = name
        if description is not None:
            check.description = description
        if target_schema is not None:
            check.target_schema = target_schema
        if target_table is not None:
            check.target_table = target_table
        if target_column is not None:
            check.target_column = target_column
        if parameters is not None:
            check.parameters = parameters
        if metadata is not None:
            check.metadata_ = metadata
        if is_active is not None:
            check.is_active = is_active
        if partition_by_column is not None:
            check.partition_by_column = partition_by_column
        if rule_parameters is not None:
            check.rule_parameters = rule_parameters

        # Revalidate the sensor template with the updated state. Guards
        # against clearing a required param (e.g. reference_table) via PATCH.
        _dry_run_render(
            check.check_type,
            check.target_schema,
            check.target_table,
            check.target_column,
            check.parameters,
        )

        await self.db.flush()
        return check

    async def delete_check(self, check_id: UUID) -> bool:
        """Delete a check (soft delete).

        Args:
            check_id: Check UUID.

        Returns:
            True if deleted, False if not found.
        """
        check = await self.get_check(check_id)
        if not check:
            return False

        check.is_active = False
        await self.db.flush()
        return True

    async def preview_check(self, check_id: UUID) -> PreviewResult:
        """Preview a check execution without saving results.

        Args:
            check_id: Check UUID.

        Returns:
            Preview result.

        Raises:
            NotFoundError: If check not found.
        """
        check = await self.get_check(check_id)
        if not check:
            raise NotFoundError("Check", str(check_id))

        # Get connection config
        conn_service = ConnectionService(self.db)
        connection = await conn_service.get_connection(check.connection_id)
        if not connection:
            raise NotFoundError("Connection", str(check.connection_id))

        # Execute check
        return await self._execute_check_internal(check, connection.decrypted_config)

    async def preview_check_config(
        self,
        connection_id: UUID,
        check_type: CheckType,
        target_table: str,
        target_schema: str | None = None,
        target_column: str | None = None,
        parameters: dict[str, Any] | None = None,
        rule_parameters: dict[str, Any] | None = None,
    ) -> PreviewResult:
        """Preview a check configuration without saving.

        Args:
            connection_id: Connection UUID.
            check_type: Check type.
            target_table: Target table.
            target_schema: Target schema.
            target_column: Target column.
            parameters: Check parameters.
            rule_parameters: Rule parameters.

        Returns:
            Preview result.

        Raises:
            NotFoundError: If connection not found.
        """
        # Get connection config
        conn_service = ConnectionService(self.db)
        connection = await conn_service.get_connection(connection_id)
        if not connection:
            raise NotFoundError("Connection", str(connection_id))

        # Create temporary check object
        check = Check(
            id=UUID("00000000-0000-0000-0000-000000000000"),
            name="preview",
            connection_id=connection_id,
            check_type=check_type,
            target_schema=target_schema,
            target_table=target_table,
            target_column=target_column,
            parameters=parameters or {},
            rule_parameters=rule_parameters,
        )

        return await self._execute_check_internal(check, connection.decrypted_config)

    async def _execute_check_internal(
        self,
        check: Check,
        connection_config: dict[str, Any],
    ) -> PreviewResult:
        """Internal method to execute a check.

        Delegates to the shared check_runner for actual execution logic.
        """
        try:
            result = await run_check(check, connection_config, db=self.db)
            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=Severity(result.severity),
                passed=result.passed,
                sensor_value=result.sensor_value,
                expected=result.expected,
                actual=result.actual,
                message=result.message,
                executed_sql=result.executed_sql,
                executed_at=result.executed_at,
            )
        except Exception as e:
            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=Severity.ERROR,
                passed=False,
                sensor_value=None,
                expected=check.parameters,
                actual=None,
                message=f"Execution failed: {e!s}",
                executed_sql=None,
                executed_at=datetime.now(UTC),
            )

    async def execute_check(
        self,
        check: Check,
        connection_config: dict[str, Any],
    ) -> PreviewResult:
        """Execute a check and return result.

        Args:
            check: Check to execute.
            connection_config: Connection configuration.

        Returns:
            Execution result.
        """
        return await self._execute_check_internal(check, connection_config)
