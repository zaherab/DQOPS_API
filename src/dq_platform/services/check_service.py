"""Check service for managing data quality checks."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.checks import (
    Severity,
    run_dqops_check,
)
from dq_platform.checks.dqops_checks import (
    DQOpsCheckType,
    get_check as get_dqops_check_def,
)
from dq_platform.checks.dqops_executor import CheckExecutionResult, DQOpsExecutor
from dq_platform.checks.gx_executor import run_gx_check
from dq_platform.checks.gx_registry import is_column_level_check
from dq_platform.checks.rules import RuleType, evaluate_rule
from dq_platform.checks.sensors import get_sensor
from dq_platform.models.check import Check, CheckMode, CheckTimeScale, CheckType
from dq_platform.models.result import CheckResult
from dq_platform.services.connection_service import ConnectionService


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
            raise ValidationError(
                f"Column-level check '{check_type.value}' requires target_column"
            )

        # Validate partition_by_column for partitioned mode
        if check_mode == CheckMode.PARTITIONED and not partition_by_column:
            raise ValidationError(
                "Partitioned checks require partition_by_column"
            )

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
        await self.db.commit()
        return check

    async def get_check(self, check_id: UUID) -> Check | None:
        """Get a check by ID.

        Args:
            check_id: Check UUID.

        Returns:
            Check instance or None if not found.
        """
        result = await self.db.execute(
            select(Check).where(
                Check.id == check_id,
                Check.is_active == True,  # noqa: E712
            )
        )
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
        count_query = select(Check.id)
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
        total = len(count_result.all())

        # Get paginated results
        query = (
            query.offset(offset)
            .limit(limit)
            .order_by(Check.created_at.desc())
        )
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
        check = await self.get_check(check_id)
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

        Args:
            check: Check to execute.
            connection_config: Connection configuration.

        Returns:
            Execution result.
        """
        executed_at = datetime.now(timezone.utc)

        # Try DQOps-style execution first
        try:
            dqops_check_type = DQOpsCheckType(check.check_type.value)
            dqops_check_def = get_dqops_check_def(dqops_check_type)

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

            # Anomaly: inject historical values into rule_params
            if dqops_check_def.rule_type == RuleType.ANOMALY_PERCENTILE:
                rule_params["_historical_values"] = await self._get_historical_values(check)

            # Cross-source: dual-connection execution (early return)
            if "reference_connection_id" in (check.parameters or {}):
                return await self._execute_cross_source_check(
                    check, connection_config, dqops_check_def, rule_params
                )

            # Execute DQOps check
            result = await run_dqops_check(
                check_type=dqops_check_type,
                connection_config=connection_config,
                schema_name=check.target_schema or "public",
                table_name=check.target_table,
                column_name=check.target_column,
                rule_params=rule_params,
            )

            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=result.severity,
                passed=result.passed,
                sensor_value=result.sensor_value,
                expected=result.expected,
                actual=result.actual,
                message=result.message,
                executed_sql=result.executed_sql,
                executed_at=executed_at,
            )

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

            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=severity,
                passed=gx_result["success"],
                sensor_value=gx_result.get("observed_value"),
                expected=check.parameters,
                actual=gx_result.get("observed_value"),
                message=gx_result.get("result", {}).get("comment", "Check executed"),
                executed_sql=None,
                executed_at=executed_at,
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
                message=f"Execution failed: {str(e)}",
                executed_sql=None,
                executed_at=executed_at,
            )

    async def _get_historical_values(self, check: Check, days: int = 90) -> list[float]:
        """Get historical sensor values for anomaly detection.

        Args:
            check: Check to get history for.
            days: Number of days of history to retrieve.

        Returns:
            List of historical actual_value floats.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        result = await self.db.execute(
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

    async def _execute_cross_source_check(
        self,
        check: Check,
        connection_config: dict[str, Any],
        dqops_check_def: Any,
        rule_params: dict[str, Any],
    ) -> PreviewResult:
        """Execute a cross-source comparison check.

        Runs the same sensor on two different connections and compares results.

        Args:
            check: Check to execute.
            connection_config: Source connection configuration.
            dqops_check_def: DQOps check definition.
            rule_params: Rule parameters.

        Returns:
            Preview result with comparison outcome.
        """
        executed_at = datetime.now(timezone.utc)
        params = check.parameters or {}

        # Get reference connection
        ref_conn_id = params.get("reference_connection_id")
        if not ref_conn_id:
            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=Severity.ERROR,
                passed=False,
                sensor_value=None,
                expected="reference_connection_id in parameters",
                actual=None,
                message="Cross-source check requires reference_connection_id parameter",
                executed_sql=None,
                executed_at=executed_at,
            )

        conn_service = ConnectionService(self.db)
        ref_connection = await conn_service.get_connection(UUID(ref_conn_id))
        if not ref_connection:
            return PreviewResult(
                check_type=check.check_type.value,
                check_name=check.name,
                severity=Severity.ERROR,
                passed=False,
                sensor_value=None,
                expected="valid reference connection",
                actual=None,
                message=f"Reference connection {ref_conn_id} not found",
                executed_sql=None,
                executed_at=executed_at,
            )

        ref_config = ref_connection.decrypted_config

        # Get sensor and render SQL for both connections
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

        # Execute on both connections
        executor = DQOpsExecutor()
        source_value = await executor._execute_sensor_sql(connection_config, source_sql)
        ref_value = await executor._execute_sensor_sql(ref_config, ref_sql)

        # Compute match percent
        if source_value is None or ref_value is None:
            match_percent = None
        elif source_value == 0 and ref_value == 0:
            match_percent = 100.0
        elif max(abs(source_value), abs(ref_value)) == 0:
            match_percent = 0.0
        else:
            match_percent = (
                min(abs(source_value), abs(ref_value))
                / max(abs(source_value), abs(ref_value))
                * 100.0
            )

        # Evaluate rule with match_percent as the sensor value
        rule_result = evaluate_rule(dqops_check_def.rule_type, match_percent, rule_params)

        combined_sql = f"-- Source:\n{source_sql}\n-- Reference:\n{ref_sql}"
        return PreviewResult(
            check_type=check.check_type.value,
            check_name=check.name,
            severity=rule_result.severity,
            passed=rule_result.passed,
            sensor_value=match_percent,
            expected=rule_result.expected,
            actual=rule_result.actual,
            message=f"Source={source_value}, Reference={ref_value}. {rule_result.message}",
            executed_sql=combined_sql,
            executed_at=executed_at,
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
