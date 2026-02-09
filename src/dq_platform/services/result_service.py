"""Result service - querying and aggregating check results."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.models.result import CheckResult, ResultSeverity


class ResultService:
    """Service for querying check execution results."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_result(
        self,
        check_id: uuid.UUID,
        job_id: uuid.UUID,
        status: str,
        severity: str,
        result_value: float | None = None,
        expected: Any = None,
        actual: Any = None,
        message: str | None = None,
        executed_sql: str | None = None,
        execution_time_ms: int | None = None,
        rows_scanned: int | None = None,
    ) -> CheckResult:
        """Create a new check result.

        Args:
            check_id: Check UUID.
            job_id: Job UUID.
            status: Status string ("passed", "failed").
            severity: Severity level ("passed", "warning", "error", "fatal").
            result_value: The sensor value from execution.
            expected: Expected value from rule.
            actual: Actual value from sensor.
            message: Result message.
            executed_sql: Executed SQL for DQOps checks.
            execution_time_ms: Execution time in milliseconds.
            rows_scanned: Number of rows scanned.

        Returns:
            Created check result.
        """
        from dq_platform.models.check import Check

        # Get check for denormalized fields
        check_result = await self.db.execute(select(Check).where(Check.id == check_id))
        check = check_result.scalar_one()

        result = CheckResult(
            check_id=check_id,
            job_id=job_id,
            connection_id=check.connection_id,
            target_table=check.target_table,
            target_column=check.target_column,
            check_type=check.check_type.value,
            actual_value=result_value,
            expected_value=float(expected) if isinstance(expected, (int, float)) else None,
            passed=(status == "passed"),
            severity=ResultSeverity(severity),
            execution_time_ms=execution_time_ms,
            rows_scanned=rows_scanned,
            result_details={
                "message": message,
                "expected": expected,
                "actual": actual,
            },
            executed_sql=executed_sql,
        )

        self.db.add(result)
        await self.db.flush()
        return result

    async def create(
        self,
        check_id: uuid.UUID,
        job_id: uuid.UUID,
        connection_id: uuid.UUID,
        target_table: str,
        check_type: str,
        executed_at: datetime,
        passed: bool,
        target_column: str | None = None,
        actual_value: float | None = None,
        expected_value: float | None = None,
        execution_time_ms: int | None = None,
        rows_scanned: int | None = None,
        result_details: dict[str, Any] | None = None,
        error_message: str | None = None,
        severity: ResultSeverity = ResultSeverity.PASSED,
    ) -> CheckResult:
        """Create a new check result (legacy method).

        Args:
            check_id: Check UUID.
            job_id: Job UUID.
            connection_id: Connection UUID.
            target_table: Target table name.
            check_type: Check type string.
            executed_at: Execution timestamp.
            passed: Whether check passed.
            target_column: Optional target column.
            actual_value: Actual value from sensor.
            expected_value: Expected value from rule.
            execution_time_ms: Execution time in milliseconds.
            rows_scanned: Number of rows scanned.
            result_details: Additional result details.
            error_message: Error message if check failed to execute.
            severity: Result severity level.

        Returns:
            Created check result.
        """
        result = CheckResult(
            check_id=check_id,
            job_id=job_id,
            connection_id=connection_id,
            target_table=target_table,
            target_column=target_column,
            check_type=check_type,
            actual_value=actual_value,
            expected_value=expected_value,
            passed=passed,
            severity=severity if not passed else ResultSeverity.PASSED,
            execution_time_ms=execution_time_ms,
            rows_scanned=rows_scanned,
            result_details=result_details or {},
            error_message=error_message,
        )

        self.db.add(result)
        await self.db.flush()
        return result

    async def query(
        self,
        offset: int = 0,
        limit: int = 100,
        check_id: uuid.UUID | None = None,
        connection_id: uuid.UUID | None = None,
        passed: bool | None = None,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> tuple[list[CheckResult], int]:
        """Query check results with filters.

        Args:
            offset: Number of records to skip.
            limit: Maximum number of records to return.
            check_id: Optional filter by check.
            connection_id: Optional filter by connection.
            passed: Optional filter by pass/fail status.
            from_date: Optional start date filter.
            to_date: Optional end date filter.

        Returns:
            Tuple of (results, total_count).
        """
        query = select(CheckResult)

        if check_id:
            query = query.where(CheckResult.check_id == check_id)
        if connection_id:
            query = query.where(CheckResult.connection_id == connection_id)
        if passed is not None:
            query = query.where(CheckResult.passed == passed)
        if from_date:
            query = query.where(CheckResult.executed_at >= from_date)
        if to_date:
            query = query.where(CheckResult.executed_at <= to_date)

        # Get total count
        count_query = select(func.count(CheckResult.id))
        if check_id:
            count_query = count_query.where(CheckResult.check_id == check_id)
        if connection_id:
            count_query = count_query.where(CheckResult.connection_id == connection_id)
        if passed is not None:
            count_query = count_query.where(CheckResult.passed == passed)
        if from_date:
            count_query = count_query.where(CheckResult.executed_at >= from_date)
        if to_date:
            count_query = count_query.where(CheckResult.executed_at <= to_date)

        count_result = await self.db.execute(count_query)
        total = count_result.scalar() or 0

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(CheckResult.executed_at.desc())
        result = await self.db.execute(query)
        results = list(result.scalars().all())

        return results, total

    async def get_summary(
        self,
        check_id: uuid.UUID | None = None,
        connection_id: uuid.UUID | None = None,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Get summary statistics for check results.

        Args:
            check_id: Optional filter by check.
            connection_id: Optional filter by connection.
            from_date: Optional start date filter.
            to_date: Optional end date filter.

        Returns:
            Summary statistics.
        """
        # Build base query
        filters = []
        if check_id:
            filters.append(CheckResult.check_id == check_id)
        if connection_id:
            filters.append(CheckResult.connection_id == connection_id)
        if from_date:
            filters.append(CheckResult.executed_at >= from_date)
        if to_date:
            filters.append(CheckResult.executed_at <= to_date)

        # Total count
        total_query = select(func.count(CheckResult.id))
        for f in filters:
            total_query = total_query.where(f)
        total_result = await self.db.execute(total_query)
        total = total_result.scalar() or 0

        # Passed count
        passed_query = select(func.count(CheckResult.id)).where(CheckResult.passed == True)  # noqa: E712
        for f in filters:
            passed_query = passed_query.where(f)
        passed_result = await self.db.execute(passed_query)
        passed = passed_result.scalar() or 0

        # Failed count
        failed = total - passed

        # By severity
        severity_query = select(CheckResult.severity, func.count(CheckResult.id)).group_by(CheckResult.severity)
        for f in filters:
            severity_query = severity_query.where(f)
        severity_result = await self.db.execute(severity_query)
        by_severity = {row[0]: row[1] for row in severity_result.all()}

        # Average execution time
        avg_time_query = select(func.avg(CheckResult.execution_time_ms))
        for f in filters:
            avg_time_query = avg_time_query.where(f)
        avg_time_result = await self.db.execute(avg_time_query)
        avg_execution_time = avg_time_result.scalar()

        return {
            "total_executions": total,
            "passed": passed,
            "failed": failed,
            "by_severity": by_severity,
            "pass_rate": (passed / total * 100) if total > 0 else 0,
            "avg_execution_time_ms": round(avg_execution_time, 2) if avg_execution_time else None,
        }
