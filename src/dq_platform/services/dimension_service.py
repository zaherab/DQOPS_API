"""Dimension scoring service — computes ODPS dimension scores from check results."""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.checks.dqops_checks import CHECK_REGISTRY, DQOpsCheckType
from dq_platform.models.check import Check
from dq_platform.models.result import CheckResult
from dq_platform.odps.dimension_mapping import (
    ALL_DIMENSIONS,
    CATEGORY_TO_DIMENSION,
    FALLBACK_CATEGORY_MAP,
    SEVERITY_WEIGHTS,
    ODPSDimension,
)
from dq_platform.schemas.dimension import (
    DimensionCheckDetail,
    DimensionDailySnapshot,
    DimensionScore,
    DimensionScoreResponse,
    DimensionTrendResponse,
)


def _check_type_to_category(check_type_str: str) -> str | None:
    """Resolve a check_type string to its category."""
    try:
        ct = DQOpsCheckType(check_type_str)
        check_def = CHECK_REGISTRY.get(ct)
        if check_def:
            return check_def.category
    except ValueError:
        pass
    return FALLBACK_CATEGORY_MAP.get(check_type_str)


def _score_from_severity_counts(
    passed: int, warning: int, error: int, fatal: int,
) -> float:
    """Compute a 0-100 score from severity counts using weighted penalties."""
    total = passed + warning + error + fatal
    if total == 0:
        return 0.0

    penalties = (
        SEVERITY_WEIGHTS["passed"] * passed
        + SEVERITY_WEIGHTS["warning"] * warning
        + SEVERITY_WEIGHTS["error"] * error
        + SEVERITY_WEIGHTS["fatal"] * fatal
    )
    max_penalty = total * SEVERITY_WEIGHTS["fatal"]
    if max_penalty == 0:
        return 100.0

    return round(max(0.0, 100.0 * (1.0 - penalties / max_penalty)), 1)


def _status_from_score(score: float | None) -> str:
    """Map a numeric score to a traffic-light status string."""
    if score is None:
        return "not_assessed"
    if score >= 80:
        return "green"
    if score >= 60:
        return "amber"
    return "red"


class DimensionService:
    """Computes ODPS quality dimension scores from DQ Platform check results."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_dimension_scores(
        self,
        connection_id: uuid.UUID | None = None,
        check_ids: list[uuid.UUID] | None = None,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> DimensionScoreResponse:
        """Compute scores for all 8 ODPS dimensions.

        Filters checks by connection_id or explicit check_ids list.
        Uses only the *latest* result per check for scoring.
        """
        # 1. Get active checks matching filters
        checks = await self._get_checks(connection_id, check_ids)

        # 2. Map checks to dimensions
        dim_checks: dict[ODPSDimension, list[Check]] = defaultdict(list)
        for check in checks:
            cat = _check_type_to_category(check.check_type.value)
            dim = CATEGORY_TO_DIMENSION.get(cat) if cat else None
            if dim is not None:
                dim_checks[dim].append(check)

        # 3. Get latest result per check
        check_id_list = [c.id for c in checks]
        latest_results = await self._get_latest_results(
            check_id_list, from_date, to_date,
        )
        result_by_check: dict[uuid.UUID, CheckResult] = {
            r.check_id: r for r in latest_results
        }

        # 4. Score each dimension
        dimension_scores: list[DimensionScore] = []
        assessed_scores: list[float] = []

        for dim in ALL_DIMENSIONS:
            dim_check_list = dim_checks.get(dim, [])
            count = len(dim_check_list)

            if count == 0:
                dimension_scores.append(
                    DimensionScore(
                        dimension=dim.value,
                        score=None,
                        status="not_assessed",
                        check_count=0,
                        passed_count=0,
                        warning_count=0,
                        error_count=0,
                        fatal_count=0,
                    )
                )
                continue

            passed = warning = error = fatal = 0
            for check in dim_check_list:
                result = result_by_check.get(check.id)
                if result is None:
                    continue
                sev = result.severity
                sev_str = sev.value if hasattr(sev, "value") else str(sev)
                if sev_str == "passed":
                    passed += 1
                elif sev_str == "warning":
                    warning += 1
                elif sev_str == "error":
                    error += 1
                elif sev_str == "fatal":
                    fatal += 1

            score = _score_from_severity_counts(passed, warning, error, fatal)
            assessed_scores.append(score)

            dimension_scores.append(
                DimensionScore(
                    dimension=dim.value,
                    score=score,
                    status=_status_from_score(score),
                    check_count=count,
                    passed_count=passed,
                    warning_count=warning,
                    error_count=error,
                    fatal_count=fatal,
                )
            )

        overall = (
            round(sum(assessed_scores) / len(assessed_scores), 1)
            if assessed_scores
            else None
        )

        return DimensionScoreResponse(
            dimensions=dimension_scores,
            overall_score=overall,
            assessed_count=len(assessed_scores),
            total_dimensions=len(ALL_DIMENSIONS),
            computed_at=datetime.now(timezone.utc),
        )

    async def get_dimension_trend(
        self,
        dimension: str,
        connection_id: uuid.UUID | None = None,
        days: int = 30,
    ) -> DimensionTrendResponse:
        """Get daily score snapshots for a single dimension."""
        odps_dim = ODPSDimension(dimension)

        # Categories for this dimension
        categories = {
            cat for cat, d in CATEGORY_TO_DIMENSION.items() if d == odps_dim
        }

        # Checks in those categories for the connection
        checks = await self._get_checks(connection_id)
        dim_check_ids = [
            c.id
            for c in checks
            if _check_type_to_category(c.check_type.value) in categories
        ]

        if not dim_check_ids:
            return DimensionTrendResponse(dimension=dimension, snapshots=[])

        # Query results within date range
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        stmt = (
            select(CheckResult)
            .where(
                CheckResult.check_id.in_(dim_check_ids),
                CheckResult.executed_at >= cutoff,
            )
            .order_by(CheckResult.executed_at.asc())
        )
        result = await self.db.execute(stmt)
        results = list(result.scalars().all())

        # Group by date
        by_date: dict[str, list[CheckResult]] = defaultdict(list)
        for r in results:
            date_str = r.executed_at.strftime("%Y-%m-%d")
            by_date[date_str].append(r)

        snapshots: list[DimensionDailySnapshot] = []
        for date_str in sorted(by_date):
            day_results = by_date[date_str]
            passed = warning = error = fatal = 0
            for r in day_results:
                sev = r.severity
                sev_str = sev.value if hasattr(sev, "value") else str(sev)
                if sev_str == "passed":
                    passed += 1
                elif sev_str == "warning":
                    warning += 1
                elif sev_str == "error":
                    error += 1
                elif sev_str == "fatal":
                    fatal += 1
            score = _score_from_severity_counts(passed, warning, error, fatal)
            snapshots.append(DimensionDailySnapshot(date=date_str, score=score))

        return DimensionTrendResponse(dimension=dimension, snapshots=snapshots)

    async def get_dimension_checks(
        self,
        dimension: str,
        connection_id: uuid.UUID | None = None,
    ) -> list[DimensionCheckDetail]:
        """Get all checks contributing to a dimension with their latest result."""
        odps_dim = ODPSDimension(dimension)

        categories = {
            cat for cat, d in CATEGORY_TO_DIMENSION.items() if d == odps_dim
        }

        checks = await self._get_checks(connection_id)
        dim_checks = [
            c
            for c in checks
            if _check_type_to_category(c.check_type.value) in categories
        ]

        if not dim_checks:
            return []

        # Get latest result per check
        check_ids = [c.id for c in dim_checks]
        latest_results = await self._get_latest_results(check_ids)
        result_by_check = {r.check_id: r for r in latest_results}

        details: list[DimensionCheckDetail] = []
        for check in dim_checks:
            cat = _check_type_to_category(check.check_type.value) or ""
            result = result_by_check.get(check.id)
            sev_str = None
            if result and result.severity:
                sev_str = (
                    result.severity.value
                    if hasattr(result.severity, "value")
                    else str(result.severity)
                )
            details.append(
                DimensionCheckDetail(
                    check_id=check.id,
                    check_name=check.name,
                    check_type=check.check_type.value,
                    category=cat,
                    dimension=dimension,
                    target_table=check.target_table,
                    target_column=check.target_column,
                    latest_passed=result.passed if result else None,
                    latest_severity=sev_str,
                    latest_executed_at=(
                        result.executed_at if result else None
                    ),
                )
            )
        return details

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _get_checks(
        self,
        connection_id: uuid.UUID | None = None,
        check_ids: list[uuid.UUID] | None = None,
    ) -> list[Check]:
        """Load active checks filtered by connection or explicit IDs."""
        stmt = select(Check).where(Check.is_active == True)  # noqa: E712
        if connection_id:
            stmt = stmt.where(Check.connection_id == connection_id)
        if check_ids:
            stmt = stmt.where(Check.id.in_(check_ids))
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def _get_latest_results(
        self,
        check_ids: list[uuid.UUID],
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> list[CheckResult]:
        """Get the latest result per check using a subquery."""
        if not check_ids:
            return []

        # Subquery: max executed_at per check_id
        date_filters = []
        if from_date:
            date_filters.append(CheckResult.executed_at >= from_date)
        if to_date:
            date_filters.append(CheckResult.executed_at <= to_date)

        latest_sub = (
            select(
                CheckResult.check_id,
                func.max(CheckResult.executed_at).label("max_at"),
            )
            .where(CheckResult.check_id.in_(check_ids))
        )
        for f in date_filters:
            latest_sub = latest_sub.where(f)
        latest_sub = latest_sub.group_by(CheckResult.check_id).subquery()

        # Main query joining on latest
        stmt = select(CheckResult).join(
            latest_sub,
            (CheckResult.check_id == latest_sub.c.check_id)
            & (CheckResult.executed_at == latest_sub.c.max_at),
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())
