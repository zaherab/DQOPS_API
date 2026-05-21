"""Dimension scoring semantics: unexecuted checks are `not_assessed`, not `red`.

Before this fix, a dimension with registered-but-never-run checks scored 0.0 /
status=red, which the UI could not distinguish from genuine failure. The fix
returns None / not_assessed in that case, and exposes `not_run_count` so UIs
can label the state honestly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from dq_platform.checks.dqops_checks import DQOpsCheckType
from dq_platform.models.check import Check
from dq_platform.services.dimension_service import (
    DimensionService,
    _score_from_severity_counts,
)


def test_score_is_none_when_no_checks_have_results() -> None:
    """total == 0 → dimension is unassessed, not 0%."""
    assert _score_from_severity_counts(passed=0, warning=0, error=0, fatal=0) is None


def test_score_is_100_when_all_passed() -> None:
    assert _score_from_severity_counts(passed=5, warning=0, error=0, fatal=0) == 100.0


def test_score_reflects_partial_failures() -> None:
    score = _score_from_severity_counts(passed=4, warning=1, error=0, fatal=0)
    assert score is not None
    assert 0 < score < 100


def test_score_is_zero_when_all_fatal() -> None:
    assert _score_from_severity_counts(passed=0, warning=0, error=0, fatal=3) == 0.0


# ─── Service-level integration: DimensionScore shape ─────────────────────────


def _stub_check(check_type: DQOpsCheckType) -> MagicMock:
    m = MagicMock(spec=Check)
    m.id = uuid4()
    # DimensionService reads `.check_type.value` to resolve category.
    m.check_type = MagicMock()
    m.check_type.value = check_type.value
    m.is_active = True
    return m


async def test_dimension_with_registered_but_unrun_checks_is_not_assessed() -> None:
    """Regression for the phantom-failure bug: a dimension that has checks
    configured but no results yet must surface as not_assessed (score=None),
    not as a red 0%.
    """
    db = MagicMock()
    svc = DimensionService(db=db)

    # 2 conformity checks, neither executed yet. number_in_range_percent and
    # negative_values_percent are range/bound checks — conformity per the
    # ODPS-aligned mapping (accuracy = veracity vs source, not range bounds).
    checks = [
        _stub_check(DQOpsCheckType.NUMBER_IN_RANGE_PERCENT),
        _stub_check(DQOpsCheckType.NEGATIVE_VALUES_PERCENT),
    ]
    svc._get_checks = AsyncMock(return_value=checks)  # type: ignore[method-assign]
    svc._get_latest_results = AsyncMock(return_value=[])  # type: ignore[method-assign]

    resp = await svc.get_dimension_scores()
    conformity = next(d for d in resp.dimensions if d.dimension == "conformity")

    assert conformity.score is None, "Unexecuted checks must not score as 0%"
    assert conformity.status == "not_assessed"
    assert conformity.check_count == 2
    assert conformity.not_run_count == 2, (
        "not_run_count must expose the gap so the UI can render 'pending first run' distinctly from 'no checks'"
    )
    # And the overall score must not be dragged down by the unassessed dim.
    assert resp.overall_score is None


async def test_dimension_with_mixed_executed_and_unrun() -> None:
    """Partial execution: one ran, one didn't. not_run_count reflects only
    the second; the dimension is assessed on the first."""
    db = MagicMock()
    svc = DimensionService(db=db)

    ran = _stub_check(DQOpsCheckType.NOT_NULLS_PERCENT)
    pending = _stub_check(DQOpsCheckType.NOT_NULLS_PERCENT)

    svc._get_checks = AsyncMock(return_value=[ran, pending])  # type: ignore[method-assign]
    # Build a fake CheckResult for `ran`
    result = MagicMock()
    result.check_id = ran.id
    result.severity = MagicMock()
    result.severity.value = "passed"
    svc._get_latest_results = AsyncMock(return_value=[result])  # type: ignore[method-assign]

    resp = await svc.get_dimension_scores()
    comp = next(d for d in resp.dimensions if d.dimension == "completeness")

    assert comp.score == 100.0, "Score should reflect the executed check only"
    assert comp.check_count == 2
    assert comp.passed_count == 1
    assert comp.not_run_count == 1
