"""CheckService dry-run validation rejects configs that can't render valid SQL.

Complement to sensor-level required_params — this is the outer guard at the
service/API boundary. Together they ensure no caller (MLG, scripts, hand-rolled
clients) can persist a check that has no chance of ever executing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from dq_platform.api.errors import ValidationError
from dq_platform.models.check import CheckType
from dq_platform.services.check_service import CheckService


@pytest.fixture
def service() -> CheckService:
    db = MagicMock()
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.refresh = AsyncMock()
    return CheckService(db=db)


async def test_create_check_rejects_cross_table_without_reference_table(
    service: CheckService,
) -> None:
    """A total_sum_match_percent check without reference_table is unrunnable;
    the service must reject it with ValidationError (→ HTTP 422), not persist
    a ghost that fails forever at execution time.
    """
    with pytest.raises(ValidationError, match="reference_table"):
        await service.create_check(
            name="phantom-sum-match",
            connection_id=uuid4(),
            check_type=CheckType.TOTAL_SUM_MATCH_PERCENT,
            target_schema="public",
            target_table="orders",
            target_column="total_amount",
            parameters={},  # no reference_table — this is the bug we're blocking
        )


async def test_create_check_accepts_cross_table_with_reference_table(
    service: CheckService,
) -> None:
    """Happy path — same check with reference_table supplied should persist."""
    result = await service.create_check(
        name="sum-match-ok",
        connection_id=uuid4(),
        check_type=CheckType.TOTAL_SUM_MATCH_PERCENT,
        target_schema="public",
        target_table="orders",
        target_column="total_amount",
        parameters={"reference_table": "orders_backup"},
    )
    assert result is not None
    service.db.flush.assert_called_once()


async def test_create_check_allows_non_referential_checks_without_params(
    service: CheckService,
) -> None:
    """Validation must not regress ordinary checks — a nulls_percent check
    with empty parameters is perfectly fine.
    """
    result = await service.create_check(
        name="email-nulls",
        connection_id=uuid4(),
        check_type=CheckType.NULLS_PERCENT,
        target_schema="public",
        target_table="users",
        target_column="email",
    )
    assert result is not None


async def test_update_check_revalidates_sensor_template(
    service: CheckService,
) -> None:
    """Clearing a required parameter on an existing check via PATCH must
    be rejected — otherwise an update could leave a check in a state that
    create_check would have refused, re-opening the silent-SQL-error hole.
    """
    from unittest.mock import AsyncMock as _AsyncMock
    from unittest.mock import MagicMock as _MagicMock

    existing = _MagicMock()
    existing.check_type = CheckType.TOTAL_SUM_MATCH_PERCENT
    existing.target_schema = "public"
    existing.target_table = "orders"
    existing.target_column = "total_amount"
    existing.parameters = {"reference_table": "orders_backup"}

    # Simulate get_check() finding the row
    service.get_check = _AsyncMock(return_value=existing)  # type: ignore[method-assign]

    # Caller tries to clear parameters (dropping reference_table) → must fail.
    with pytest.raises(ValidationError, match="reference_table"):
        await service.update_check(check_id=uuid4(), parameters={})


async def test_update_check_preserves_valid_state(service: CheckService) -> None:
    """A legitimate update (e.g. renaming) shouldn't trip the validator."""
    from unittest.mock import AsyncMock as _AsyncMock
    from unittest.mock import MagicMock as _MagicMock

    existing = _MagicMock()
    existing.check_type = CheckType.NULLS_PERCENT
    existing.target_schema = "public"
    existing.target_table = "users"
    existing.target_column = "email"
    existing.parameters = {}
    service.get_check = _AsyncMock(return_value=existing)  # type: ignore[method-assign]

    result = await service.update_check(check_id=uuid4(), name="renamed")
    assert result is existing
    assert existing.name == "renamed"
