"""check_runner must propagate real execution errors instead of masking them.

Before this fix, the DQOps path's try/except covered the entire execution
flow and caught any ValueError/KeyError from sensor render or SQL
execution — then silently fell through to the GX executor, which threw
"GX executor not implemented" and polluted logs/UIs with a misleading
error. Narrowing the try to just the enum lookup means the real cause
now lands in the job's error_message field.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dq_platform.checks import check_runner
from dq_platform.checks.check_runner import run_check
from dq_platform.models.check import Check, CheckType


def _stub_check(check_type_value: str) -> MagicMock:
    c = MagicMock(spec=Check)
    c.check_type = MagicMock()
    c.check_type.value = check_type_value
    c.target_schema = "public"
    c.target_table = "orders"
    c.target_column = "total_amount"
    c.parameters = {}
    c.rule_parameters = None
    return c


async def test_sensor_render_errors_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    """If run_dqops_check raises a ValueError (e.g. missing required_params
    from the sensor's template), the caller must see that exact error —
    NOT a misleading 'GX executor not implemented'.
    """

    async def _explode(**_: Any) -> None:
        raise ValueError("Sensor 'SUM_MATCH_PERCENT' requires non-empty parameter 'reference_table'")

    monkeypatch.setattr(check_runner, "run_dqops_check", _explode)

    # run_gx_check must NOT be reached. If it were, the test would silently
    # pass with the wrong error — fail loud by asserting it's never called.
    gx_calls = {"n": 0}

    async def _gx_fail(**_: Any) -> None:
        gx_calls["n"] += 1
        raise AssertionError("GX fallback must not fire for known DQOps check types")

    monkeypatch.setattr(check_runner, "run_gx_check", _gx_fail)

    check = _stub_check(CheckType.TOTAL_SUM_MATCH_PERCENT.value)
    with pytest.raises(ValueError, match="reference_table"):
        await run_check(check, connection_config={"type": "postgresql"})
    assert gx_calls["n"] == 0


async def test_unknown_check_type_falls_back_to_gx(monkeypatch: pytest.MonkeyPatch) -> None:
    """GX fallback is still the right path for check types that aren't in
    the DQOps registry at all. Narrowing the try/except shouldn't break
    this legitimate case."""
    # Pick a CheckType that exists in the enum but IS NOT a DQOpsCheckType.
    # `ALLOWED_VALUES` is a classic GX-only check on the platform.
    calls = {"gx": 0}

    async def _fake_gx(**_: Any) -> dict[str, Any]:
        calls["gx"] += 1
        return {"success": True, "observed_value": 42, "result": {"comment": "ok"}}

    # Sanity — run_dqops_check must NOT be called for a GX-only type.
    async def _dqops_should_not_run(**_: Any) -> None:
        raise AssertionError("run_dqops_check must not be called for GX-only types")

    monkeypatch.setattr(check_runner, "run_gx_check", _fake_gx)
    monkeypatch.setattr(check_runner, "run_dqops_check", _dqops_should_not_run)

    check = _stub_check(CheckType.ALLOWED_VALUES.value)
    result = await run_check(check, connection_config={"type": "postgresql"})
    assert calls["gx"] == 1
    assert result.passed is True
