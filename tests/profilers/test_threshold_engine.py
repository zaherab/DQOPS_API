"""Tests for promise→rule_parameters projection.

Two layers:
1. Direct unit assertions on parse helpers + edge cases.
2. Shared fixture suite driven from tests/fixtures/threshold_cases.json so
   the Python port and the TS source-of-truth stay in lockstep.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dq_platform.profilers.threshold_engine import (
    parse_promise_hours,
    parse_promise_percent,
    promise_for_dimension,
    promised_dimensions_from_profile,
    thresholds_from_promise,
)

# ─── parse_promise_percent ───────────────────────────────────────────────────


class TestParsePromisePercent:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("95", 95.0),
            ("95%", 95.0),
            ("95.5", 95.5),
            ("0", 0.0),
            ("100", 100.0),
            (" 95 ", 95.0),
        ],
    )
    def test_valid(self, value: str, expected: float) -> None:
        assert parse_promise_percent(value) == expected

    @pytest.mark.parametrize(
        "value",
        [None, "", "-5%", "150", "abc", "95.5.5", "9 5"],
    )
    def test_invalid(self, value: str | None) -> None:
        assert parse_promise_percent(value) is None


# ─── parse_promise_hours ─────────────────────────────────────────────────────


class TestParsePromiseHours:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("24h", 24.0),
            ("1hr", 1.0),
            ("2hours", 2.0),
            ("60m", 1.0),
            ("5min", 5 / 60.0),
            ("30s", 30 / 3600.0),
            ("1d", 24.0),
            ("2days", 48.0),
            ("12", 12.0),  # bare = hours
        ],
    )
    def test_valid(self, value: str, expected: float) -> None:
        got = parse_promise_hours(value)
        assert got is not None
        assert abs(got - expected) < 1e-9

    @pytest.mark.parametrize(
        "value",
        [None, "", "abc"],
    )
    def test_invalid(self, value: str | None) -> None:
        assert parse_promise_hours(value) is None


# ─── Shared fixture suite ────────────────────────────────────────────────────


_FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "threshold_cases.json"


def _load_fixtures() -> list[dict]:
    with _FIXTURE_PATH.open() as f:
        data = json.load(f)
    return data["cases"]


@pytest.mark.parametrize("case", _load_fixtures(), ids=lambda c: c["name"])
def test_fixture_case(case: dict) -> None:
    """Each fixture must produce the expected rule_parameters payload.

    Same fixture file is consumed by the TS test in MLG-Spec-Reader to
    prevent drift between implementations during the AI-removal transition.
    """
    got = thresholds_from_promise(case["rule_type"], case["promise"])
    expected = case["expected"]
    if expected is None:
        assert got is None, f"{case['name']}: expected None, got {got}"
    else:
        assert got is not None, f"{case['name']}: expected {expected}, got None"
        # Compare with float tolerance — JSON serialization can introduce
        # tiny representation differences across runtimes.
        _assert_rule_params_equal(got, expected, case["name"])


def _assert_rule_params_equal(
    got: dict[str, dict[str, float]],
    expected: dict[str, dict[str, float]],
    name: str,
) -> None:
    assert set(got.keys()) == set(expected.keys()), (
        f"{name}: severity keys mismatch — got {sorted(got)}, expected {sorted(expected)}"
    )
    for sev in expected:
        assert set(got[sev].keys()) == set(expected[sev].keys()), f"{name}.{sev}: param keys mismatch"
        for k, v in expected[sev].items():
            assert abs(got[sev][k] - v) < 1e-6, f"{name}.{sev}.{k}: expected {v}, got {got[sev][k]}"


# ─── Promise extraction from profile ─────────────────────────────────────────


class TestPromisedDimensionsFromProfile:
    def test_returns_only_promised(self) -> None:
        profile = {
            "completeness": "99",
            "uniqueness": "100",
            "validity": None,
            "conformity": "",
            "accuracy": "95",
        }
        got = promised_dimensions_from_profile(profile)
        assert sorted(got) == ["accuracy", "completeness", "uniqueness"]

    def test_empty_profile(self) -> None:
        assert promised_dimensions_from_profile({}) == []

    def test_none_profile(self) -> None:
        assert promised_dimensions_from_profile(None) == []

    def test_whitespace_promise_treated_as_unpromised(self) -> None:
        assert promised_dimensions_from_profile({"completeness": "   "}) == []


class TestPromiseForDimension:
    def test_returns_value(self) -> None:
        assert promise_for_dimension({"completeness": "99"}, "completeness") == "99"

    def test_returns_none_for_blank(self) -> None:
        assert promise_for_dimension({"completeness": ""}, "completeness") is None

    def test_returns_none_for_missing(self) -> None:
        assert promise_for_dimension({}, "completeness") is None

    def test_handles_none_profile(self) -> None:
        assert promise_for_dimension(None, "completeness") is None
