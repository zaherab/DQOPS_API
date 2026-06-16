"""Regression tests for multi-tier severity evaluation (evaluate_tiers).

Before the fix, check_runner flattened a check's rule_parameters to a single
tier (most-severe present) and judged only that one, so the warning band was
dead whenever an error band was also present — e.g. a data_freshness check with
warning=24h and error=36h would report PASSED at 33h instead of WARNING.
"""

from dq_platform.checks.rules import RuleType, Severity, evaluate_tiers


class TestTwoTierMaxPercent:
    tiers = {"warning": {"max_percent": 1.0}, "error": {"max_percent": 2.0}}

    def test_within_both_bands_passes(self) -> None:
        r = evaluate_tiers(RuleType.MAX_PERCENT, 0.5, {}, self.tiers)
        assert r.passed is True
        assert r.severity == Severity.PASSED

    def test_breaching_only_warning_reports_warning(self) -> None:
        # 1.5% > warning(1%) but <= error(2%): the previously-dead warning band.
        r = evaluate_tiers(RuleType.MAX_PERCENT, 1.5, {}, self.tiers)
        assert r.passed is False
        assert r.severity == Severity.WARNING
        assert r.expected == "<= 1.0%"

    def test_breaching_error_reports_error(self) -> None:
        r = evaluate_tiers(RuleType.MAX_PERCENT, 3.0, {}, self.tiers)
        assert r.passed is False
        assert r.severity == Severity.ERROR
        assert r.expected == "<= 2.0%"


class TestFreshnessRegression:
    """The exact bug: 33.4h stale under a 24h warning / 36h error promise."""

    def test_freshness_warns_between_bands(self) -> None:
        tiers = {"warning": {"max_value": 86400.0}, "error": {"max_value": 129600.0}}
        r = evaluate_tiers(RuleType.MAX_VALUE, 120262.0, {}, tiers)
        assert r.passed is False
        assert r.severity == Severity.WARNING


class TestSeverityPrecedence:
    def test_fatal_takes_precedence_over_error_and_warning(self) -> None:
        tiers = {
            "warning": {"max_percent": 1.0},
            "error": {"max_percent": 2.0},
            "fatal": {"max_percent": 3.0},
        }
        r = evaluate_tiers(RuleType.MAX_PERCENT, 5.0, {}, tiers)
        assert r.severity == Severity.FATAL


class TestBackwardCompatibility:
    def test_error_only_check_unchanged(self) -> None:
        tiers = {"error": {"max_percent": 2.0}}
        assert evaluate_tiers(RuleType.MAX_PERCENT, 3.0, {}, tiers).severity == Severity.ERROR
        assert evaluate_tiers(RuleType.MAX_PERCENT, 1.0, {}, tiers).passed is True

    def test_no_tiers_runs_single_default_eval(self) -> None:
        # Sensor-baseline checks carry no tiers; threshold lives in base params,
        # severity defaults to ERROR on breach.
        r = evaluate_tiers(RuleType.MAX_PERCENT, 5.0, {"max_percent": 2.0}, {})
        assert r.passed is False
        assert r.severity == Severity.ERROR
        passing = evaluate_tiers(RuleType.MAX_PERCENT, 1.0, {"max_percent": 2.0}, {})
        assert passing.passed is True

    def test_base_params_merged_into_every_tier(self) -> None:
        # A base param (e.g. an injected value) must be visible to each tier's
        # evaluation, not dropped. min_percent rule reads min_percent from the
        # tier while base carries unrelated keys harmlessly.
        tiers = {"warning": {"min_percent": 100.0}, "error": {"min_percent": 99.0}}
        r = evaluate_tiers(RuleType.MIN_PERCENT, 99.84, {"expected_values": ["X"]}, tiers)
        assert r.passed is False
        assert r.severity == Severity.WARNING
