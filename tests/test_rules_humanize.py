"""Tests for human-readable rule message formatting."""

from dq_platform.checks.rules import (
    RuleResult,
    RuleType,
    Severity,
    _humanize_duration,
    _humanize_message,
    evaluate_rule,
)


class TestHumanizeDuration:
    """Test duration formatting helper."""

    def test_seconds(self) -> None:
        assert _humanize_duration(30) == "30 seconds"

    def test_minutes(self) -> None:
        assert _humanize_duration(300) == "5.0 minutes"

    def test_hours(self) -> None:
        assert _humanize_duration(7200) == "2.0 hours"

    def test_days(self) -> None:
        assert _humanize_duration(172800) == "2.0 days"

    def test_fractional_days(self) -> None:
        assert _humanize_duration(2385014) == "27.6 days"

    def test_zero(self) -> None:
        assert _humanize_duration(0) == "0 seconds"


def _make_result(
    actual: float | None,
    expected: str,
    passed: bool,
    message: str = "",
    severity: Severity = Severity.ERROR,
) -> RuleResult:
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=message or f"Value {actual}",
        expected=expected,
        actual=actual,
        passed=passed,
    )


class TestHumanizeTimeliness:
    def test_stale_days(self) -> None:
        r = _humanize_message(_make_result(2385014.0, "<= 86400", False), "timeliness")
        assert "27.6 days" in r.message
        assert "stale" in r.message
        assert "1.0 days" in r.message

    def test_fresh_minutes(self) -> None:
        r = _humanize_message(_make_result(300.0, "<= 86400", True), "timeliness")
        assert "5.0 minutes" in r.message
        assert "fresh" in r.message

    def test_null_unchanged(self) -> None:
        r = _make_result(None, "<= 86400", False, "Sensor returned NULL")
        assert _humanize_message(r, "timeliness").message == "Sensor returned NULL"


class TestHumanizeVolume:
    def test_row_count_pass(self) -> None:
        r = _humanize_message(_make_result(1234, ">= 1000", True), "volume")
        assert "1,234" in r.message
        assert "rows" in r.message

    def test_row_count_fail(self) -> None:
        r = _humanize_message(_make_result(5, ">= 1000", False), "volume")
        assert "5 rows" in r.message


class TestHumanizeNulls:
    def test_null_percent(self) -> None:
        r = _humanize_message(_make_result(3.2, "<= 1%", False), "nulls")
        assert "3.20% null values" in r.message
        assert "<= 1%" in r.message


class TestHumanizeUniqueness:
    def test_duplicate_percent(self) -> None:
        r = _humanize_message(_make_result(2.1, "<= 0%", False), "uniqueness")
        assert "2.10% duplicates" in r.message

    def test_duplicate_count(self) -> None:
        r = _humanize_message(_make_result(150, "<= 0", False), "uniqueness")
        assert "150 duplicates" in r.message


class TestHumanizeNumeric:
    def test_in_range(self) -> None:
        r = _humanize_message(_make_result(42.5, "[10, 100]", True), "numeric")
        assert "within" in r.message

    def test_out_of_range(self) -> None:
        r = _humanize_message(_make_result(150, "[10, 100]", False), "numeric")
        assert "outside" in r.message

    def test_statistical_same_as_numeric(self) -> None:
        r = _humanize_message(_make_result(5.5, "[1, 10]", True), "statistical")
        assert "within" in r.message


class TestHumanizeText:
    def test_percent_check(self) -> None:
        r = _humanize_message(_make_result(85.2, ">= 95%", False), "text")
        assert "85.20%" in r.message
        assert "failed" in r.message

    def test_length_check(self) -> None:
        r = _humanize_message(_make_result(5, ">= 3", True), "text")
        assert "within" in r.message


class TestHumanizePatterns:
    def test_percent_pass(self) -> None:
        r = _humanize_message(_make_result(99.5, ">= 95%", True), "patterns")
        assert "99.50%" in r.message
        assert "match" in r.message

    def test_count_fail(self) -> None:
        r = _humanize_message(_make_result(12, "<= 0", False), "patterns")
        assert "12" in r.message
        assert "fail" in r.message

    def test_pii_same_as_patterns(self) -> None:
        r = _humanize_message(_make_result(3.5, "<= 0%", False), "pii")
        assert "3.50%" in r.message
        assert "fail" in r.message


class TestHumanizeBoolean:
    def test_true_percent(self) -> None:
        r = _humanize_message(_make_result(95.0, ">= 90% true", True), "boolean")
        assert "95.00%" in r.message
        assert "true" in r.message


class TestHumanizeDatetime:
    def test_date_validation(self) -> None:
        r = _humanize_message(_make_result(2.5, "<= 0%", False), "datetime")
        assert "2.50%" in r.message
        assert "failed" in r.message


class TestHumanizeGeographic:
    def test_valid_percent(self) -> None:
        r = _humanize_message(_make_result(98.0, ">= 95%", True), "geographic")
        assert "98.00%" in r.message
        assert "valid" in r.message

    def test_invalid_count(self) -> None:
        r = _humanize_message(_make_result(5, "<= 0", False), "geographic")
        assert "5" in r.message
        assert "invalid" in r.message


class TestHumanizeDatatype:
    def test_match(self) -> None:
        r = _humanize_message(_make_result(1, "1", True), "datatype")
        assert "matches" in r.message

    def test_changed(self) -> None:
        r = _humanize_message(_make_result(2, "1", False), "datatype")
        assert "changed" in r.message


class TestHumanizeChange:
    def test_change_within(self) -> None:
        r = _humanize_message(_make_result(5.2, "<= 10%", True), "change")
        assert "5.20% change" in r.message

    def test_change_detection_alias(self) -> None:
        r = _humanize_message(_make_result(15.0, "<= 10%", False), "change_detection")
        assert "15.00% change" in r.message


class TestHumanizeAnomaly:
    def test_keeps_iqr_message(self) -> None:
        r = _make_result(42.0, "[10, 50]", True, "Value 42 is within IQR bounds [10.00, 50.00]")
        h = _humanize_message(r, "anomaly")
        assert "IQR" in h.message  # Preserved


class TestHumanizeComparison:
    def test_match_percent(self) -> None:
        r = _humanize_message(_make_result(99.5, ">= 95%", True), "comparison")
        assert "99.50% match rate" in r.message

    def test_value_differs(self) -> None:
        r = _humanize_message(_make_result(42, "50", False), "comparison")
        assert "differs" in r.message


class TestHumanizeReferential:
    def test_fk_percent(self) -> None:
        r = _humanize_message(_make_result(97.0, ">= 95%", True), "referential")
        assert "97.00%" in r.message
        assert "foreign keys" in r.message

    def test_orphan_count(self) -> None:
        r = _humanize_message(_make_result(15, "<= 0", False), "referential")
        assert "15" in r.message
        assert "orphaned" in r.message


class TestHumanizeAcceptedValues:
    def test_percent_in_set(self) -> None:
        r = _humanize_message(_make_result(92.0, ">= 95%", False), "accepted_values")
        assert "92.00%" in r.message
        assert "accepted" in r.message

    def test_count_in_use(self) -> None:
        r = _humanize_message(_make_result(5, ">= 3", True), "accepted_values")
        assert "5" in r.message
        assert "in use" in r.message


class TestHumanizeSchema:
    def test_passed(self) -> None:
        r = _humanize_message(_make_result(5, ">= 5", True), "schema")
        assert "passed" in r.message

    def test_changed(self) -> None:
        r = _make_result(3, "5", False, "Column count changed from 5 to 3")
        h = _humanize_message(r, "schema")
        assert "Schema changed" in h.message


class TestHumanizeAvailability:
    def test_available(self) -> None:
        r = _humanize_message(_make_result(1, "True/1", True), "availability")
        assert r.message == "Table is available"

    def test_unavailable(self) -> None:
        r = _humanize_message(_make_result(0, "True/1", False), "availability")
        assert r.message == "Table is unavailable or unreachable"


class TestHumanizeCustomSql:
    def test_keeps_original(self) -> None:
        r = _make_result(3, "<= 0", False, "Value 3 is > 0")
        h = _humanize_message(r, "custom_sql")
        assert h.message == "Value 3 is > 0"


class TestDynamicFallback:
    """Test the description-based fallback for unknown categories."""

    def test_with_description(self) -> None:
        r = _make_result(42, ">= 10", True, "Value 42 is >= 10")
        h = _humanize_message(r, "some_future_category", "Check that data meets SLA threshold")
        assert "data meets sla threshold" in h.message.lower()
        assert "passed" in h.message

    def test_strips_check_that_prefix(self) -> None:
        r = _make_result(5, ">= 3", True)
        h = _humanize_message(r, "unknown", "Check that minimum word count is met")
        assert h.message.startswith("Minimum word count is met")

    def test_without_description_keeps_original(self) -> None:
        r = _make_result(42, ">= 10", True, "Value 42 is >= 10")
        h = _humanize_message(r, "unknown_no_desc")
        assert h.message == "Value 42 is >= 10"


class TestEvaluateRuleIntegration:
    """Test evaluate_rule with category and description threading."""

    def test_timeliness_humanized(self) -> None:
        r = evaluate_rule(
            RuleType.MAX_VALUE,
            2385014.0,
            {"max_value": 86400, "severity": "error"},
            category="timeliness",
        )
        assert "days" in r.message
        assert "stale" in r.message
        assert r.passed is False

    def test_without_category_generic(self) -> None:
        r = evaluate_rule(
            RuleType.MAX_VALUE,
            100.0,
            {"max_value": 86400, "severity": "error"},
        )
        assert "Value 100.0" in r.message

    def test_with_description_fallback(self) -> None:
        r = evaluate_rule(
            RuleType.MIN_VALUE,
            42.0,
            {"min_value": 10, "severity": "error"},
            category="brand_new_category",
            description="Check that custom metric exceeds threshold",
        )
        assert "Custom metric exceeds threshold" in r.message
        assert "passed" in r.message

    def test_preserves_severity_and_passed(self) -> None:
        r = evaluate_rule(
            RuleType.MAX_PERCENT,
            15.0,
            {"max_percent": 5, "severity": "fatal"},
            category="nulls",
        )
        assert r.severity == Severity.FATAL
        assert r.passed is False
        assert "15.00% null values" in r.message
