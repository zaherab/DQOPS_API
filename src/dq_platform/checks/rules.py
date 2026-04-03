"""Rule evaluation functions for DQOps-style checks.

Rules are Python functions that evaluate sensor output against thresholds.
They determine the severity (passed, warning, error, fatal) of a check result.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """Check result severity levels."""

    PASSED = "passed"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


class RuleType(str, Enum):
    """Types of rules for evaluating sensor results."""

    # Threshold rules
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    MIN_MAX_VALUE = "min_max_value"

    # Percentage rules
    MIN_PERCENT = "min_percent"
    MAX_PERCENT = "max_percent"
    MIN_MAX_PERCENT = "min_max_percent"

    # Change detection rules
    MAX_CHANGE_PERCENT = "max_change_percent"

    # Count rules
    MIN_COUNT = "min_count"
    MAX_COUNT = "max_count"
    MIN_MAX_COUNT = "min_max_count"

    # Comparison rules
    EQUAL_TO = "equal_to"
    NOT_EQUAL_TO = "not_equal_to"

    # Boolean rules
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"

    # Anomaly detection rules
    ANOMALY_PERCENTILE = "anomaly_percentile"


@dataclass
class RuleResult:
    """Result of rule evaluation."""

    severity: Severity
    message: str
    expected: Any
    actual: Any
    passed: bool


# Type alias for rule functions
RuleFunction = Callable[[float | None, dict[str, Any]], RuleResult]


# =============================================================================
# Threshold Rules
# =============================================================================


def _min_value_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must be >= min_value.

    Parameters:
        min_value: Minimum acceptable value
        severity: Severity if rule fails (default: ERROR)
    """
    min_val = params.get("min_value")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or min_val is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected >= {min_val}",
            expected=f">= {min_val}",
            actual=None,
            passed=False,
        )

    passed = sensor_value >= float(min_val)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'>=' if passed else '<'} {min_val}",
        expected=f">= {min_val}",
        actual=sensor_value,
        passed=passed,
    )


def _max_value_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must be <= max_value.

    Parameters:
        max_value: Maximum acceptable value
        severity: Severity if rule fails (default: ERROR)
    """
    max_val = params.get("max_value")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or max_val is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected <= {max_val}",
            expected=f"<= {max_val}",
            actual=None,
            passed=False,
        )

    passed = sensor_value <= float(max_val)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'<=' if passed else '>'} {max_val}",
        expected=f"<= {max_val}",
        actual=sensor_value,
        passed=passed,
    )


def _min_max_value_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must be within [min_value, max_value] range.

    Parameters:
        min_value: Minimum acceptable value (optional)
        max_value: Maximum acceptable value (optional)
        severity: Severity if rule fails (default: ERROR)
    """
    min_val = params.get("min_value")
    max_val = params.get("max_value")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected in range [{min_val}, {max_val}]",
            expected=f"[{min_val}, {max_val}]",
            actual=None,
            passed=False,
        )

    passed = True
    if min_val is not None and sensor_value < min_val:
        passed = False
    if max_val is not None and sensor_value > max_val:
        passed = False

    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'within' if passed else 'outside'} range [{min_val}, {max_val}]",
        expected=f"[{min_val}, {max_val}]",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Percentage Rules
# =============================================================================


def _min_percent_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor percentage must be >= min_percent.

    Parameters:
        min_percent: Minimum acceptable percentage
        severity: Severity if rule fails (default: ERROR)
    """
    min_pct = params.get("min_percent")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or min_pct is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected >= {min_pct}%",
            expected=f">= {min_pct}%",
            actual=None,
            passed=False,
        )

    passed = sensor_value >= float(min_pct)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Percentage {sensor_value:.2f}% is {'>=' if passed else '<'} {min_pct}%",
        expected=f">= {min_pct}%",
        actual=sensor_value,
        passed=passed,
    )


def _max_percent_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor percentage must be <= max_percent.

    Parameters:
        max_percent: Maximum acceptable percentage
        severity: Severity if rule fails (default: ERROR)
    """
    max_pct = params.get("max_percent")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or max_pct is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected <= {max_pct}%",
            expected=f"<= {max_pct}%",
            actual=None,
            passed=False,
        )

    passed = sensor_value <= float(max_pct)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Percentage {sensor_value:.2f}% is {'<=' if passed else '>'} {max_pct}%",
        expected=f"<= {max_pct}%",
        actual=sensor_value,
        passed=passed,
    )


def _min_max_percent_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor percentage must be within [min_percent, max_percent] range.

    Parameters:
        min_percent: Minimum acceptable percentage (optional)
        max_percent: Maximum acceptable percentage (optional)
        severity: Severity if rule fails (default: ERROR)
    """
    min_pct = params.get("min_percent")
    max_pct = params.get("max_percent")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected in range [{min_pct}%, {max_pct}%]",
            expected=f"[{min_pct}%, {max_pct}%]",
            actual=None,
            passed=False,
        )

    passed = True
    if min_pct is not None and sensor_value < min_pct:
        passed = False
    if max_pct is not None and sensor_value > max_pct:
        passed = False

    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Percentage {sensor_value:.2f}% is {'within' if passed else 'outside'} range [{min_pct}%, {max_pct}%]",
        expected=f"[{min_pct}%, {max_pct}%]",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Change Detection Rules
# =============================================================================


def _max_change_percent_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: percentage change must be <= max_change_percent.

    Parameters:
        max_change_percent: Maximum acceptable percentage change
        severity: Severity if rule fails (default: ERROR)
    """
    max_change = params.get("max_change_percent")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or max_change is None:
        return RuleResult(
            severity=severity,
            message="Could not calculate change (insufficient historical data)",
            expected=f"<= {max_change}% change",
            actual=None,
            passed=False,
        )

    passed = abs(sensor_value) <= float(max_change)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Change of {sensor_value:.2f}% is {'within' if passed else 'exceeds'} limit of {max_change}%",
        expected=f"<= ±{max_change}%",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Count Rules
# =============================================================================


def _min_count_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor count must be >= min_count.

    Parameters:
        min_count: Minimum acceptable count
        severity: Severity if rule fails (default: ERROR)
    """
    min_cnt = params.get("min_count")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or min_cnt is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected >= {min_cnt}",
            expected=f">= {min_cnt}",
            actual=None,
            passed=False,
        )

    passed = sensor_value >= float(min_cnt)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Count {int(sensor_value)} is {'>=' if passed else '<'} {min_cnt}",
        expected=f">= {min_cnt}",
        actual=int(sensor_value),
        passed=passed,
    )


def _max_count_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor count must be <= max_count.

    Parameters:
        max_count: Maximum acceptable count
        severity: Severity if rule fails (default: ERROR)
    """
    max_cnt = params.get("max_count")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None or max_cnt is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected <= {max_cnt}",
            expected=f"<= {max_cnt}",
            actual=None,
            passed=False,
        )

    passed = sensor_value <= float(max_cnt)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Count {int(sensor_value)} is {'<=' if passed else '>'} {max_cnt}",
        expected=f"<= {max_cnt}",
        actual=int(sensor_value),
        passed=passed,
    )


def _min_max_count_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor count must be within [min_count, max_count] range.

    Parameters:
        min_count: Minimum acceptable count (optional)
        max_count: Maximum acceptable count (optional)
        severity: Severity if rule fails (default: ERROR)
    """
    min_cnt = params.get("min_count")
    max_cnt = params.get("max_count")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected in range [{min_cnt}, {max_cnt}]",
            expected=f"[{min_cnt}, {max_cnt}]",
            actual=None,
            passed=False,
        )

    passed = True
    if min_cnt is not None and sensor_value < min_cnt:
        passed = False
    if max_cnt is not None and sensor_value > max_cnt:
        passed = False

    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Count {int(sensor_value)} is {'within' if passed else 'outside'} range [{min_cnt}, {max_cnt}]",
        expected=f"[{min_cnt}, {max_cnt}]",
        actual=int(sensor_value),
        passed=passed,
    )


# =============================================================================
# Comparison Rules
# =============================================================================


def _equal_to_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must equal expected value.

    Parameters:
        expected_value: Expected value
        severity: Severity if rule fails (default: ERROR)
    """
    expected = params.get("expected_value")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message=f"Sensor returned NULL, expected {expected}",
            expected=str(expected),
            actual=None,
            passed=False,
        )

    passed = sensor_value == expected
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'equal to' if passed else 'not equal to'} {expected}",
        expected=str(expected),
        actual=sensor_value,
        passed=passed,
    )


def _not_equal_to_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must not equal forbidden value.

    Parameters:
        forbidden_value: Forbidden value
        severity: Severity if rule fails (default: ERROR)
    """
    forbidden = params.get("forbidden_value")
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=Severity.PASSED,
            message="Sensor returned NULL",
            expected=f"!= {forbidden}",
            actual=None,
            passed=True,
        )

    passed = sensor_value != forbidden
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'not equal to' if passed else 'equal to'} {forbidden}",
        expected=f"!= {forbidden}",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Boolean Rules
# =============================================================================


def _is_true_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must be truthy (1, True, non-zero).

    Parameters:
        severity: Severity if rule fails (default: ERROR)
    """
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message="Sensor returned NULL, expected True/1",
            expected="True/1",
            actual=None,
            passed=False,
        )

    passed = bool(sensor_value)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'truthy' if passed else 'falsy'}",
        expected="True/1",
        actual=sensor_value,
        passed=passed,
    )


def _is_false_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: sensor value must be falsy (0, False).

    Parameters:
        severity: Severity if rule fails (default: ERROR)
    """
    severity = Severity(params.get("severity", Severity.ERROR.value))

    if sensor_value is None:
        return RuleResult(
            severity=Severity.PASSED,
            message="Sensor returned NULL (treated as falsy)",
            expected="False/0",
            actual=None,
            passed=True,
        )

    passed = not bool(sensor_value)
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=f"Value {sensor_value} is {'falsy' if passed else 'truthy'}",
        expected="False/0",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Anomaly Detection Rules
# =============================================================================


def _anomaly_percentile_rule(sensor_value: float | None, params: dict[str, Any]) -> RuleResult:
    """Rule: detect anomalies using IQR (Interquartile Range) method.

    Requires historical values injected via params["_historical_values"].
    If fewer than 7 valid historical values exist, returns PASSED (insufficient history).

    Parameters:
        _historical_values: List of historical sensor values (injected by service layer)
        anomaly_percent: Forward-compatibility param (unused, IQR uses fixed 1.5 multiplier)
        severity: Severity if anomaly detected (default: ERROR)
    """
    severity = Severity(params.get("severity", Severity.ERROR.value))
    historical = params.get("_historical_values", [])

    # Filter out None values
    valid_history = sorted([v for v in historical if v is not None])

    if len(valid_history) < 7:
        return RuleResult(
            severity=Severity.PASSED,
            message="Insufficient history for anomaly detection (need >= 7 data points)",
            expected="within IQR bounds",
            actual=sensor_value,
            passed=True,
        )

    if sensor_value is None:
        return RuleResult(
            severity=severity,
            message="Sensor returned NULL, cannot assess anomaly",
            expected="within IQR bounds",
            actual=None,
            passed=False,
        )

    # Compute IQR
    n = len(valid_history)
    q1 = valid_history[n // 4]
    q3 = valid_history[(3 * n) // 4]
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    passed = lower_bound <= sensor_value <= upper_bound
    return RuleResult(
        severity=Severity.PASSED if passed else severity,
        message=(
            f"Value {sensor_value} is {'within' if passed else 'outside'} "
            f"IQR bounds [{lower_bound:.2f}, {upper_bound:.2f}]"
        ),
        expected=f"[{lower_bound:.2f}, {upper_bound:.2f}]",
        actual=sensor_value,
        passed=passed,
    )


# =============================================================================
# Human-readable message formatting
# =============================================================================


def _humanize_duration(seconds: float) -> str:
    """Convert seconds to human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    if seconds < 3600:
        return f"{seconds / 60:.1f} minutes"
    if seconds < 86400:
        return f"{seconds / 3600:.1f} hours"
    return f"{seconds / 86400:.1f} days"


def _parse_threshold(expected: str) -> float | None:
    """Extract numeric threshold from expected strings like '>= 86400' or '<= 5%'."""
    import re

    m = re.search(r"[\d.]+", str(expected))
    return float(m.group()) if m else None


def _fmt_number(value: float) -> str:
    """Format a number: integers get commas, floats get 2 decimal places."""
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def _fmt_percent(value: float) -> str:
    """Format a percentage value."""
    return f"{value:.2f}%"


def _humanize_message(
    result: RuleResult,
    category: str,
    description: str | None = None,
) -> RuleResult:
    """Rewrite rule message to be human-readable based on check category and description.

    Uses category for known transformations, falls back to description-based
    formatting for unknown categories. This ensures any new check type
    automatically gets a readable message.
    """
    actual = result.actual
    if actual is None:
        return result  # Keep NULL messages as-is

    threshold = _parse_threshold(str(result.expected))
    status = "passed" if result.passed else "failed"

    # ── Timeliness: seconds → human duration ─────────────────────────────
    if category == "timeliness":
        human_actual = _humanize_duration(float(actual))
        human_threshold = _humanize_duration(threshold) if threshold else str(result.expected)
        label = "fresh" if result.passed else "stale"
        result.message = f"Data is {human_actual} {label} (threshold: {human_threshold})"

    # ── Volume: row/record counts with commas ────────────────────────────
    elif category == "volume":
        result.message = f"{_fmt_number(float(actual))} rows (expected: {result.expected})"

    # ── Nulls/Completeness: null percentage ──────────────────────────────
    elif category == "nulls":
        result.message = f"{_fmt_percent(float(actual))} null values (limit: {result.expected})"

    # ── Uniqueness: duplicate percentage/count ───────────────────────────
    elif category == "uniqueness":
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} duplicates (limit: {result.expected})"
        else:
            result.message = f"{_fmt_number(float(actual))} duplicates (limit: {result.expected})"

    # ── Numeric/Statistical: value ranges ────────────────────────────────
    elif category in ("numeric", "statistical"):
        result.message = f"Value {_fmt_number(float(actual))} is {'within' if result.passed else 'outside'} expected range ({result.expected})"

    # ── Text: length and content checks ──────────────────────────────────
    elif category == "text":
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} of text values {status} check (target: {result.expected})"
        else:
            result.message = f"Text length {_fmt_number(float(actual))} is {'within' if result.passed else 'outside'} limit ({result.expected})"

    # ── Patterns/Format: email, UUID, IP, phone, etc. ────────────────────
    elif category in ("patterns", "pii"):
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} of values {'match' if result.passed else 'fail'} format check (target: {result.expected})"
        else:
            result.message = f"{_fmt_number(float(actual))} values {'match' if result.passed else 'fail'} format check (limit: {result.expected})"

    # ── Boolean: true/false percentages ──────────────────────────────────
    elif category == "boolean":
        result.message = f"{_fmt_percent(float(actual))} of values are {'true' if 'true' in str(result.expected).lower() else 'false'} (target: {result.expected})"

    # ── DateTime: date range and future dates ────────────────────────────
    elif category == "datetime":
        result.message = f"{_fmt_percent(float(actual))} of dates {status} validation (target: {result.expected})"

    # ── Geographic: lat/lon validation ───────────────────────────────────
    elif category == "geographic":
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} of coordinates are valid (target: {result.expected})"
        else:
            result.message = f"{_fmt_number(float(actual))} invalid coordinates found (limit: {result.expected})"

    # ── Datatype detection ───────────────────────────────────────────────
    elif category == "datatype":
        result.message = (
            f"Detected datatype {'matches' if result.passed else 'changed from'} expected ({result.expected})"
        )

    # ── Change detection: percentage changes ─────────────────────────────
    elif category in ("change", "change_detection"):
        result.message = f"{_fmt_percent(float(actual))} change detected (limit: {result.expected})"

    # ── Anomaly detection: keep IQR message (already readable) ───────────
    elif category == "anomaly":
        pass  # IQR bounds message from rule is already clear

    # ── Comparison: cross-source match checks ────────────────────────────
    elif category == "comparison":
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} match rate across sources (target: {result.expected})"
        else:
            result.message = f"Value {_fmt_number(float(actual))} {'matches' if result.passed else 'differs from'} reference ({result.expected})"

    # ── Referential integrity ────────────────────────────────────────────
    elif category == "referential":
        if "%" in str(result.expected):
            result.message = (
                f"{_fmt_percent(float(actual))} of foreign keys found in reference (target: {result.expected})"
            )
        else:
            result.message = f"{_fmt_number(float(actual))} orphaned foreign keys (limit: {result.expected})"

    # ── Accepted values / domain checks ──────────────────────────────────
    elif category == "accepted_values":
        if "%" in str(result.expected):
            result.message = f"{_fmt_percent(float(actual))} of values in accepted set (target: {result.expected})"
        else:
            result.message = f"{_fmt_number(float(actual))} accepted values in use (expected: {result.expected})"

    # ── Schema: structure checks ─────────────────────────────────────────
    elif category == "schema":
        if result.passed:
            result.message = f"Schema check passed ({_fmt_number(float(actual))} matches expected)"
        else:
            result.message = f"Schema changed — {result.message}"

    # ── Availability ─────────────────────────────────────────────────────
    elif category == "availability":
        result.message = "Table is available" if result.passed else "Table is unavailable or unreachable"

    # ── Custom SQL: keep original (we can't know what it measures) ───────
    elif category == "custom_sql":
        pass  # Keep the generic message

    # ── Dynamic fallback: use check description ──────────────────────────
    else:
        if description:
            # Strip "Check that " prefix from description for cleaner message
            desc = description
            if desc.lower().startswith("check that "):
                desc = desc[11:]
            if desc.lower().startswith("check "):
                desc = desc[6:]
            qualifier = "passed" if result.passed else "failed"
            result.message = (
                f"{desc.capitalize()}: {qualifier} (actual: {_fmt_number(float(actual))}, expected: {result.expected})"
            )

    return result


# =============================================================================
# Rule Registry
# =============================================================================

RULE_REGISTRY: dict[RuleType, RuleFunction] = {
    # Threshold
    RuleType.MIN_VALUE: _min_value_rule,
    RuleType.MAX_VALUE: _max_value_rule,
    RuleType.MIN_MAX_VALUE: _min_max_value_rule,
    # Percentage
    RuleType.MIN_PERCENT: _min_percent_rule,
    RuleType.MAX_PERCENT: _max_percent_rule,
    RuleType.MIN_MAX_PERCENT: _min_max_percent_rule,
    # Change detection
    RuleType.MAX_CHANGE_PERCENT: _max_change_percent_rule,
    # Count
    RuleType.MIN_COUNT: _min_count_rule,
    RuleType.MAX_COUNT: _max_count_rule,
    RuleType.MIN_MAX_COUNT: _min_max_count_rule,
    # Comparison
    RuleType.EQUAL_TO: _equal_to_rule,
    RuleType.NOT_EQUAL_TO: _not_equal_to_rule,
    # Boolean
    RuleType.IS_TRUE: _is_true_rule,
    RuleType.IS_FALSE: _is_false_rule,
    # Anomaly detection
    RuleType.ANOMALY_PERCENTILE: _anomaly_percentile_rule,
}


def get_rule(rule_type: RuleType) -> RuleFunction:
    """Get a rule function by type.

    Args:
        rule_type: The type of rule to retrieve.

    Returns:
        The rule function.

    Raises:
        ValueError: If rule type is not registered.
    """
    if rule_type not in RULE_REGISTRY:
        raise ValueError(f"Unknown rule type: {rule_type}")
    return RULE_REGISTRY[rule_type]


def evaluate_rule(
    rule_type: RuleType,
    sensor_value: float | None,
    params: dict[str, Any],
    category: str | None = None,
    description: str | None = None,
) -> RuleResult:
    """Evaluate a sensor value against a rule.

    Args:
        rule_type: The type of rule to apply.
        sensor_value: The value from the sensor.
        params: Rule parameters (thresholds, etc.).
        category: Check category for human-readable message formatting.
        description: Check description for dynamic fallback messages.

    Returns:
        The rule evaluation result.
    """
    rule_func = get_rule(rule_type)
    result = rule_func(sensor_value, params)
    if category:
        result = _humanize_message(result, category, description)
    return result


def list_rules() -> list[RuleType]:
    """List all registered rule types.

    Returns:
        List of all rule types.
    """
    return list(RULE_REGISTRY.keys())
