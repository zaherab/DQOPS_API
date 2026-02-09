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
) -> RuleResult:
    """Evaluate a sensor value against a rule.

    Args:
        rule_type: The type of rule to apply.
        sensor_value: The value from the sensor.
        params: Rule parameters (thresholds, etc.).

    Returns:
        The rule evaluation result.
    """
    rule_func = get_rule(rule_type)
    return rule_func(sensor_value, params)


def list_rules() -> list[RuleType]:
    """List all registered rule types.

    Returns:
        List of all rule types.
    """
    return list(RULE_REGISTRY.keys())
