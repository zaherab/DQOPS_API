#!/usr/bin/env python
"""Unit tests for DQOps-style checks (sensors, rules, and executor).

These tests don't require a running database or API server.
Run with: python tests/test_dqops_checks.py

Note: For full API integration tests, run test_api_checks.py with:
  1. docker-compose up -d (to start PostgreSQL and Redis)
  2. uvicorn dq_platform.main:app --reload --port 8000
  3. python tests/test_api_checks.py
"""

import sys
from datetime import datetime


def print_result(test_name: str, passed: bool, details: str = "") -> None:
    """Print test result with color coding."""
    status = "PASS" if passed else "FAIL"
    color = "\033[92m" if passed else "\033[91m"
    reset = "\033[0m"
    print(f"{color}[{status}]{reset} {test_name}")
    if details:
        print(f"       {details}")


def test_sensors() -> dict:
    """Test sensor definitions and SQL generation."""
    from dq_platform.checks.sensors import (
        SensorType,
        get_sensor,
        list_sensors,
        get_column_level_sensors,
        get_table_level_sensors,
    )

    results = {"passed": 0, "failed": 0}

    print("\n=== Testing Sensors ===\n")

    # Test list sensors
    sensors = list_sensors()
    if len(sensors) > 0:
        print_result("List sensors", True, f"Found {len(sensors)} sensors")
        results["passed"] += 1
    else:
        print_result("List sensors", False, "No sensors found")
        results["failed"] += 1

    # Test column vs table level
    col_sensors = get_column_level_sensors()
    table_sensors = get_table_level_sensors()
    if len(col_sensors) > 0 and len(table_sensors) > 0:
        print_result("Column vs Table level sensors", True,
                    f"{len(col_sensors)} column-level, {len(table_sensors)} table-level")
        results["passed"] += 1
    else:
        print_result("Column vs Table level sensors", False,
                    f"Column: {len(col_sensors)}, Table: {len(table_sensors)}")
        results["failed"] += 1

    # Test get_sensor for specific types
    test_cases = [
        (SensorType.ROW_COUNT, "row_count", False),
        (SensorType.NULLS_PERCENT, "nulls_percent", True),
        (SensorType.DISTINCT_COUNT, "distinct_count", True),
        (SensorType.MIN_VALUE, "min_value", True),
    ]

    for sensor_type, expected_name, is_column_level in test_cases:
        sensor = get_sensor(sensor_type)
        if sensor.name == expected_name and sensor.is_column_level == is_column_level:
            print_result(f"Get sensor: {expected_name}", True)
            results["passed"] += 1
        else:
            print_result(f"Get sensor: {expected_name}", False,
                        f"Name: {sensor.name}, is_column_level: {sensor.is_column_level}")
            results["failed"] += 1

    # Test SQL rendering
    row_count_sensor = get_sensor(SensorType.ROW_COUNT)
    sql = row_count_sensor.render({
        "schema_name": "public",
        "table_name": "users",
    })
    if "SELECT COUNT(*)" in sql and "public" in sql and "users" in sql:
        print_result("SQL rendering: row_count", True)
        results["passed"] += 1
    else:
        print_result("SQL rendering: row_count", False, f"SQL: {sql[:100]}")
        results["failed"] += 1

    # Test SQL rendering with partition filter
    nulls_sensor = get_sensor(SensorType.NULLS_PERCENT)
    sql = nulls_sensor.render({
        "schema_name": "public",
        "table_name": "users",
        "column_name": "email",
        "partition_filter": "created_at >= '2024-01-01'",
    })
    if "NULL" in sql and "email" in sql and "partition_filter" not in sql:
        # partition_filter should be used in the WHERE clause
        print_result("SQL rendering: nulls_percent with partition", True)
        results["passed"] += 1
    else:
        print_result("SQL rendering: nulls_percent with partition", False, f"SQL: {sql[:200]}")
        results["failed"] += 1

    return results


def test_rules() -> dict:
    """Test rule evaluation functions."""
    from dq_platform.checks.rules import (
        RuleType,
        evaluate_rule,
        Severity,
        list_rules,
    )

    results = {"passed": 0, "failed": 0}

    print("\n=== Testing Rules ===\n")

    # Test list rules
    rules = list_rules()
    if len(rules) > 0:
        print_result("List rules", True, f"Found {len(rules)} rules")
        results["passed"] += 1
    else:
        print_result("List rules", False, "No rules found")
        results["failed"] += 1

    # Test threshold rules
    test_cases = [
        # (rule_type, sensor_value, params, expected_passed, expected_severity)
        (RuleType.MAX_PERCENT, 5.0, {"max_percent": 10.0, "severity": "error"}, True, Severity.PASSED),
        (RuleType.MAX_PERCENT, 15.0, {"max_percent": 10.0, "severity": "error"}, False, Severity.ERROR),
        (RuleType.MIN_PERCENT, 95.0, {"min_percent": 90.0, "severity": "error"}, True, Severity.PASSED),
        (RuleType.MIN_PERCENT, 85.0, {"min_percent": 90.0, "severity": "error"}, False, Severity.ERROR),
        (RuleType.MAX_COUNT, 5, {"max_count": 10, "severity": "error"}, True, Severity.PASSED),
        (RuleType.MAX_COUNT, 15, {"max_count": 10, "severity": "error"}, False, Severity.ERROR),
        (RuleType.MIN_MAX_VALUE, 50.0, {"min_value": 10.0, "max_value": 100.0, "severity": "error"}, True, Severity.PASSED),
        (RuleType.MIN_MAX_VALUE, 5.0, {"min_value": 10.0, "max_value": 100.0, "severity": "error"}, False, Severity.ERROR),
        (RuleType.MAX_CHANGE_PERCENT, 5.0, {"max_change_percent": 10.0, "severity": "error"}, True, Severity.PASSED),
        (RuleType.MAX_CHANGE_PERCENT, 15.0, {"max_change_percent": 10.0, "severity": "error"}, False, Severity.ERROR),
    ]

    for rule_type, sensor_value, params, expected_passed, expected_severity in test_cases:
        result = evaluate_rule(rule_type, sensor_value, params)
        if result.passed == expected_passed and result.severity == expected_severity:
            print_result(f"Rule: {rule_type.value} with {sensor_value}", True,
                        f"passed={result.passed}, severity={result.severity.value}")
            results["passed"] += 1
        else:
            print_result(f"Rule: {rule_type.value} with {sensor_value}", False,
                        f"Expected passed={expected_passed}, severity={expected_severity.value}, "
                        f"Got passed={result.passed}, severity={result.severity.value}")
            results["failed"] += 1

    # Test null handling
    result = evaluate_rule(RuleType.MAX_PERCENT, None, {"max_percent": 10.0, "severity": "error"})
    if not result.passed and result.severity == Severity.ERROR:
        print_result("Rule: null value handling", True)
        results["passed"] += 1
    else:
        print_result("Rule: null value handling", False,
                    f"Expected failed with error, got passed={result.passed}, severity={result.severity.value}")
        results["failed"] += 1

    # Test ANOMALY_PERCENTILE rule
    print("\n--- Anomaly Percentile Rule ---")

    # Test 1: Insufficient history (< 7 values) -> PASSED
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 100.0,
        {"_historical_values": [1.0, 2.0, 3.0], "severity": "error"}
    )
    if result.passed and result.severity == Severity.PASSED:
        print_result("Anomaly: insufficient history -> PASSED", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: insufficient history -> PASSED", False,
                    f"Expected passed=True, got passed={result.passed}")
        results["failed"] += 1

    # Test 2: Normal value within IQR -> PASSED
    history = [10.0, 12.0, 11.0, 13.0, 10.5, 11.5, 12.5, 11.0, 12.0, 10.0]
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 11.0,
        {"_historical_values": history, "severity": "error"}
    )
    if result.passed and result.severity == Severity.PASSED:
        print_result("Anomaly: normal value within IQR -> PASSED", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: normal value within IQR -> PASSED", False,
                    f"Expected passed=True, got passed={result.passed}, msg={result.message}")
        results["failed"] += 1

    # Test 3: Anomalous value above upper bound -> severity
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 100.0,
        {"_historical_values": history, "severity": "error"}
    )
    if not result.passed and result.severity == Severity.ERROR:
        print_result("Anomaly: value above upper bound -> ERROR", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: value above upper bound -> ERROR", False,
                    f"Expected passed=False, severity=error, got passed={result.passed}, severity={result.severity.value}")
        results["failed"] += 1

    # Test 4: Anomalous value below lower bound -> severity
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, -50.0,
        {"_historical_values": history, "severity": "warning"}
    )
    if not result.passed and result.severity == Severity.WARNING:
        print_result("Anomaly: value below lower bound -> WARNING", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: value below lower bound -> WARNING", False,
                    f"Expected passed=False, severity=warning, got passed={result.passed}, severity={result.severity.value}")
        results["failed"] += 1

    # Test 5: Null sensor value with sufficient history -> severity
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, None,
        {"_historical_values": history, "severity": "error"}
    )
    if not result.passed and result.severity == Severity.ERROR:
        print_result("Anomaly: null sensor value -> ERROR", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: null sensor value -> ERROR", False,
                    f"Expected passed=False, got passed={result.passed}")
        results["failed"] += 1

    # Test 6: All identical history (IQR=0) -> only that value passes
    identical_history = [5.0] * 10
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 5.0,
        {"_historical_values": identical_history, "severity": "error"}
    )
    if result.passed:
        print_result("Anomaly: identical history, same value -> PASSED", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: identical history, same value -> PASSED", False,
                    f"Expected passed=True, got passed={result.passed}, msg={result.message}")
        results["failed"] += 1

    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 6.0,
        {"_historical_values": identical_history, "severity": "error"}
    )
    if not result.passed:
        print_result("Anomaly: identical history, different value -> FAIL", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: identical history, different value -> FAIL", False,
                    f"Expected passed=False, got passed={result.passed}")
        results["failed"] += 1

    # Test 7: History with None values -> filtered out
    history_with_nones = [10.0, None, 12.0, 11.0, None, 13.0, 10.5, 11.5, 12.5, 11.0]
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 11.0,
        {"_historical_values": history_with_nones, "severity": "error"}
    )
    if result.passed:
        print_result("Anomaly: history with Nones filtered -> PASSED", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: history with Nones filtered -> PASSED", False,
                    f"Expected passed=True, got passed={result.passed}")
        results["failed"] += 1

    # Test 8: Empty historical values -> insufficient history
    result = evaluate_rule(
        RuleType.ANOMALY_PERCENTILE, 50.0,
        {"_historical_values": [], "severity": "error"}
    )
    if result.passed and result.severity == Severity.PASSED:
        print_result("Anomaly: empty history -> PASSED", True)
        results["passed"] += 1
    else:
        print_result("Anomaly: empty history -> PASSED", False,
                    f"Expected passed=True, got passed={result.passed}")
        results["failed"] += 1

    return results


def test_dqops_checks() -> dict:
    """Test DQOps check definitions."""
    from dq_platform.checks.dqops_checks import (
        DQOpsCheckType,
        get_check,
        list_checks,
        get_column_level_checks,
        get_table_level_checks,
        get_checks_by_category,
    )

    results = {"passed": 0, "failed": 0}

    print("\n=== Testing DQOps Checks ===\n")

    # Test list checks
    checks = list_checks()
    if len(checks) > 0:
        print_result("List DQOps checks", True, f"Found {len(checks)} checks")
        results["passed"] += 1
    else:
        print_result("List DQOps checks", False, "No checks found")
        results["failed"] += 1

    # Test column vs table level
    col_checks = get_column_level_checks()
    table_checks = get_table_level_checks()
    if len(col_checks) > 0 and len(table_checks) > 0:
        print_result("Column vs Table level checks", True,
                    f"{len(col_checks)} column-level, {len(table_checks)} table-level")
        results["passed"] += 1
    else:
        print_result("Column vs Table level checks", False,
                    f"Column: {len(col_checks)}, Table: {len(table_checks)}")
        results["failed"] += 1

    # Test categories
    categories = ["volume", "nulls", "uniqueness", "numeric", "text", "geographic",
                  "boolean", "datetime", "patterns", "referential", "custom_sql",
                  "anomaly", "comparison"]
    for cat in categories:
        cat_checks = get_checks_by_category(cat)
        if len(cat_checks) > 0:
            print_result(f"Category: {cat}", True, f"{len(cat_checks)} checks")
            results["passed"] += 1
        else:
            print_result(f"Category: {cat}", False, "No checks found")
            results["failed"] += 1

    # Test specific checks
    test_cases = [
        (DQOpsCheckType.ROW_COUNT, "volume", False),
        (DQOpsCheckType.NULLS_PERCENT, "nulls", True),
        (DQOpsCheckType.DUPLICATE_PERCENT, "uniqueness", True),
        (DQOpsCheckType.MEAN_IN_RANGE, "numeric", True),
        (DQOpsCheckType.TEXT_MAX_LENGTH, "text", True),
        (DQOpsCheckType.INVALID_LATITUDE, "geographic", True),
        (DQOpsCheckType.TRUE_PERCENT, "boolean", True),
        (DQOpsCheckType.DATE_VALUES_IN_FUTURE_PERCENT, "datetime", True),
        (DQOpsCheckType.INVALID_EMAIL_FORMAT_FOUND, "patterns", True),
        (DQOpsCheckType.INVALID_UUID_FORMAT_PERCENT, "patterns", True),
        (DQOpsCheckType.FOREIGN_KEY_NOT_FOUND, "referential", True),
        (DQOpsCheckType.DUPLICATE_RECORD_COUNT, "uniqueness", False),
        (DQOpsCheckType.SQL_CONDITION_FAILED_ON_TABLE, "custom_sql", False),
    ]

    for check_type, expected_category, is_column_level in test_cases:
        check = get_check(check_type)
        if check.category == expected_category and check.is_column_level == is_column_level:
            print_result(f"Check: {check_type.value}", True,
                        f"category={check.category}, sensor={check.sensor_type.value}, rule={check.rule_type.value}")
            results["passed"] += 1
        else:
            print_result(f"Check: {check_type.value}", False,
                        f"Expected category={expected_category}, is_column_level={is_column_level}, "
                        f"Got category={check.category}, is_column_level={check.is_column_level}")
            results["failed"] += 1

    return results


def test_check_parameters() -> dict:
    """Test check parameter validation."""
    from dq_platform.checks.dqops_checks import DQOpsCheckType, get_check
    from dq_platform.checks.sensors import get_sensor
    from dq_platform.checks.rules import evaluate_rule, RuleType

    results = {"passed": 0, "failed": 0}

    print("\n=== Testing Check Parameters ===\n")

    # Test default parameters are applied
    check = get_check(DQOpsCheckType.NULLS_PERCENT)
    if check.default_params and "max_percent" in check.default_params:
        print_result("Default params: nulls_percent", True,
                    f"max_percent={check.default_params['max_percent']}")
        results["passed"] += 1
    else:
        print_result("Default params: nulls_percent", False, "No default max_percent found")
        results["failed"] += 1

    # Test sensor default parameters
    sensor = get_sensor(check.sensor_type)
    if sensor.default_params:
        print_result("Sensor default params", True, f"{sensor.name} has defaults")
        results["passed"] += 1
    else:
        print_result("Sensor default params", True, f"{sensor.name} has no defaults (OK)")
        results["passed"] += 1

    # Test rule evaluation with check defaults
    test_value = 3.0  # 3% nulls
    rule_params = check.default_params.copy()
    rule_params["severity"] = "error"
    rule_result = evaluate_rule(check.rule_type, test_value, rule_params)

    if rule_result.passed:  # 3% is less than default 5%, so should pass
        print_result("Rule evaluation with defaults", True,
                    f"Value {test_value}% passed against max {rule_params.get('max_percent')}%")
        results["passed"] += 1
    else:
        print_result("Rule evaluation with defaults", False,
                    f"Value {test_value}% failed against max {rule_params.get('max_percent')}%")
        results["failed"] += 1

    return results


def main() -> int:
    """Run all unit tests."""
    print("=" * 60)
    print("DQOps Checks Unit Tests")
    print("=" * 60)

    # Run all test suites
    sensor_results = test_sensors()
    rule_results = test_rules()
    check_results = test_dqops_checks()
    param_results = test_check_parameters()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Sensor Tests:    {sensor_results['passed']} passed, {sensor_results['failed']} failed")
    print(f"Rule Tests:      {rule_results['passed']} passed, {rule_results['failed']} failed")
    print(f"Check Tests:     {check_results['passed']} passed, {check_results['failed']} failed")
    print(f"Parameter Tests: {param_results['passed']} passed, {param_results['failed']} failed")
    print("=" * 60)

    total_passed = (sensor_results['passed'] + rule_results['passed'] +
                   check_results['passed'] + param_results['passed'])
    total_failed = (sensor_results['failed'] + rule_results['failed'] +
                   check_results['failed'] + param_results['failed'])

    print(f"Total: {total_passed} passed, {total_failed} failed")
    print("=" * 60)

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
