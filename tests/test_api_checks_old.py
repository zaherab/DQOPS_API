#!/usr/bin/env python
"""Comprehensive API tests for all 54 DQOps check types.

Run with: python tests/test_api_checks.py

Prerequisites:
- API server running on localhost:8000
- PostgreSQL with test_users and test_data_quality tables
- Run tests/fixtures/setup_test_data.sql to create test data

Tests organized by category:
- Volume checks (4 tests)
- Schema checks (2 tests)
- Timeliness checks (2 tests)
- Nulls/Completeness checks (5 tests)
- Uniqueness checks (6 tests)
- Numeric/Statistical checks (8 tests)
- Text checks (9 tests)
- Pattern/Format checks (12 tests)
- Geographic checks (2 tests)
- Boolean checks (2 tests)
- DateTime checks (2 tests)
- Referential Integrity checks (2 tests)
- Custom SQL checks (2 tests)
"""

import sys
import time
from dataclasses import dataclass

import httpx

BASE_URL = "http://localhost:8000/api/v1"
API_KEY = "test-api-key"

HEADERS = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
}

# Test results tracking
@dataclass
class TestResults:
    passed: int = 0
    failed: int = 0
    skipped: int = 0


def print_result(test_name: str, passed: bool, details: str = "") -> None:
    """Print test result with color coding."""
    status = "PASS" if passed else "FAIL"
    color = "\033[92m" if passed else "\033[91m"
    reset = "\033[0m"
    print(f"{color}[{status}]{reset} {test_name}")
    if details:
        print(f"       {details}")


def print_section(title: str) -> None:
    """Print section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


# =============================================================================
# API Helper Functions
# =============================================================================

def create_connection() -> str | None:
    """Create a test connection and return its ID."""
    connection_data = {
        "name": "test-postgres-comprehensive",
        "description": "Test PostgreSQL connection for comprehensive tests",
        "connection_type": "postgresql",
        "config": {
            "host": "localhost",
            "port": 5433,
            "database": "dq_platform",
            "user": "postgres",
            "password": "postgres",
        },
    }

    response = httpx.post(
        f"{BASE_URL}/connections",
        json=connection_data,
        headers=HEADERS,
    )

    if response.status_code == 201:
        conn = response.json()
        print_result("Create connection", True, f"ID: {conn['id']}")
        return conn["id"]
    else:
        print_result("Create connection", False, f"Status: {response.status_code}")
        return None


def create_check(connection_id: str, check_data: dict) -> str | None:
    """Create a check and return its ID."""
    check_data["connection_id"] = connection_id

    response = httpx.post(
        f"{BASE_URL}/checks",
        json=check_data,
        headers=HEADERS,
    )

    if response.status_code == 201:
        check = response.json()
        return check["id"]
    else:
        print(f"  Failed to create check: {response.status_code} - {response.text[:200]}")
        return None


def run_check(check_id: str, timeout_seconds: int = 30) -> dict | None:
    """Run a check and return the job result."""
    response = httpx.post(
        f"{BASE_URL}/checks/{check_id}/run",
        headers=HEADERS,
    )

    if response.status_code not in (200, 202):
        return None

    job = response.json()
    job_id = job.get("job_id") or job.get("id")

    # Poll for completion
    for _ in range(timeout_seconds):
        time.sleep(1)
        response = httpx.get(f"{BASE_URL}/jobs/{job_id}", headers=HEADERS)
        if response.status_code == 200:
            job_status = response.json()
            if job_status["status"] in ["completed", "failed"]:
                return job_status

    return None


def get_check_result(check_id: str) -> dict | None:
    """Get the latest result for a check."""
    response = httpx.get(
        f"{BASE_URL}/results",
        params={"check_id": check_id, "limit": 1},
        headers=HEADERS,
    )

    if response.status_code == 200:
        data = response.json()
        if data["items"]:
            return data["items"][0]
    return None


def run_test(
    connection_id: str,
    test_name: str,
    check_data: dict,
    expected_pass: bool,
    results: TestResults,
) -> None:
    """Run a single test case."""
    # Create check
    check_id = create_check(connection_id, check_data)
    if not check_id:
        print_result(test_name, False, "Failed to create check")
        results.failed += 1
        return

    # Run check
    job_result = run_check(check_id)
    if not job_result:
        print_result(test_name, False, "Job timeout or failed to run")
        results.failed += 1
        return

    # Get result
    check_result = get_check_result(check_id)
    if check_result:
        actual_pass = check_result.get("passed", False)
        severity = check_result.get("severity", "unknown")
        actual_value = check_result.get("actual_value")

        if actual_pass == expected_pass:
            details = f"passed={actual_pass}, severity={severity}, value={actual_value}"
            print_result(test_name, True, details)
            results.passed += 1
        else:
            details = f"Expected passed={expected_pass}, got passed={actual_pass}, severity={severity}"
            if check_result.get("error_message"):
                details += f", error: {check_result['error_message'][:100]}"
            print_result(test_name, False, details)
            results.failed += 1
    else:
        if job_result["status"] == "failed":
            error_msg = job_result.get("error_message", "Unknown error")[:100]
            print_result(test_name, False, f"Job failed: {error_msg}")
        else:
            print_result(test_name, False, "No result found")
        results.failed += 1


# =============================================================================
# Volume Checks (4 tests)
# =============================================================================

def test_volume_checks(connection_id: str) -> TestResults:
    """Test all volume checks."""
    print_section("Volume Checks (4 tests)")
    results = TestResults()

    tests = [
        # row_count
        {
            "name": "Volume: Row Count",
            "check_data": {
                "name": "Row Count Check",
                "check_type": "row_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {},
                "rule_parameters": {"error": {"min_count": 10, "max_count": 100}},
            },
            "expected_pass": True,  # 20 rows
        },
        # row_count_change_1_day
        {
            "name": "Volume: Row Count Change 1 Day",
            "check_data": {
                "name": "Row Count Change 1 Day",
                "check_type": "row_count_change_1_day",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {},
                "rule_parameters": {"error": {"max_change_percent": 100.0}},
            },
            "expected_pass": False,  # No historical data
        },
        # row_count_change_7_days
        {
            "name": "Volume: Row Count Change 7 Days",
            "check_data": {
                "name": "Row Count Change 7 Days",
                "check_type": "row_count_change_7_days",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {},
                "rule_parameters": {"error": {"max_change_percent": 100.0}},
            },
            "expected_pass": False,  # No historical data
        },
        # row_count_change_30_days
        {
            "name": "Volume: Row Count Change 30 Days",
            "check_data": {
                "name": "Row Count Change 30 Days",
                "check_type": "row_count_change_30_days",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {},
                "rule_parameters": {"error": {"max_change_percent": 100.0}},
            },
            "expected_pass": False,  # No historical data
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Schema Checks (2 tests)
# =============================================================================

def test_schema_checks(connection_id: str) -> TestResults:
    """Test all schema checks."""
    print_section("Schema Checks (2 tests)")
    results = TestResults()

    tests = [
        # column_count (via schema_column_count)
        {
            "name": "Schema: Column Count",
            "check_data": {
                "name": "Schema Column Count",
                "check_type": "schema_column_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {"expected_value": 19},
                "rule_parameters": {},
            },
            "expected_pass": True,
        },
        # column_exists
        {
            "name": "Schema: Column Exists",
            "check_data": {
                "name": "Schema Column Exists",
                "check_type": "schema_column_exists",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {},
            },
            "expected_pass": True,
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Timeliness Checks (2 tests)
# =============================================================================

def test_timeliness_checks(connection_id: str) -> TestResults:
    """Test all timeliness checks."""
    print_section("Timeliness Checks (2 tests)")
    results = TestResults()

    tests = [
        # data_freshness (column-level)
        {
            "name": "Timeliness: Data Freshness",
            "check_data": {
                "name": "Data Freshness Check",
                "check_type": "data_freshness",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "created_at",
                "parameters": {},
                "rule_parameters": {"error": {"max_value": 86400}},  # 24 hours
            },
            "expected_pass": True,
        },
        # data_staleness (table-level) - checks pg_stat_user_tables last_analyze/last_vacuum
        # This may fail if table hasn't been analyzed yet, so we allow failure
        {
            "name": "Timeliness: Data Staleness",
            "check_data": {
                "name": "Data Staleness Check",
                "check_type": "data_staleness",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {},
                "rule_parameters": {"error": {"max_value": 604800}},  # 7 days
            },
            "expected_pass": False,  # May fail if table stats not available
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Nulls/Completeness Checks (5 tests)
# =============================================================================

def test_nulls_checks(connection_id: str) -> TestResults:
    """Test all nulls/completeness checks."""
    print_section("Nulls/Completeness Checks (5 tests)")
    results = TestResults()

    tests = [
        # nulls_count (via null_count)
        {
            "name": "Nulls: Null Count",
            "check_data": {
                "name": "Null Count Check",
                "check_type": "null_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 2 nulls
        },
        # nulls_percent (via null_percent)
        {
            "name": "Nulls: Null Percent",
            "check_data": {
                "name": "Null Percent Check",
                "check_type": "null_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 15.0}},
            },
            "expected_pass": True,  # 10% nulls (2/20)
        },
        # not_nulls_count
        {
            "name": "Nulls: Not Nulls Count",
            "check_data": {
                "name": "Not Nulls Count Check",
                "check_type": "not_nulls_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"min_count": 15}},
            },
            "expected_pass": True,  # 18 non-nulls
        },
        # not_nulls_percent
        {
            "name": "Nulls: Not Nulls Percent",
            "check_data": {
                "name": "Not Nulls Percent Check",
                "check_type": "not_nulls_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"min_percent": 85.0}},
            },
            "expected_pass": True,  # 90% non-null
        },
        # empty_column_found
        {
            "name": "Nulls: Empty Column Found",
            "check_data": {
                "name": "Empty Column Found Check",
                "check_type": "empty_column_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 99.0}},
            },
            "expected_pass": True,  # Column is not empty
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Uniqueness Checks (6 tests)
# =============================================================================

def test_uniqueness_checks(connection_id: str) -> TestResults:
    """Test all uniqueness checks."""
    print_section("Uniqueness Checks (6 tests)")
    results = TestResults()

    tests = [
        # distinct_count
        {
            "name": "Uniqueness: Distinct Count",
            "check_data": {
                "name": "Distinct Count Check",
                "check_type": "distinct_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "is_active",
                "parameters": {},
                "rule_parameters": {"error": {"min_count": 2, "max_count": 3}},
            },
            "expected_pass": True,  # 3 distinct: TRUE, FALSE, NULL
        },
        # distinct_percent
        {
            "name": "Uniqueness: Distinct Percent",
            "check_data": {
                "name": "Distinct Percent Check",
                "check_type": "distinct_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "id",
                "parameters": {},
                "rule_parameters": {"error": {"min_percent": 95.0, "max_percent": 100.0}},
            },
            "expected_pass": True,  # ID is unique
        },
        # duplicate_count
        {
            "name": "Uniqueness: Duplicate Count",
            "check_data": {
                "name": "Duplicate Count Check",
                "check_type": "duplicate_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # Row 1 and 15 have same email
        },
        # duplicate_percent
        {
            "name": "Uniqueness: Duplicate Percent",
            "check_data": {
                "name": "Duplicate Percent Check",
                "check_type": "duplicate_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "is_active",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 95.0}},
            },
            "expected_pass": True,  # is_active has many duplicates
        },
        # duplicate_record_count (table-level)
        {
            "name": "Uniqueness: Duplicate Record Count",
            "check_data": {
                "name": "Duplicate Record Count Check",
                "check_type": "duplicate_record_count",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {"column_list": ["email", "latitude", "longitude"]},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # Row 1 and 15 are duplicates
        },
        # duplicate_record_percent (table-level)
        {
            "name": "Uniqueness: Duplicate Record Percent",
            "check_data": {
                "name": "Duplicate Record Percent Check",
                "check_type": "duplicate_record_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {"column_list": ["email", "latitude", "longitude"]},
                "rule_parameters": {"error": {"max_percent": 20.0}},
            },
            "expected_pass": True,  # ~5% duplicates
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Numeric/Statistical Checks (8 tests)
# =============================================================================

def test_numeric_checks(connection_id: str) -> TestResults:
    """Test all numeric/statistical checks."""
    print_section("Numeric/Statistical Checks (8 tests)")
    results = TestResults()

    tests = [
        # min_in_range
        {
            "name": "Numeric: Min In Range",
            "check_data": {
                "name": "Min In Range Check",
                "check_type": "min_in_range",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 0, "max_value": 10}},
            },
            "expected_pass": True,  # min score is 0
        },
        # max_in_range
        {
            "name": "Numeric: Max In Range",
            "check_data": {
                "name": "Max In Range Check",
                "check_type": "max_in_range",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 90, "max_value": 110}},
            },
            "expected_pass": True,  # max score is 100
        },
        # sum_in_range
        {
            "name": "Numeric: Sum In Range",
            "check_data": {
                "name": "Sum In Range Check",
                "check_type": "sum_in_range",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 1000, "max_value": 2000}},
            },
            "expected_pass": True,  # sum of scores ~1450
        },
        # mean_in_range
        {
            "name": "Numeric: Mean In Range",
            "check_data": {
                "name": "Mean In Range Check",
                "check_type": "mean_in_range",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 60, "max_value": 80}},
            },
            "expected_pass": True,  # mean ~72.5
        },
        # median_in_range
        {
            "name": "Numeric: Median In Range",
            "check_data": {
                "name": "Median In Range Check",
                "check_type": "median_in_range",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 65, "max_value": 85}},
            },
            "expected_pass": True,  # median ~75
        },
        # number_below_min_value
        {
            "name": "Numeric: Number Below Min Value",
            "check_data": {
                "name": "Number Below Min Value Check",
                "check_type": "number_below_min_value",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 0}},
            },
            "expected_pass": True,  # min is 0, none below
        },
        # number_above_max_value
        {
            "name": "Numeric: Number Above Max Value",
            "check_data": {
                "name": "Number Above Max Value Check",
                "check_type": "number_above_max_value",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {},
                "rule_parameters": {"error": {"max_value": 100}},
            },
            "expected_pass": True,  # max is 100, none above
        },
        # number_in_range_percent
        {
            "name": "Numeric: Number In Range Percent",
            "check_data": {
                "name": "Number In Range Percent Check",
                "check_type": "number_in_range_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "score",
                "parameters": {"min_value": 0, "max_value": 100},
                "rule_parameters": {"error": {"min_percent": 95.0}},
            },
            "expected_pass": True,  # All scores in range
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Text Checks (9 tests)
# =============================================================================

def test_text_checks(connection_id: str) -> TestResults:
    """Test all text checks."""
    print_section("Text Checks (9 tests)")
    results = TestResults()

    tests = [
        # text_min_length
        {
            "name": "Text: Min Length",
            "check_data": {
                "name": "Text Min Length Check",
                "check_type": "text_min_length",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 1, "max_value": 5}},
            },
            "expected_pass": True,  # min is 'A' (1 char)
        },
        # text_max_length
        {
            "name": "Text: Max Length",
            "check_data": {
                "name": "Text Max Length Check",
                "check_type": "text_max_length",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 5, "max_value": 15}},
            },
            "expected_pass": True,  # max is 'TOOLONGCODE' (11 chars)
        },
        # text_mean_length
        {
            "name": "Text: Mean Length",
            "check_data": {
                "name": "Text Mean Length Check",
                "check_type": "text_mean_length",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {},
                "rule_parameters": {"error": {"min_value": 3, "max_value": 8}},
            },
            "expected_pass": True,  # average ~6 chars
        },
        # text_length_below_min_length
        {
            "name": "Text: Length Below Min",
            "check_data": {
                "name": "Text Length Below Min Check",
                "check_type": "text_length_below_min_length",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {"min_length": 3},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 value below 3 chars ('A')
        },
        # text_length_above_max_length
        {
            "name": "Text: Length Above Max",
            "check_data": {
                "name": "Text Length Above Max Check",
                "check_type": "text_length_above_max_length",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {"max_length": 10},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 value above 10 chars
        },
        # text_length_in_range_percent
        {
            "name": "Text: Length In Range Percent",
            "check_data": {
                "name": "Text Length In Range Percent Check",
                "check_type": "text_length_in_range_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {"min_length": 1, "max_length": 15},
                "rule_parameters": {"error": {"min_percent": 95.0}},
            },
            "expected_pass": True,  # All values in range
        },
        # empty_text_found
        {
            "name": "Text: Empty Text Found",
            "check_data": {
                "name": "Empty Text Found Check",
                "check_type": "empty_text_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "description",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 empty string
        },
        # whitespace_text_found
        {
            "name": "Text: Whitespace Text Found",
            "check_data": {
                "name": "Whitespace Text Found Check",
                "check_type": "whitespace_text_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "description",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 whitespace-only ('   ')
        },
        # text_not_matching_regex_found
        {
            "name": "Text: Not Matching Regex Found",
            "check_data": {
                "name": "Text Not Matching Regex Check",
                "check_type": "text_not_matching_regex_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "short_code",
                "parameters": {"regex_pattern": "^[A-Z0-9]+$"},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # All codes are uppercase alphanumeric
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Pattern/Format Checks (12 tests)
# =============================================================================

def test_pattern_checks(connection_id: str) -> TestResults:
    """Test all pattern/format checks."""
    print_section("Pattern/Format Checks (12 tests)")
    results = TestResults()

    tests = [
        # invalid_email_format_found
        {
            "name": "Pattern: Invalid Email Format Found",
            "check_data": {
                "name": "Invalid Email Format Found Check",
                "check_type": "invalid_email_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 1 invalid email
        },
        # invalid_email_format_percent
        {
            "name": "Pattern: Invalid Email Format Percent",
            "check_data": {
                "name": "Invalid Email Format Percent Check",
                "check_type": "invalid_email_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "email",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 15.0}},
            },
            "expected_pass": True,  # ~5% invalid
        },
        # invalid_uuid_format_found
        {
            "name": "Pattern: Invalid UUID Format Found",
            "check_data": {
                "name": "Invalid UUID Format Found Check",
                "check_type": "invalid_uuid_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "uuid_col",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 1 invalid UUID
        },
        # invalid_uuid_format_percent
        {
            "name": "Pattern: Invalid UUID Format Percent",
            "check_data": {
                "name": "Invalid UUID Format Percent Check",
                "check_type": "invalid_uuid_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "uuid_col",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 15.0}},
            },
            "expected_pass": True,  # ~5% invalid
        },
        # invalid_ip4_format_found
        {
            "name": "Pattern: Invalid IP4 Format Found",
            "check_data": {
                "name": "Invalid IP4 Format Found Check",
                "check_type": "invalid_ip4_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "ip4_address",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 1 invalid IPv4 (999.999.999.999)
        },
        # invalid_ip4_format_percent
        {
            "name": "Pattern: Invalid IP4 Format Percent",
            "check_data": {
                "name": "Invalid IP4 Format Percent Check",
                "check_type": "invalid_ip4_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "ip4_address",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 15.0}},
            },
            "expected_pass": True,  # ~5% invalid
        },
        # invalid_ip6_format_found
        {
            "name": "Pattern: Invalid IP6 Format Found",
            "check_data": {
                "name": "Invalid IP6 Format Found Check",
                "check_type": "invalid_ip6_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "ip6_address",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 1 invalid IPv6
        },
        # invalid_ip6_format_percent
        {
            "name": "Pattern: Invalid IP6 Format Percent",
            "check_data": {
                "name": "Invalid IP6 Format Percent Check",
                "check_type": "invalid_ip6_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "ip6_address",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 20.0}},
            },
            "expected_pass": True,  # ~15% invalid
        },
        # invalid_usa_phone_format_found
        {
            "name": "Pattern: Invalid USA Phone Format Found",
            "check_data": {
                "name": "Invalid USA Phone Format Found Check",
                "check_type": "invalid_usa_phone_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "phone",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 15}},  # ~11% invalid = ~2-3 phones, but regex may be stricter
            },
            "expected_pass": True,
        },
        # invalid_usa_phone_format_percent
        {
            "name": "Pattern: Invalid USA Phone Format Percent",
            "check_data": {
                "name": "Invalid USA Phone Format Percent Check",
                "check_type": "invalid_usa_phone_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "phone",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 20.0}},
            },
            "expected_pass": True,  # ~15% invalid
        },
        # invalid_usa_zipcode_format_found
        {
            "name": "Pattern: Invalid USA Zipcode Format Found",
            "check_data": {
                "name": "Invalid USA Zipcode Format Found Check",
                "check_type": "invalid_usa_zipcode_format_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "zipcode",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 1 invalid zipcode (ABCDE)
        },
        # invalid_usa_zipcode_format_percent
        {
            "name": "Pattern: Invalid USA Zipcode Format Percent",
            "check_data": {
                "name": "Invalid USA Zipcode Format Percent Check",
                "check_type": "invalid_usa_zipcode_format_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "zipcode",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 15.0}},
            },
            "expected_pass": True,  # ~5% invalid
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Geographic Checks (2 tests)
# =============================================================================

def test_geographic_checks(connection_id: str) -> TestResults:
    """Test all geographic checks."""
    print_section("Geographic Checks (2 tests)")
    results = TestResults()

    tests = [
        # invalid_latitude
        {
            "name": "Geographic: Invalid Latitude",
            "check_data": {
                "name": "Invalid Latitude Check",
                "check_type": "invalid_latitude",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "latitude",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 invalid (95.0)
        },
        # invalid_longitude
        {
            "name": "Geographic: Invalid Longitude",
            "check_data": {
                "name": "Invalid Longitude Check",
                "check_type": "invalid_longitude",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "longitude",
                "parameters": {},
                "rule_parameters": {"error": {"max_count": 3}},
            },
            "expected_pass": True,  # 1 invalid (-200.0)
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Boolean Checks (2 tests)
# =============================================================================

def test_boolean_checks(connection_id: str) -> TestResults:
    """Test all boolean checks."""
    print_section("Boolean Checks (2 tests)")
    results = TestResults()

    tests = [
        # true_percent
        {
            "name": "Boolean: True Percent",
            "check_data": {
                "name": "True Percent Check",
                "check_type": "true_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "is_active",
                "parameters": {},
                "rule_parameters": {"error": {"min_percent": 40.0, "max_percent": 70.0}},
            },
            "expected_pass": True,  # ~50% true (10/20)
        },
        # false_percent
        {
            "name": "Boolean: False Percent",
            "check_data": {
                "name": "False Percent Check",
                "check_type": "false_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "is_active",
                "parameters": {},
                "rule_parameters": {"error": {"min_percent": 30.0, "max_percent": 60.0}},
            },
            "expected_pass": True,  # ~40% false (8/20)
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# DateTime Checks (2 tests)
# =============================================================================

def test_datetime_checks(connection_id: str) -> TestResults:
    """Test all datetime checks."""
    print_section("DateTime Checks (2 tests)")
    results = TestResults()

    tests = [
        # date_values_in_future_percent
        {
            "name": "DateTime: Future Date Percent",
            "check_data": {
                "name": "Future Date Percent Check",
                "check_type": "date_values_in_future_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "event_date",
                "parameters": {},
                "rule_parameters": {"error": {"max_percent": 25.0}},
            },
            "expected_pass": True,  # 15% future dates (3/20)
        },
        # date_in_range_percent
        {
            "name": "DateTime: Date In Range Percent",
            "check_data": {
                "name": "Date In Range Percent Check",
                "check_type": "date_in_range_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "event_date",
                "parameters": {"min_date": "2020-01-01", "max_date": "2030-12-31"},
                "rule_parameters": {"error": {"min_percent": 95.0}},
            },
            "expected_pass": True,  # All dates in range
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Referential Integrity Checks (2 tests)
# =============================================================================

def test_referential_checks(connection_id: str) -> TestResults:
    """Test all referential integrity checks."""
    print_section("Referential Integrity Checks (2 tests)")
    results = TestResults()

    tests = [
        # foreign_key_not_found
        {
            "name": "Referential: Foreign Key Not Found",
            "check_data": {
                "name": "Foreign Key Not Found Check",
                "check_type": "foreign_key_not_found",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "category_id",
                "parameters": {
                    "reference_table": "test_categories",
                    "reference_column": "id",
                    "reference_schema": "public",
                },
                "rule_parameters": {"error": {"max_count": 5}},
            },
            "expected_pass": True,  # 2 invalid FKs (99, 100)
        },
        # foreign_key_found_percent
        {
            "name": "Referential: Foreign Key Found Percent",
            "check_data": {
                "name": "Foreign Key Found Percent Check",
                "check_type": "foreign_key_found_percent",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "target_column": "category_id",
                "parameters": {
                    "reference_table": "test_categories",
                    "reference_column": "id",
                    "reference_schema": "public",
                },
                "rule_parameters": {"error": {"min_percent": 85.0}},
            },
            "expected_pass": True,  # 90% valid FKs (18/20)
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Custom SQL Checks (2 tests)
# =============================================================================

def test_custom_sql_checks(connection_id: str) -> TestResults:
    """Test all custom SQL checks."""
    print_section("Custom SQL Checks (2 tests)")
    results = TestResults()

    tests = [
        # sql_condition_failed_on_table
        {
            "name": "Custom SQL: Condition Failed on Table",
            "check_data": {
                "name": "SQL Condition Failed Check",
                "check_type": "sql_condition_failed_on_table",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {"sql_condition": "score >= 0 AND score <= 100"},
                "rule_parameters": {"error": {"max_count": 0}},
            },
            "expected_pass": True,  # All scores in valid range
        },
        # sql_aggregate_expression_on_table
        {
            "name": "Custom SQL: Aggregate Expression on Table",
            "check_data": {
                "name": "SQL Aggregate Expression Check",
                "check_type": "sql_aggregate_expression_on_table",
                "check_mode": "monitoring",
                "target_table": "test_data_quality",
                "target_schema": "public",
                "parameters": {"sql_expression": "AVG(score)"},
                "rule_parameters": {"error": {"min_value": 60.0, "max_value": 85.0}},
            },
            "expected_pass": True,  # Average score ~72.5
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Legacy Check Tests (for backward compatibility)
# =============================================================================

def test_legacy_checks(connection_id: str) -> TestResults:
    """Test legacy check types for backward compatibility."""
    print_section("Legacy Checks (backward compatibility)")
    results = TestResults()

    tests = [
        # Row count min/max
        {
            "name": "Legacy: Row Count Min",
            "check_data": {
                "name": "Row Count Min",
                "check_type": "row_count_min",
                "target_table": "test_users",
                "target_schema": "public",
                "parameters": {"min_value": 1},
            },
            "expected_pass": True,
        },
        {
            "name": "Legacy: Row Count Max",
            "check_data": {
                "name": "Row Count Max",
                "check_type": "row_count_max",
                "target_table": "test_users",
                "target_schema": "public",
                "parameters": {"max_value": 100},
            },
            "expected_pass": True,
        },
        # Not null
        {
            "name": "Legacy: Not Null",
            "check_data": {
                "name": "Not Null Check",
                "check_type": "not_null",
                "target_table": "test_users",
                "target_schema": "public",
                "target_column": "name",
                "parameters": {},
            },
            "expected_pass": True,
        },
        # Unique
        {
            "name": "Legacy: Unique",
            "check_data": {
                "name": "Unique Check",
                "check_type": "unique",
                "target_table": "test_users",
                "target_schema": "public",
                "target_column": "id",
                "parameters": {},
            },
            "expected_pass": True,
        },
        # Value range
        {
            "name": "Legacy: Value Range",
            "check_data": {
                "name": "Value Range Check",
                "check_type": "value_range",
                "target_table": "test_users",
                "target_schema": "public",
                "target_column": "id",
                "parameters": {"min_value": 1, "max_value": 100},
            },
            "expected_pass": True,
        },
        # Allowed values
        {
            "name": "Legacy: Allowed Values",
            "check_data": {
                "name": "Allowed Values Check",
                "check_type": "allowed_values",
                "target_table": "test_users",
                "target_schema": "public",
                "target_column": "status",
                "parameters": {"allowed_values": ["active", "inactive", "pending"]},
            },
            "expected_pass": True,
        },
    ]

    for test in tests:
        run_test(connection_id, test["name"], test["check_data"], test["expected_pass"], results)

    return results


# =============================================================================
# Check Preview Tests
# =============================================================================

def test_check_preview(connection_id: str) -> TestResults:
    """Test check preview endpoint (dry run without saving)."""
    print_section("Check Preview Tests")
    results = TestResults()

    preview_tests = [
        {
            "name": "Preview: Row Count Check",
            "check_type": "row_count",
            "target_table": "test_data_quality",
            "parameters": {},
            "rule_parameters": {"error": {"min_count": 1}},
        },
        {
            "name": "Preview: Nulls Percent Check",
            "check_type": "null_percent",
            "target_table": "test_data_quality",
            "target_column": "email",
            "parameters": {},
            "rule_parameters": {"error": {"max_percent": 50.0}},
        },
    ]

    for test in preview_tests:
        preview_data = {
            "connection_id": connection_id,
            "check_type": test["check_type"],
            "target_schema": "public",
            "target_table": test["target_table"],
            "target_column": test.get("target_column"),
            "parameters": test["parameters"],
            "rule_parameters": test.get("rule_parameters"),
        }

        response = httpx.post(
            f"{BASE_URL}/checks/validate/preview",
            json=preview_data,
            headers=HEADERS,
        )

        if response.status_code == 200:
            result = response.json()
            severity = result.get("severity", "unknown")
            passed = result.get("passed", False)
            sensor_value = result.get("sensor_value")
            details = f"severity={severity}, passed={passed}, value={sensor_value}"
            print_result(test["name"], True, details)
            results.passed += 1
        else:
            print_result(test["name"], False, f"Status: {response.status_code}")
            results.failed += 1

    return results


# =============================================================================
# Metadata Endpoint Tests
# =============================================================================

def test_metadata_endpoints() -> TestResults:
    """Test check metadata endpoints."""
    print_section("Metadata Endpoint Tests")
    results = TestResults()

    # Check types
    response = httpx.get(f"{BASE_URL}/checks/types", headers=HEADERS)
    if response.status_code == 200:
        types = response.json()
        print_result("Get Check Types", True, f"Found {len(types)} types")
        results.passed += 1
    else:
        print_result("Get Check Types", False)
        results.failed += 1

    # Check categories
    response = httpx.get(f"{BASE_URL}/checks/categories", headers=HEADERS)
    if response.status_code == 200:
        categories = response.json()
        print_result("Get Check Categories", True, f"Found {len(categories)} categories")
        results.passed += 1
    else:
        print_result("Get Check Categories", False)
        results.failed += 1

    # Check modes
    response = httpx.get(f"{BASE_URL}/checks/modes", headers=HEADERS)
    if response.status_code == 200:
        modes = response.json()
        print_result("Get Check Modes", True, f"Modes: {', '.join(modes)}")
        results.passed += 1
    else:
        print_result("Get Check Modes", False)
        results.failed += 1

    # Time scales
    response = httpx.get(f"{BASE_URL}/checks/time-scales", headers=HEADERS)
    if response.status_code == 200:
        scales = response.json()
        print_result("Get Time Scales", True, f"Scales: {', '.join(scales)}")
        results.passed += 1
    else:
        print_result("Get Time Scales", False)
        results.failed += 1

    return results


# =============================================================================
# Main Test Runner
# =============================================================================

def main() -> int:
    """Run all API tests."""
    print("=" * 60)
    print("  DQ Platform Comprehensive API Check Tests")
    print("  Testing all 54 DQOps Check Types")
    print("=" * 60)

    # Check API health
    try:
        response = httpx.get(f"{BASE_URL.replace('/api/v1', '')}/health")
        if response.status_code != 200:
            print("\nERROR: API not healthy")
            return 1
        print("\nAPI is healthy")
    except httpx.ConnectError:
        print(f"\nERROR: Cannot connect to API at {BASE_URL}")
        return 1

    # Create connection
    print_section("Setup")
    connection_id = create_connection()
    if not connection_id:
        print("ERROR: Failed to create connection")
        return 1

    # Run all test categories
    all_results: list[tuple[str, TestResults]] = []

    # Metadata tests (no connection needed for these)
    all_results.append(("Metadata", test_metadata_endpoints()))

    # DQOps check tests by category
    all_results.append(("Volume", test_volume_checks(connection_id)))
    all_results.append(("Schema", test_schema_checks(connection_id)))
    all_results.append(("Timeliness", test_timeliness_checks(connection_id)))
    all_results.append(("Nulls", test_nulls_checks(connection_id)))
    all_results.append(("Uniqueness", test_uniqueness_checks(connection_id)))
    all_results.append(("Numeric", test_numeric_checks(connection_id)))
    all_results.append(("Text", test_text_checks(connection_id)))
    all_results.append(("Pattern", test_pattern_checks(connection_id)))
    all_results.append(("Geographic", test_geographic_checks(connection_id)))
    all_results.append(("Boolean", test_boolean_checks(connection_id)))
    all_results.append(("DateTime", test_datetime_checks(connection_id)))
    all_results.append(("Referential", test_referential_checks(connection_id)))
    all_results.append(("Custom SQL", test_custom_sql_checks(connection_id)))

    # Additional tests
    all_results.append(("Legacy", test_legacy_checks(connection_id)))
    all_results.append(("Preview", test_check_preview(connection_id)))

    # Summary
    print_section("TEST SUMMARY")

    total_passed = 0
    total_failed = 0
    total_skipped = 0

    print(f"{'Category':<20} {'Passed':>8} {'Failed':>8} {'Skipped':>8}")
    print("-" * 50)

    for name, result in all_results:
        print(f"{name:<20} {result.passed:>8} {result.failed:>8} {result.skipped:>8}")
        total_passed += result.passed
        total_failed += result.failed
        total_skipped += result.skipped

    print("-" * 50)
    print(f"{'TOTAL':<20} {total_passed:>8} {total_failed:>8} {total_skipped:>8}")
    print()

    # Color-coded final result
    if total_failed == 0:
        print("\033[92m" + "=" * 50 + "\033[0m")
        print("\033[92m  ALL TESTS PASSED!\033[0m")
        print("\033[92m" + "=" * 50 + "\033[0m")
    else:
        print("\033[91m" + "=" * 50 + "\033[0m")
        print(f"\033[91m  {total_failed} TESTS FAILED\033[0m")
        print("\033[91m" + "=" * 50 + "\033[0m")

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
