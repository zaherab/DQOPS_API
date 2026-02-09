"""Comprehensive API integration tests for all 171 DQOps check types.

This module tests the DQ Platform's data quality checks through the API,
covering all check categories with both positive (expected pass) and
negative (expected fail) test cases.

Test Coverage:
    - 230+ parametrized test cases across 33 test classes
    - Original checks (54): Volume, Schema, Timeliness, Nulls, Uniqueness,
      Numeric, Text, Pattern, Geographic, Boolean, DateTime, Referential, Custom SQL
    - Phase 1 (10): Whitespace & text checks
    - Phase 2 (10): Geographic & numeric percent variants
    - Phase 3 (10): Statistical & percentile checks
    - Phase 4 (7): Accepted values & domain checks
    - Phase 5 (5): Date pattern & data type detection
    - Phase 6 (6): PII detection checks
    - Phase 7 (6): Change detection checks
    - Phase 8 (6): Cross-table comparison checks
    - Phase 9 (3): Table-level misc checks
    - Phase 10 (22): Text length percent, column/table custom SQL, schema detection
    - Phase 11 (16): Import external results, generic change detection
    - Phase 12 (29): Anomaly detection, cross-source comparison

Prerequisites:
    - API server running on localhost:8000 (or DQ_API_URL env var)
    - PostgreSQL with test data (auto-setup via conftest.py)
    - Celery worker running for async job processing

Run with:
    pytest tests/integration/test_api_checks.py -v

Run specific category:
    pytest tests/integration/test_api_checks.py::TestVolumeChecks -v
    pytest tests/integration/test_api_checks.py::TestWhitespaceTextChecks -v
    pytest tests/integration/test_api_checks.py::TestStatisticalChecks -v

Test data reference (test_data_quality table - 20 rows):
    - email: 2 nulls, 1 invalid format, 1 duplicate
    - score: range 0-100, mean ~72.5
    - short_code: lengths 1-11 chars
    - latitude: 1 invalid (95.0)
    - longitude: 1 invalid (-200.0)
    - is_active: ~50% true, ~40% false, some nulls
    - uuid_col: 1 invalid UUID
    - ip4_address: 1 invalid IPv4
    - ip6_address: 1-3 invalid IPv6
    - phone: ~15% invalid
    - zipcode: 1 invalid
    - event_date: 3 future dates (15%)
    - category_id: 2 invalid FKs (99, 100)
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    import httpx

from tests.integration.conftest import run_check_and_wait


# =============================================================================
# Test Data Constants
# =============================================================================

# Default table for most tests
DEFAULT_TABLE = "test_data_quality"
DEFAULT_SCHEMA = "public"

# Test table for legacy checks
LEGACY_TABLE = "test_users"


# =============================================================================
# Volume Checks (4 tests + negative cases)
# =============================================================================

VOLUME_CHECK_CASES = [
    # (test_id, check_type, params, rule_params, expected_pass, description)
    pytest.param(
        "row_count_pass",
        "row_count",
        {},
        {"error": {"min_count": 10, "max_count": 100}},
        True,
        "20 rows within 10-100 range",
        id="row_count-pass",
    ),
    pytest.param(
        "row_count_fail_min",
        "row_count",
        {},
        {"error": {"min_count": 50}},
        False,
        "20 rows below min_count=50",
        id="row_count-fail-below-min",
    ),
    pytest.param(
        "row_count_fail_max",
        "row_count",
        {},
        {"error": {"max_count": 10}},
        False,
        "20 rows above max_count=10",
        id="row_count-fail-above-max",
    ),
    pytest.param(
        "row_count_change_1_day_fail",
        "row_count_change_1_day",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data - expected to fail",
        id="row_count_change_1_day-no-history",
    ),
    pytest.param(
        "row_count_change_7_days_fail",
        "row_count_change_7_days",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data - expected to fail",
        id="row_count_change_7_days-no-history",
    ),
    pytest.param(
        "row_count_change_30_days_fail",
        "row_count_change_30_days",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data - expected to fail",
        id="row_count_change_30_days-no-history",
    ),
]


class TestVolumeChecks:
    """Volume check tests - row counts and changes."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        VOLUME_CHECK_CASES,
    )
    async def test_volume_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test volume checks with various thresholds."""
        check = await check_factory({
            "name": f"pytest-volume-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, (
            f"{desc}: expected passed={expected_pass}, "
            f"got passed={result.get('passed')}, "
            f"actual_value={result.get('actual_value')}, "
            f"error={result.get('error_message', '')}"
        )


# =============================================================================
# Schema Checks (2 tests + negative cases)
# =============================================================================

SCHEMA_CHECK_CASES = [
    pytest.param(
        "column_count_pass",
        "column_count",
        {},
        {"error": {"min_count": 15, "max_count": 25}},
        True,
        "Table has 19 columns in 15-25 range",
        None,
        id="schema_column_count-pass",
    ),
    pytest.param(
        "column_count_fail",
        "column_count",
        {},
        {"error": {"min_count": 25}},
        False,
        "Table has 19 columns, below min_count=25",
        None,
        id="schema_column_count-fail",
    ),
    pytest.param(
        "column_exists_pass",
        "column_exists",
        {},
        {},
        True,
        "email column exists",
        "email",
        id="schema_column_exists-pass",
    ),
    pytest.param(
        "column_exists_fail",
        "column_exists",
        {},
        {},
        False,
        "nonexistent_column does not exist",
        "nonexistent_column",
        id="schema_column_exists-fail",
    ),
]


class TestSchemaChecks:
    """Schema check tests - column count and existence."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        SCHEMA_CHECK_CASES,
    )
    async def test_schema_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str | None,
    ):
        """Test schema checks."""
        check_data = {
            "name": f"pytest-schema-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if target_column:
            check_data["target_column"] = target_column

        check = await check_factory(check_data)
        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Timeliness Checks (2 tests + negative cases)
# =============================================================================

TIMELINESS_CHECK_CASES = [
    pytest.param(
        "data_freshness_pass",
        "data_freshness",
        {},
        {"error": {"max_value": 86400}},  # 24 hours
        True,
        "Data updated within 24 hours",
        "created_at",
        id="data_freshness-pass",
    ),
    pytest.param(
        "data_freshness_fail",
        "data_freshness",
        {},
        {"error": {"max_value": 1}},  # 1 second - will fail
        False,
        "Data older than 1 second",
        "created_at",
        id="data_freshness-fail-strict-threshold",
    ),
    pytest.param(
        "data_staleness_fail",
        "data_staleness",
        {},
        {"error": {"max_value": 604800}},  # 7 days
        False,
        "Table stats may not be available",
        None,
        id="data_staleness-no-stats",
    ),
]


class TestTimelinessChecks:
    """Timeliness check tests - data freshness and staleness."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        TIMELINESS_CHECK_CASES,
    )
    async def test_timeliness_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str | None,
    ):
        """Test timeliness checks."""
        check_data = {
            "name": f"pytest-timeliness-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if target_column:
            check_data["target_column"] = target_column

        check = await check_factory(check_data)
        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Nulls/Completeness Checks (5 tests + negative cases)
# =============================================================================

NULLS_CHECK_CASES = [
    # nulls_count
    pytest.param(
        "nulls_count_pass",
        "nulls_count",
        {},
        {"error": {"max_count": 5}},
        True,
        "2 nulls <= max_count=5",
        "email",
        id="nulls_count-pass",
    ),
    pytest.param(
        "nulls_count_fail",
        "nulls_count",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 null > max_count=0",
        "email",
        id="nulls_count-fail",
    ),
    # nulls_percent
    pytest.param(
        "nulls_percent_pass",
        "nulls_percent",
        {},
        {"error": {"max_percent": 10.0}},
        True,
        "5% nulls <= max_percent=10%",
        "email",
        id="nulls_percent-pass",
    ),
    pytest.param(
        "nulls_percent_fail",
        "nulls_percent",
        {},
        {"error": {"max_percent": 2.0}},
        False,
        "5% nulls > max_percent=2%",
        "email",
        id="nulls_percent-fail",
    ),
    # not_nulls_count
    pytest.param(
        "not_nulls_count_pass",
        "not_nulls_count",
        {},
        {"error": {"min_count": 15}},
        True,
        "18 non-nulls >= min_count=15",
        "email",
        id="not_nulls_count-pass",
    ),
    pytest.param(
        "not_nulls_count_fail",
        "not_nulls_count",
        {},
        {"error": {"min_count": 20}},
        False,
        "18 non-nulls < min_count=20",
        "email",
        id="not_nulls_count-fail",
    ),
    # not_nulls_percent
    pytest.param(
        "not_nulls_percent_pass",
        "not_nulls_percent",
        {},
        {"error": {"min_percent": 85.0}},
        True,
        "90% non-null >= min_percent=85%",
        "email",
        id="not_nulls_percent-pass",
    ),
    pytest.param(
        "not_nulls_percent_fail",
        "not_nulls_percent",
        {},
        {"error": {"min_percent": 98.0}},
        False,
        "95% non-null < min_percent=98%",
        "email",
        id="not_nulls_percent-fail",
    ),
    # empty_column_found
    pytest.param(
        "empty_column_found_pass",
        "empty_column_found",
        {},
        {"error": {"max_percent": 99.0}},
        True,
        "Column is not empty",
        "email",
        id="empty_column_found-pass",
    ),
]


class TestNullsChecks:
    """Nulls/completeness check tests."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        NULLS_CHECK_CASES,
    )
    async def test_nulls_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test null/completeness checks."""
        check = await check_factory({
            "name": f"pytest-nulls-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Uniqueness Checks (6 tests + negative cases)
# =============================================================================

UNIQUENESS_CHECK_CASES = [
    # distinct_count
    pytest.param(
        "distinct_count_pass",
        "distinct_count",
        {},
        {"error": {"min_count": 2, "max_count": 5}},
        True,
        "is_active has 3 distinct values",
        "is_active",
        None,
        id="distinct_count-pass",
    ),
    pytest.param(
        "distinct_count_fail",
        "distinct_count",
        {},
        {"error": {"min_count": 10}},
        False,
        "is_active has only 3 distinct values",
        "is_active",
        None,
        id="distinct_count-fail",
    ),
    # distinct_percent
    pytest.param(
        "distinct_percent_pass",
        "distinct_percent",
        {},
        {"error": {"min_percent": 95.0}},
        True,
        "ID is unique (100%)",
        "id",
        None,
        id="distinct_percent-pass",
    ),
    pytest.param(
        "distinct_percent_fail",
        "distinct_percent",
        {},
        {"error": {"min_percent": 50.0}},
        False,
        "is_active has low distinct percent",
        "is_active",
        None,
        id="distinct_percent-fail",
    ),
    # duplicate_count
    pytest.param(
        "duplicate_count_pass",
        "duplicate_count",
        {},
        {"error": {"max_count": 5}},
        True,
        "1-2 duplicate emails <= max_count=5",
        "email",
        None,
        id="duplicate_count-pass",
    ),
    pytest.param(
        "duplicate_count_fail",
        "duplicate_count",
        {},
        {"error": {"max_count": 0}},
        False,
        "1-2 duplicate emails > max_count=0",
        "email",
        None,
        id="duplicate_count-fail",
    ),
    # duplicate_percent
    pytest.param(
        "duplicate_percent_pass",
        "duplicate_percent",
        {},
        {"error": {"max_percent": 95.0}},
        True,
        "is_active has many duplicates but under 95%",
        "is_active",
        None,
        id="duplicate_percent-pass",
    ),
    # duplicate_record_count (table-level)
    pytest.param(
        "duplicate_record_count_pass",
        "duplicate_record_count",
        {"column_list": ["email", "latitude", "longitude"]},
        {"error": {"max_count": 5}},
        True,
        "1 duplicate record combination",
        None,
        None,
        id="duplicate_record_count-pass",
    ),
    pytest.param(
        "duplicate_record_count_pass_strict",
        "duplicate_record_count",
        {},
        {"error": {"max_count": 0}},
        True,
        "No row-level duplicates (ctid unique)",
        None,
        None,
        id="duplicate_record_count-strict",
    ),
    # duplicate_record_percent (table-level)
    pytest.param(
        "duplicate_record_percent_pass",
        "duplicate_record_percent",
        {"column_list": ["email", "latitude", "longitude"]},
        {"error": {"max_percent": 20.0}},
        True,
        "~5% duplicate records",
        None,
        None,
        id="duplicate_record_percent-pass",
    ),
]


class TestUniquenessChecks:
    """Uniqueness check tests - distinct counts and duplicates."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column,_unused",
        UNIQUENESS_CHECK_CASES,
    )
    async def test_uniqueness_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str | None,
        _unused: Any,
    ):
        """Test uniqueness checks."""
        check_data = {
            "name": f"pytest-uniqueness-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if target_column:
            check_data["target_column"] = target_column

        check = await check_factory(check_data)
        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Numeric/Statistical Checks (8 tests + negative cases)
# =============================================================================

NUMERIC_CHECK_CASES = [
    # min_in_range
    pytest.param(
        "min_in_range_pass",
        "min_in_range",
        {},
        {"error": {"min_value": 0, "max_value": 10}},
        True,
        "min score is 0",
        "score",
        id="min_in_range-pass",
    ),
    pytest.param(
        "min_in_range_fail",
        "min_in_range",
        {},
        {"error": {"min_value": 10, "max_value": 20}},
        False,
        "min score 0 not in 10-20",
        "score",
        id="min_in_range-fail",
    ),
    # max_in_range
    pytest.param(
        "max_in_range_pass",
        "max_in_range",
        {},
        {"error": {"min_value": 90, "max_value": 110}},
        True,
        "max score is 100",
        "score",
        id="max_in_range-pass",
    ),
    pytest.param(
        "max_in_range_fail",
        "max_in_range",
        {},
        {"error": {"min_value": 50, "max_value": 80}},
        False,
        "max score 100 not in 50-80",
        "score",
        id="max_in_range-fail",
    ),
    # sum_in_range
    pytest.param(
        "sum_in_range_pass",
        "sum_in_range",
        {},
        {"error": {"min_value": 1000, "max_value": 2000}},
        True,
        "sum ~1450 in range",
        "score",
        id="sum_in_range-pass",
    ),
    pytest.param(
        "sum_in_range_fail",
        "sum_in_range",
        {},
        {"error": {"min_value": 2000, "max_value": 3000}},
        False,
        "sum ~1450 not in 2000-3000",
        "score",
        id="sum_in_range-fail",
    ),
    # mean_in_range
    pytest.param(
        "mean_in_range_pass",
        "mean_in_range",
        {},
        {"error": {"min_value": 60, "max_value": 80}},
        True,
        "mean ~72.5 in range",
        "score",
        id="mean_in_range-pass",
    ),
    pytest.param(
        "mean_in_range_fail",
        "mean_in_range",
        {},
        {"error": {"min_value": 80, "max_value": 100}},
        False,
        "mean ~72.5 not in 80-100",
        "score",
        id="mean_in_range-fail",
    ),
    # median_in_range
    pytest.param(
        "median_in_range_pass",
        "median_in_range",
        {},
        {"error": {"min_value": 65, "max_value": 85}},
        True,
        "median ~75 in range",
        "score",
        id="median_in_range-pass",
    ),
    pytest.param(
        "median_in_range_fail",
        "median_in_range",
        {},
        {"error": {"min_value": 0, "max_value": 50}},
        False,
        "median ~75 not in 0-50",
        "score",
        id="median_in_range-fail",
    ),
    # number_below_min_value
    pytest.param(
        "number_below_min_value_pass",
        "number_below_min_value",
        {},
        {"error": {"min_value": 0}},
        True,
        "no values below 0",
        "score",
        id="number_below_min_value-pass",
    ),
    # number_above_max_value
    pytest.param(
        "number_above_max_value_pass",
        "number_above_max_value",
        {},
        {"error": {"max_value": 100}},
        True,
        "no values above 100",
        "score",
        id="number_above_max_value-pass",
    ),
    # number_in_range_percent
    pytest.param(
        "number_in_range_percent_pass",
        "number_in_range_percent",
        {"min_value": 0, "max_value": 100},
        {"error": {"min_percent": 95.0}},
        True,
        "all scores in 0-100 range",
        "score",
        id="number_in_range_percent-pass",
    ),
    pytest.param(
        "number_in_range_percent_fail",
        "number_in_range_percent",
        {"min_value": 50, "max_value": 100},
        {"error": {"min_percent": 95.0}},
        False,
        "some scores below 50",
        "score",
        id="number_in_range_percent-fail",
    ),
]


class TestNumericChecks:
    """Numeric/statistical check tests."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        NUMERIC_CHECK_CASES,
    )
    async def test_numeric_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test numeric/statistical checks."""
        check = await check_factory({
            "name": f"pytest-numeric-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Text Checks (9 tests + negative cases)
# =============================================================================

TEXT_CHECK_CASES = [
    # text_min_length
    pytest.param(
        "text_min_length_pass",
        "text_min_length",
        {},
        {"error": {"min_value": 1, "max_value": 5}},
        True,
        "min length is 1 char",
        "short_code",
        id="text_min_length-pass",
    ),
    pytest.param(
        "text_min_length_fail",
        "text_min_length",
        {},
        {"error": {"min_value": 3, "max_value": 5}},
        False,
        "min length 1 < expected 3",
        "short_code",
        id="text_min_length-fail",
    ),
    # text_max_length
    pytest.param(
        "text_max_length_pass",
        "text_max_length",
        {},
        {"error": {"min_value": 5, "max_value": 15}},
        True,
        "max length is 11 chars",
        "short_code",
        id="text_max_length-pass",
    ),
    pytest.param(
        "text_max_length_fail",
        "text_max_length",
        {},
        {"error": {"min_value": 1, "max_value": 5}},
        False,
        "max length 11 > expected 5",
        "short_code",
        id="text_max_length-fail",
    ),
    # text_mean_length
    pytest.param(
        "text_mean_length_pass",
        "text_mean_length",
        {},
        {"error": {"min_value": 3, "max_value": 8}},
        True,
        "mean length ~6 chars",
        "short_code",
        id="text_mean_length-pass",
    ),
    # text_length_below_min_length
    pytest.param(
        "text_length_below_min_length_pass",
        "text_length_below_min_length",
        {"min_length": 3},
        {"error": {"max_count": 3}},
        True,
        "1 value below 3 chars",
        "short_code",
        id="text_length_below_min_length-pass",
    ),
    pytest.param(
        "text_length_below_min_length_fail",
        "text_length_below_min_length",
        {"min_length": 3},
        {"error": {"max_count": 0}},
        False,
        "1 value below 3 chars > max_count=0",
        "short_code",
        id="text_length_below_min_length-fail",
    ),
    # text_length_above_max_length
    pytest.param(
        "text_length_above_max_length_pass",
        "text_length_above_max_length",
        {"max_length": 10},
        {"error": {"max_count": 3}},
        True,
        "1 value above 10 chars",
        "short_code",
        id="text_length_above_max_length-pass",
    ),
    # text_length_in_range_percent
    pytest.param(
        "text_length_in_range_percent_pass",
        "text_length_in_range_percent",
        {"min_length": 1, "max_length": 15},
        {"error": {"min_percent": 95.0}},
        True,
        "All short_code values have length 1-15",
        "short_code",
        id="text_length_in_range_percent-pass",
    ),
    # empty_text_found
    pytest.param(
        "empty_text_found_pass",
        "empty_text_found",
        {},
        {"error": {"max_count": 3}},
        True,
        "1 empty string in description",
        "description",
        id="empty_text_found-pass",
    ),
    pytest.param(
        "empty_text_found_fail",
        "empty_text_found",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 empty string > max_count=0",
        "description",
        id="empty_text_found-fail",
    ),
    # whitespace_text_found
    pytest.param(
        "whitespace_text_found_pass",
        "whitespace_text_found",
        {},
        {"error": {"max_count": 3}},
        True,
        "1 whitespace-only value",
        "description",
        id="whitespace_text_found-pass",
    ),
    # text_not_matching_regex_found
    pytest.param(
        "text_not_matching_regex_found_pass",
        "text_not_matching_regex_found",
        {"regex_pattern": "^[A-Z0-9]+$"},
        {"error": {"max_count": 5}},
        True,
        "all codes are uppercase alphanumeric",
        "short_code",
        id="text_not_matching_regex_found-pass",
    ),
]


class TestTextChecks:
    """Text check tests - lengths, patterns, whitespace."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        TEXT_CHECK_CASES,
    )
    async def test_text_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test text checks."""
        check = await check_factory({
            "name": f"pytest-text-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Pattern/Format Checks (12 tests + negative cases)
# =============================================================================

PATTERN_CHECK_CASES = [
    # invalid_email_format_found
    pytest.param(
        "invalid_email_format_found_pass",
        "invalid_email_format_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "1 invalid email <= max_count=5",
        "email",
        id="invalid_email_format_found-pass",
    ),
    pytest.param(
        "invalid_email_format_found_fail",
        "invalid_email_format_found",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid email > max_count=0",
        "email",
        id="invalid_email_format_found-fail",
    ),
    # invalid_email_format_percent
    pytest.param(
        "invalid_email_format_percent_pass",
        "invalid_email_format_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% invalid emails",
        "email",
        id="invalid_email_format_percent-pass",
    ),
    # invalid_uuid_format_found
    pytest.param(
        "invalid_uuid_format_found_pass",
        "invalid_uuid_format_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "1 invalid UUID",
        "uuid_col",
        id="invalid_uuid_format_found-pass",
    ),
    pytest.param(
        "invalid_uuid_format_found_fail",
        "invalid_uuid_format_found",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid UUID > max_count=0",
        "uuid_col",
        id="invalid_uuid_format_found-fail",
    ),
    # invalid_uuid_format_percent
    pytest.param(
        "invalid_uuid_format_percent_pass",
        "invalid_uuid_format_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% invalid UUIDs",
        "uuid_col",
        id="invalid_uuid_format_percent-pass",
    ),
    # invalid_ip4_format_found
    pytest.param(
        "invalid_ip4_format_found_pass",
        "invalid_ip4_format_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "1 invalid IPv4",
        "ip4_address",
        id="invalid_ip4_format_found-pass",
    ),
    pytest.param(
        "invalid_ip4_format_found_fail",
        "invalid_ip4_format_found",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid IPv4 > max_count=0",
        "ip4_address",
        id="invalid_ip4_format_found-fail",
    ),
    # invalid_ip4_format_percent
    pytest.param(
        "invalid_ip4_format_percent_pass",
        "invalid_ip4_format_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% invalid IPv4",
        "ip4_address",
        id="invalid_ip4_format_percent-pass",
    ),
    # invalid_ip6_format_found
    pytest.param(
        "invalid_ip6_format_found_pass",
        "invalid_ip6_format_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "1-3 invalid IPv6",
        "ip6_address",
        id="invalid_ip6_format_found-pass",
    ),
    # invalid_ip6_format_percent
    pytest.param(
        "invalid_ip6_format_percent_pass",
        "invalid_ip6_format_percent",
        {},
        {"error": {"max_percent": 20.0}},
        True,
        "~15% invalid IPv6",
        "ip6_address",
        id="invalid_ip6_format_percent-pass",
    ),
    # invalid_usa_phone_format_found
    pytest.param(
        "invalid_usa_phone_format_found_pass",
        "invalid_usa_phone_format_found",
        {},
        {"error": {"max_count": 15}},
        True,
        "invalid phones within threshold",
        "phone",
        id="invalid_usa_phone_format_found-pass",
    ),
    # invalid_usa_phone_format_percent
    pytest.param(
        "invalid_usa_phone_format_percent_pass",
        "invalid_usa_phone_format_percent",
        {},
        {"error": {"max_percent": 20.0}},
        True,
        "~15% invalid phones",
        "phone",
        id="invalid_usa_phone_format_percent-pass",
    ),
    # invalid_usa_zipcode_format_found
    pytest.param(
        "invalid_usa_zipcode_format_found_pass",
        "invalid_usa_zipcode_format_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "1 invalid zipcode",
        "zipcode",
        id="invalid_usa_zipcode_format_found-pass",
    ),
    pytest.param(
        "invalid_usa_zipcode_format_found_fail",
        "invalid_usa_zipcode_format_found",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid zipcode > max_count=0",
        "zipcode",
        id="invalid_usa_zipcode_format_found-fail",
    ),
    # invalid_usa_zipcode_format_percent
    pytest.param(
        "invalid_usa_zipcode_format_percent_pass",
        "invalid_usa_zipcode_format_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% invalid zipcodes",
        "zipcode",
        id="invalid_usa_zipcode_format_percent-pass",
    ),
]


class TestPatternChecks:
    """Pattern/format check tests - email, UUID, IP, phone, zipcode."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        PATTERN_CHECK_CASES,
    )
    async def test_pattern_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test pattern/format checks."""
        check = await check_factory({
            "name": f"pytest-pattern-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Geographic Checks (2 tests + negative cases)
# =============================================================================

GEOGRAPHIC_CHECK_CASES = [
    # invalid_latitude
    pytest.param(
        "invalid_latitude_pass",
        "invalid_latitude",
        {},
        {"error": {"max_count": 3}},
        True,
        "1 invalid latitude (95.0)",
        "latitude",
        id="invalid_latitude-pass",
    ),
    pytest.param(
        "invalid_latitude_fail",
        "invalid_latitude",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid latitude > max_count=0",
        "latitude",
        id="invalid_latitude-fail",
    ),
    # invalid_longitude
    pytest.param(
        "invalid_longitude_pass",
        "invalid_longitude",
        {},
        {"error": {"max_count": 3}},
        True,
        "1 invalid longitude (-200.0)",
        "longitude",
        id="invalid_longitude-pass",
    ),
    pytest.param(
        "invalid_longitude_fail",
        "invalid_longitude",
        {},
        {"error": {"max_count": 0}},
        False,
        "1 invalid longitude > max_count=0",
        "longitude",
        id="invalid_longitude-fail",
    ),
]


class TestGeographicChecks:
    """Geographic check tests - latitude/longitude validation."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        GEOGRAPHIC_CHECK_CASES,
    )
    async def test_geographic_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test geographic checks."""
        check = await check_factory({
            "name": f"pytest-geo-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Boolean Checks (2 tests + negative cases)
# =============================================================================

BOOLEAN_CHECK_CASES = [
    # true_percent
    pytest.param(
        "true_percent_pass",
        "true_percent",
        {},
        {"error": {"min_percent": 40.0, "max_percent": 70.0}},
        True,
        "~50% true",
        "is_active",
        id="true_percent-pass",
    ),
    pytest.param(
        "true_percent_fail",
        "true_percent",
        {},
        {"error": {"min_percent": 80.0}},
        False,
        "~50% true < min_percent=80%",
        "is_active",
        id="true_percent-fail",
    ),
    # false_percent
    pytest.param(
        "false_percent_pass",
        "false_percent",
        {},
        {"error": {"min_percent": 30.0, "max_percent": 60.0}},
        True,
        "~40% false",
        "is_active",
        id="false_percent-pass",
    ),
    pytest.param(
        "false_percent_fail",
        "false_percent",
        {},
        {"error": {"min_percent": 80.0}},
        False,
        "~40% false < min_percent=80%",
        "is_active",
        id="false_percent-fail",
    ),
]


class TestBooleanChecks:
    """Boolean check tests - true/false percentages."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        BOOLEAN_CHECK_CASES,
    )
    async def test_boolean_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test boolean checks."""
        check = await check_factory({
            "name": f"pytest-bool-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# DateTime Checks (2 tests + negative cases)
# =============================================================================

DATETIME_CHECK_CASES = [
    # date_values_in_future_percent
    pytest.param(
        "date_values_in_future_percent_pass",
        "date_values_in_future_percent",
        {},
        {"error": {"max_percent": 25.0}},
        True,
        "15% future dates",
        "event_date",
        id="date_values_in_future_percent-pass",
    ),
    pytest.param(
        "date_values_in_future_percent_fail",
        "date_values_in_future_percent",
        {},
        {"error": {"max_percent": 5.0}},
        False,
        "15% future dates > max_percent=5%",
        "event_date",
        id="date_values_in_future_percent-fail",
    ),
    # date_in_range_percent
    pytest.param(
        "date_in_range_percent_pass",
        "date_in_range_percent",
        {"min_date": "2020-01-01", "max_date": "2030-12-31"},
        {"error": {"min_percent": 95.0}},
        True,
        "all dates in range",
        "event_date",
        id="date_in_range_percent-pass",
    ),
    pytest.param(
        "date_in_range_percent_fail",
        "date_in_range_percent",
        {"min_date": "2024-01-01", "max_date": "2024-12-31"},
        {"error": {"min_percent": 95.0}},
        False,
        "most dates not in 2024",
        "event_date",
        id="date_in_range_percent-fail",
    ),
]


class TestDateTimeChecks:
    """DateTime check tests - future dates, date ranges."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        DATETIME_CHECK_CASES,
    )
    async def test_datetime_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test datetime checks."""
        check = await check_factory({
            "name": f"pytest-datetime-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Referential Integrity Checks (2 tests + negative cases)
# =============================================================================

REFERENTIAL_CHECK_CASES = [
    # foreign_key_not_found
    pytest.param(
        "foreign_key_not_found_pass",
        "foreign_key_not_found",
        {
            "reference_table": "test_categories",
            "reference_column": "id",
            "reference_schema": "public",
        },
        {"error": {"max_count": 5}},
        True,
        "2 invalid FKs (99, 100)",
        "category_id",
        id="foreign_key_not_found-pass",
    ),
    pytest.param(
        "foreign_key_not_found_fail",
        "foreign_key_not_found",
        {
            "reference_table": "test_categories",
            "reference_column": "id",
            "reference_schema": "public",
        },
        {"error": {"max_count": 0}},
        False,
        "2 invalid FKs > max_count=0",
        "category_id",
        id="foreign_key_not_found-fail",
    ),
    # foreign_key_found_percent
    pytest.param(
        "foreign_key_found_percent_pass",
        "foreign_key_found_percent",
        {
            "reference_table": "test_categories",
            "reference_column": "id",
            "reference_schema": "public",
        },
        {"error": {"min_percent": 85.0}},
        True,
        "90% valid FKs",
        "category_id",
        id="foreign_key_found_percent-pass",
    ),
    pytest.param(
        "foreign_key_found_percent_fail",
        "foreign_key_found_percent",
        {
            "reference_table": "test_categories",
            "reference_column": "id",
            "reference_schema": "public",
        },
        {"error": {"min_percent": 95.0}},
        False,
        "90% valid FKs < min_percent=95%",
        "category_id",
        id="foreign_key_found_percent-fail",
    ),
]


class TestReferentialChecks:
    """Referential integrity check tests - foreign key validation."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        REFERENTIAL_CHECK_CASES,
    )
    async def test_referential_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test referential integrity checks."""
        check = await check_factory({
            "name": f"pytest-ref-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Custom SQL Checks (2 tests + negative cases)
# =============================================================================

CUSTOM_SQL_CHECK_CASES = [
    # sql_condition_failed_on_table
    pytest.param(
        "sql_condition_failed_on_table_pass",
        "sql_condition_failed_on_table",
        {"sql_condition": "score >= 0 AND score <= 100"},
        {"error": {"max_count": 0}},
        True,
        "all scores in valid range",
        id="sql_condition_failed_on_table-pass",
    ),
    pytest.param(
        "sql_condition_failed_on_table_fail",
        "sql_condition_failed_on_table",
        {"sql_condition": "score > 50"},
        {"error": {"max_count": 0}},
        False,
        "some scores <= 50",
        id="sql_condition_failed_on_table-fail",
    ),
    # sql_aggregate_expression_on_table
    pytest.param(
        "sql_aggregate_expression_on_table_pass",
        "sql_aggregate_expression_on_table",
        {"sql_expression": "AVG(score)"},
        {"error": {"min_value": 60.0, "max_value": 85.0}},
        True,
        "avg score ~72.5",
        id="sql_aggregate_expression_on_table-pass",
    ),
    pytest.param(
        "sql_aggregate_expression_on_table_fail",
        "sql_aggregate_expression_on_table",
        {"sql_expression": "AVG(score)"},
        {"error": {"min_value": 80.0, "max_value": 100.0}},
        False,
        "avg score ~72.5 not in 80-100",
        id="sql_aggregate_expression_on_table-fail",
    ),
]


class TestCustomSQLChecks:
    """Custom SQL check tests - arbitrary SQL conditions and aggregates."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        CUSTOM_SQL_CHECK_CASES,
    )
    async def test_custom_sql_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test custom SQL checks."""
        check = await check_factory({
            "name": f"pytest-sql-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Legacy Check Tests (backward compatibility)
# =============================================================================

LEGACY_CHECK_CASES = [
    pytest.param(
        "row_count_min_pass",
        "row_count_min",
        {"min_value": 1},
        {},
        True,
        "at least 1 row",
        None,
        LEGACY_TABLE,
        id="legacy-row_count_min-pass",
    ),
    pytest.param(
        "row_count_max_pass",
        "row_count_max",
        {"max_value": 100},
        {},
        True,
        "less than 100 rows",
        None,
        LEGACY_TABLE,
        id="legacy-row_count_max-pass",
    ),
    pytest.param(
        "not_null_pass",
        "not_null",
        {},
        {},
        True,
        "name column has no nulls",
        "name",
        LEGACY_TABLE,
        id="legacy-not_null-pass",
    ),
    pytest.param(
        "unique_pass",
        "unique",
        {},
        {},
        True,
        "id column is unique",
        "id",
        LEGACY_TABLE,
        id="legacy-unique-pass",
    ),
    pytest.param(
        "value_range_pass",
        "value_range",
        {"min_value": 1, "max_value": 100},
        {},
        True,
        "id values in range",
        "id",
        LEGACY_TABLE,
        id="legacy-value_range-pass",
    ),
    pytest.param(
        "allowed_values_pass",
        "allowed_values",
        {"allowed_values": ["active", "inactive", "pending"]},
        {},
        True,
        "status values in allowed set",
        "status",
        LEGACY_TABLE,
        id="legacy-allowed_values-pass",
    ),
]


class TestLegacyChecks:
    """Legacy check tests for backward compatibility."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column,target_table",
        LEGACY_CHECK_CASES,
    )
    async def test_legacy_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str | None,
        target_table: str,
    ):
        """Test legacy check types."""
        check_data = {
            "name": f"pytest-legacy-{test_id}",
            "check_type": check_type,
            "target_table": target_table,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
        }
        if rule_params:
            check_data["rule_parameters"] = rule_params
        if target_column:
            check_data["target_column"] = target_column

        check = await check_factory(check_data)
        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Check Preview Tests
# =============================================================================


class TestCheckPreview:
    """Test check preview endpoint (dry run without saving)."""

    @pytest.mark.asyncio
    async def test_preview_row_count(self, api_client: httpx.AsyncClient, connection_id: str):
        """Test preview of row count check."""
        preview_data = {
            "connection_id": connection_id,
            "check_type": "row_count",
            "target_schema": DEFAULT_SCHEMA,
            "target_table": DEFAULT_TABLE,
            "parameters": {},
            "rule_parameters": {"error": {"min_count": 1}},
        }

        response = await api_client.post("/checks/validate/preview", json=preview_data)

        assert response.status_code == 200
        result = response.json()
        assert result.get("passed") is True
        assert result.get("sensor_value") is not None

    @pytest.mark.asyncio
    async def test_preview_nulls_percent(self, api_client: httpx.AsyncClient, connection_id: str):
        """Test preview of nulls percent check."""
        preview_data = {
            "connection_id": connection_id,
            "check_type": "nulls_percent",
            "target_schema": DEFAULT_SCHEMA,
            "target_table": DEFAULT_TABLE,
            "target_column": "email",
            "parameters": {},
            "rule_parameters": {"error": {"max_percent": 50.0}},
        }

        response = await api_client.post("/checks/validate/preview", json=preview_data)

        assert response.status_code == 200
        result = response.json()
        assert result.get("passed") is True
        assert result.get("sensor_value") is not None


# =============================================================================
# Metadata Endpoint Tests
# =============================================================================


class TestMetadataEndpoints:
    """Test check metadata endpoints."""

    @pytest.mark.asyncio
    async def test_get_check_types(self, api_client: httpx.AsyncClient):
        """Test getting available check types."""
        response = await api_client.get("/checks/types")

        assert response.status_code == 200
        types = response.json()
        assert len(types) > 0

    @pytest.mark.asyncio
    async def test_get_check_categories(self, api_client: httpx.AsyncClient):
        """Test getting check categories."""
        response = await api_client.get("/checks/categories")

        assert response.status_code == 200
        categories = response.json()
        assert len(categories) > 0

    @pytest.mark.asyncio
    async def test_get_check_modes(self, api_client: httpx.AsyncClient):
        """Test getting check modes."""
        response = await api_client.get("/checks/modes")

        assert response.status_code == 200
        modes = response.json()
        assert "profiling" in modes or "monitoring" in modes

    @pytest.mark.asyncio
    async def test_get_time_scales(self, api_client: httpx.AsyncClient):
        """Test getting time scales."""
        response = await api_client.get("/checks/time-scales")

        assert response.status_code == 200
        scales = response.json()
        assert len(scales) > 0


# =============================================================================
# Phase 1: Whitespace & Text Checks (NEW)
# =============================================================================

WHITESPACE_TEXT_CHECK_CASES = [
    # empty_text_percent
    pytest.param(
        "empty_text_percent_pass",
        "empty_text_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% empty strings (1/20)",
        "description",
        id="empty_text_percent-pass",
    ),
    pytest.param(
        "empty_text_percent_fail",
        "empty_text_percent",
        {},
        {"error": {"max_percent": 1.0}},
        False,
        "~5% empty strings > max_percent=1%",
        "description",
        id="empty_text_percent-fail",
    ),
    # whitespace_text_percent
    pytest.param(
        "whitespace_text_percent_pass",
        "whitespace_text_percent",
        {},
        {"error": {"max_percent": 15.0}},
        True,
        "~5% whitespace-only (1/20)",
        "description",
        id="whitespace_text_percent-pass",
    ),
    pytest.param(
        "whitespace_text_percent_fail",
        "whitespace_text_percent",
        {},
        {"error": {"max_percent": 1.0}},
        False,
        "~5% whitespace > max_percent=1%",
        "description",
        id="whitespace_text_percent-fail",
    ),
    # null_placeholder_text_found
    pytest.param(
        "null_placeholder_text_found_pass",
        "null_placeholder_text_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "no null placeholder text in email",
        "email",
        id="null_placeholder_text_found-pass",
    ),
    # null_placeholder_text_percent
    pytest.param(
        "null_placeholder_text_percent_pass",
        "null_placeholder_text_percent",
        {},
        {"error": {"max_percent": 10.0}},
        True,
        "no null placeholder text",
        "email",
        id="null_placeholder_text_percent-pass",
    ),
    # text_surrounded_by_whitespace_found
    pytest.param(
        "text_surrounded_by_whitespace_found_pass",
        "text_surrounded_by_whitespace_found",
        {},
        {"error": {"max_count": 5}},
        True,
        "no leading/trailing whitespace",
        "short_code",
        id="text_surrounded_by_whitespace_found-pass",
    ),
    # text_surrounded_by_whitespace_percent
    pytest.param(
        "text_surrounded_by_whitespace_percent_pass",
        "text_surrounded_by_whitespace_percent",
        {},
        {"error": {"max_percent": 10.0}},
        True,
        "no surrounded whitespace",
        "short_code",
        id="text_surrounded_by_whitespace_percent-pass",
    ),
    # texts_not_matching_regex_percent
    pytest.param(
        "texts_not_matching_regex_percent_pass",
        "texts_not_matching_regex_percent",
        {"regex_pattern": "^[A-Za-z0-9]+$"},
        {"error": {"max_percent": 50.0}},
        True,
        "most codes are alphanumeric",
        "short_code",
        id="texts_not_matching_regex_percent-pass",
    ),
    # text_matching_regex_percent
    pytest.param(
        "text_matching_regex_percent_pass",
        "text_matching_regex_percent",
        {"regex_pattern": ".*@.*"},
        {"error": {"min_percent": 80.0}},
        True,
        "most emails contain @",
        "email",
        id="text_matching_regex_percent-pass",
    ),
    # min_word_count
    pytest.param(
        "min_word_count_pass",
        "min_word_count",
        {},
        {"error": {"min_value": 1, "max_value": 5}},
        True,
        "min words in description >= 1",
        "description",
        id="min_word_count-pass",
    ),
    # max_word_count
    pytest.param(
        "max_word_count_pass",
        "max_word_count",
        {},
        {"error": {"min_value": 3, "max_value": 15}},
        True,
        "max words in description reasonable",
        "description",
        id="max_word_count-pass",
    ),
]


class TestWhitespaceTextChecks:
    """Phase 1: Whitespace and text checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        WHITESPACE_TEXT_CHECK_CASES,
    )
    async def test_whitespace_text_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test whitespace and text checks."""
        check = await check_factory({
            "name": f"pytest-whitespace-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 2: Geographic & Numeric Percent Variants (NEW)
# =============================================================================

GEO_NUMERIC_PERCENT_CHECK_CASES = [
    # valid_latitude_percent
    pytest.param(
        "valid_latitude_percent_pass",
        "valid_latitude_percent",
        {},
        {"error": {"min_percent": 85.0}},
        True,
        "~90% valid latitudes",
        "latitude",
        id="valid_latitude_percent-pass",
    ),
    pytest.param(
        "valid_latitude_percent_fail",
        "valid_latitude_percent",
        {},
        {"error": {"min_percent": 99.0}},
        False,
        "~90% valid < min_percent=99%",
        "latitude",
        id="valid_latitude_percent-fail",
    ),
    # valid_longitude_percent
    pytest.param(
        "valid_longitude_percent_pass",
        "valid_longitude_percent",
        {},
        {"error": {"min_percent": 85.0}},
        True,
        "~90% valid longitudes",
        "longitude",
        id="valid_longitude_percent-pass",
    ),
    pytest.param(
        "valid_longitude_percent_fail",
        "valid_longitude_percent",
        {},
        {"error": {"min_percent": 99.0}},
        False,
        "~90% valid < min_percent=99%",
        "longitude",
        id="valid_longitude_percent-fail",
    ),
    # number_below_min_value_percent
    pytest.param(
        "number_below_min_value_percent_pass",
        "number_below_min_value_percent",
        {"min_value": 0},
        {"error": {"max_percent": 5.0}},
        True,
        "no scores below 0",
        "score",
        id="number_below_min_value_percent-pass",
    ),
    # number_above_max_value_percent
    pytest.param(
        "number_above_max_value_percent_pass",
        "number_above_max_value_percent",
        {"max_value": 100},
        {"error": {"max_percent": 5.0}},
        True,
        "no scores above 100",
        "score",
        id="number_above_max_value_percent-pass",
    ),
    # negative_values
    pytest.param(
        "negative_values_pass",
        "negative_values",
        {},
        {"error": {"max_count": 0}},
        True,
        "no negative scores",
        "score",
        id="negative_values-pass",
    ),
    # negative_values_percent
    pytest.param(
        "negative_values_percent_pass",
        "negative_values_percent",
        {},
        {"error": {"max_percent": 5.0}},
        True,
        "0% negative scores",
        "score",
        id="negative_values_percent-pass",
    ),
    # non_negative_values
    pytest.param(
        "non_negative_values_pass",
        "non_negative_values",
        {},
        {"error": {"min_count": 15}},
        True,
        "all 20 scores non-negative",
        "score",
        id="non_negative_values-pass",
    ),
    # non_negative_values_percent
    pytest.param(
        "non_negative_values_percent_pass",
        "non_negative_values_percent",
        {},
        {"error": {"min_percent": 95.0}},
        True,
        "100% non-negative",
        "score",
        id="non_negative_values_percent-pass",
    ),
]


class TestGeoNumericPercentChecks:
    """Phase 2: Geographic and numeric percent checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        GEO_NUMERIC_PERCENT_CHECK_CASES,
    )
    async def test_geo_numeric_percent_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test geographic and numeric percent checks."""
        check = await check_factory({
            "name": f"pytest-geonumeric-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 3: Statistical & Percentile Checks (NEW)
# =============================================================================

STATISTICAL_CHECK_CASES = [
    # integer_in_range_percent
    pytest.param(
        "integer_in_range_percent_pass",
        "integer_in_range_percent",
        {"min_value": 0, "max_value": 100},
        {"error": {"min_percent": 95.0}},
        True,
        "all integer scores in 0-100",
        "score",
        id="integer_in_range_percent-pass",
    ),
    # sample_stddev_in_range
    pytest.param(
        "sample_stddev_in_range_pass",
        "sample_stddev_in_range",
        {},
        {"error": {"min_value": 10, "max_value": 40}},
        True,
        "score stddev reasonable",
        "score",
        id="sample_stddev_in_range-pass",
    ),
    # population_stddev_in_range
    pytest.param(
        "population_stddev_in_range_pass",
        "population_stddev_in_range",
        {},
        {"error": {"min_value": 10, "max_value": 40}},
        True,
        "population stddev reasonable",
        "score",
        id="population_stddev_in_range-pass",
    ),
    # sample_variance_in_range
    pytest.param(
        "sample_variance_in_range_pass",
        "sample_variance_in_range",
        {},
        {"error": {"min_value": 100, "max_value": 2000}},
        True,
        "sample variance reasonable",
        "score",
        id="sample_variance_in_range-pass",
    ),
    # population_variance_in_range
    pytest.param(
        "population_variance_in_range_pass",
        "population_variance_in_range",
        {},
        {"error": {"min_value": 100, "max_value": 2000}},
        True,
        "population variance reasonable",
        "score",
        id="population_variance_in_range-pass",
    ),
    # percentile_in_range (custom percentile)
    pytest.param(
        "percentile_in_range_pass",
        "percentile_in_range",
        {"percentile": 0.5},
        {"error": {"min_value": 50, "max_value": 85}},
        True,
        "50th percentile ~75",
        "score",
        id="percentile_in_range-pass",
    ),
    # percentile_10_in_range
    pytest.param(
        "percentile_10_in_range_pass",
        "percentile_10_in_range",
        {},
        {"error": {"min_value": 0, "max_value": 60}},
        True,
        "10th percentile low",
        "score",
        id="percentile_10_in_range-pass",
    ),
    # percentile_25_in_range
    pytest.param(
        "percentile_25_in_range_pass",
        "percentile_25_in_range",
        {},
        {"error": {"min_value": 40, "max_value": 75}},
        True,
        "25th percentile in range",
        "score",
        id="percentile_25_in_range-pass",
    ),
    # percentile_75_in_range
    pytest.param(
        "percentile_75_in_range_pass",
        "percentile_75_in_range",
        {},
        {"error": {"min_value": 75, "max_value": 95}},
        True,
        "75th percentile high",
        "score",
        id="percentile_75_in_range-pass",
    ),
    # percentile_90_in_range
    pytest.param(
        "percentile_90_in_range_pass",
        "percentile_90_in_range",
        {},
        {"error": {"min_value": 85, "max_value": 100}},
        True,
        "90th percentile near max",
        "score",
        id="percentile_90_in_range-pass",
    ),
]


class TestStatisticalChecks:
    """Phase 3: Statistical and percentile checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        STATISTICAL_CHECK_CASES,
    )
    async def test_statistical_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test statistical and percentile checks."""
        check = await check_factory({
            "name": f"pytest-statistical-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 4: Accepted Values & Domain Checks (NEW)
# =============================================================================

ACCEPTED_VALUES_CHECK_CASES = [
    # text_found_in_set_percent
    pytest.param(
        "text_found_in_set_percent_pass",
        "text_found_in_set_percent",
        {"expected_values": ["ABC123", "XYZ789", "DEF456", "GHI789", "JKL012", "MNO345", "PQR678"]},
        {"error": {"min_percent": 30.0}},
        True,
        "some short_codes in allowed set",
        "short_code",
        id="text_found_in_set_percent-pass",
    ),
    # number_found_in_set_percent
    pytest.param(
        "number_found_in_set_percent_pass",
        "number_found_in_set_percent",
        {"expected_values": [1, 2, 3, 4, 5]},
        {"error": {"min_percent": 70.0}},
        True,
        "most category_ids valid",
        "category_id",
        id="number_found_in_set_percent-pass",
    ),
    # expected_text_values_in_use_count
    pytest.param(
        "expected_text_values_in_use_count_pass",
        "expected_text_values_in_use_count",
        {"expected_values": ["ABC123", "XYZ789", "DEF456"]},
        {"error": {"min_count": 2}},
        True,
        "at least 2 expected codes used",
        "short_code",
        id="expected_text_values_in_use_count-pass",
    ),
    # expected_numbers_in_use_count
    pytest.param(
        "expected_numbers_in_use_count_pass",
        "expected_numbers_in_use_count",
        {"expected_values": [1, 2, 3, 4, 5]},
        {"error": {"min_count": 3}},
        True,
        "at least 3 category_ids used",
        "category_id",
        id="expected_numbers_in_use_count-pass",
    ),
    # expected_texts_in_top_values_count
    pytest.param(
        "expected_texts_in_top_values_count_pass",
        "expected_texts_in_top_values_count",
        {"expected_values": ["ABC123"], "top_n": 10},
        {"error": {"min_count": 1}},
        True,
        "ABC123 is a common code",
        "short_code",
        id="expected_texts_in_top_values_count-pass",
    ),
    # text_valid_country_code_percent
    pytest.param(
        "text_valid_country_code_percent_pass",
        "text_valid_country_code_percent",
        {},
        {"error": {"min_percent": 0.0}},
        True,
        "no country codes in short_code",
        "short_code",
        id="text_valid_country_code_percent-pass",
    ),
    # text_valid_currency_code_percent
    pytest.param(
        "text_valid_currency_code_percent_pass",
        "text_valid_currency_code_percent",
        {},
        {"error": {"min_percent": 0.0}},
        True,
        "no currency codes",
        "short_code",
        id="text_valid_currency_code_percent-pass",
    ),
]


class TestAcceptedValuesChecks:
    """Phase 4: Accepted values and domain checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        ACCEPTED_VALUES_CHECK_CASES,
    )
    async def test_accepted_values_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test accepted values and domain checks."""
        check = await check_factory({
            "name": f"pytest-accepted-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 5: Date Pattern & Data Type Detection Checks (NEW)
# =============================================================================

DATE_DATATYPE_CHECK_CASES = [
    # text_parsable_to_integer_percent
    pytest.param(
        "text_parsable_to_integer_percent_pass",
        "text_parsable_to_integer_percent",
        {},
        {"error": {"min_percent": 90.0}},
        True,
        "most category_ids are integers",
        "category_id",
        id="text_parsable_to_integer_percent-pass",
    ),
    # text_parsable_to_float_percent
    pytest.param(
        "text_parsable_to_float_percent_pass",
        "text_parsable_to_float_percent",
        {},
        {"error": {"min_percent": 90.0}},
        True,
        "prices are floats",
        "price",
        id="text_parsable_to_float_percent-pass",
    ),
    # text_parsable_to_boolean_percent
    pytest.param(
        "text_parsable_to_boolean_percent_pass",
        "text_parsable_to_boolean_percent",
        {},
        {"error": {"min_percent": 80.0}},
        True,
        "is_active is boolean",
        "is_active",
        id="text_parsable_to_boolean_percent-pass",
    ),
    # text_not_matching_date_pattern_found
    pytest.param(
        "text_not_matching_date_pattern_found_pass",
        "text_not_matching_date_pattern_found",
        {"date_format": "YYYY-MM-DD"},
        {"error": {"max_count": 5}},
        True,
        "dates match ISO format",
        "event_date",
        id="text_not_matching_date_pattern_found-pass",
    ),
    # text_match_date_format_percent
    pytest.param(
        "text_match_date_format_percent_pass",
        "text_match_date_format_percent",
        {"date_format": "YYYY-MM-DD"},
        {"error": {"min_percent": 90.0}},
        True,
        "most dates in ISO format",
        "event_date",
        id="text_match_date_format_percent-pass",
    ),
]


class TestDateDatatypeChecks:
    """Phase 5: Date pattern and data type detection checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        DATE_DATATYPE_CHECK_CASES,
    )
    async def test_date_datatype_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test date pattern and data type detection checks."""
        check = await check_factory({
            "name": f"pytest-datedt-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 6: PII Detection Checks (NEW)
# =============================================================================

PII_DETECTION_CHECK_CASES = [
    # contains_email_percent
    pytest.param(
        "contains_email_percent_pass",
        "contains_email_percent",
        {},
        {"error": {"max_percent": 100.0}},
        True,
        "email column contains emails",
        "email",
        id="contains_email_percent-pass",
    ),
    # contains_usa_phone_percent
    pytest.param(
        "contains_usa_phone_percent_pass",
        "contains_usa_phone_percent",
        {},
        {"error": {"max_percent": 100.0}},
        True,
        "phone column contains phones",
        "phone",
        id="contains_usa_phone_percent-pass",
    ),
    # contains_usa_zipcode_percent
    pytest.param(
        "contains_usa_zipcode_percent_pass",
        "contains_usa_zipcode_percent",
        {},
        {"error": {"max_percent": 100.0}},
        True,
        "zipcode column contains zips",
        "zipcode",
        id="contains_usa_zipcode_percent-pass",
    ),
    # contains_ip4_percent
    pytest.param(
        "contains_ip4_percent_pass",
        "contains_ip4_percent",
        {},
        {"error": {"max_percent": 100.0}},
        True,
        "ip4 column contains IPs",
        "ip4_address",
        id="contains_ip4_percent-pass",
    ),
    # contains_ip6_percent
    pytest.param(
        "contains_ip6_percent_pass",
        "contains_ip6_percent",
        {},
        {"error": {"max_percent": 100.0}},
        True,
        "ip6 column contains IPv6",
        "ip6_address",
        id="contains_ip6_percent-pass",
    ),
    # PII detection on non-PII column (should find nothing)
    pytest.param(
        "contains_email_percent_non_pii",
        "contains_email_percent",
        {},
        {"error": {"max_percent": 5.0}},
        True,
        "short_code has no emails",
        "short_code",
        id="contains_email_percent-non-pii",
    ),
]


class TestPIIDetectionChecks:
    """Phase 6: PII detection checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        PII_DETECTION_CHECK_CASES,
    )
    async def test_pii_detection_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test PII detection checks."""
        check = await check_factory({
            "name": f"pytest-pii-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 7: Change Detection Checks (NEW)
# =============================================================================

CHANGE_DETECTION_CHECK_CASES = [
    # nulls_percent_change_1_day (no history = fail)
    pytest.param(
        "nulls_percent_change_1_day_fail",
        "nulls_percent_change_1_day",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data - expected to fail",
        "email",
        id="nulls_percent_change_1_day-no-history",
    ),
    # distinct_count_change_1_day
    pytest.param(
        "distinct_count_change_1_day_fail",
        "distinct_count_change_1_day",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data",
        "email",
        id="distinct_count_change_1_day-no-history",
    ),
    # distinct_percent_change_7_days
    pytest.param(
        "distinct_percent_change_7_days_fail",
        "distinct_percent_change_7_days",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data",
        "score",
        id="distinct_percent_change_7_days-no-history",
    ),
    # mean_change_1_day
    pytest.param(
        "mean_change_1_day_fail",
        "mean_change_1_day",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data",
        "score",
        id="mean_change_1_day-no-history",
    ),
    # median_change_7_days
    pytest.param(
        "median_change_7_days_fail",
        "median_change_7_days",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data",
        "score",
        id="median_change_7_days-no-history",
    ),
    # sum_change_30_days
    pytest.param(
        "sum_change_30_days_fail",
        "sum_change_30_days",
        {},
        {"error": {"max_change_percent": 100.0}},
        False,
        "No historical data",
        "score",
        id="sum_change_30_days-no-history",
    ),
]


class TestChangeDetectionChecks:
    """Phase 7: Change detection checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        CHANGE_DETECTION_CHECK_CASES,
    )
    async def test_change_detection_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str,
    ):
        """Test change detection checks."""
        check = await check_factory({
            "name": f"pytest-change-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": target_column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 8: Cross-Table Comparison Checks (NEW)
# =============================================================================

CROSS_TABLE_CHECK_CASES = [
    # total_row_count_match_percent
    pytest.param(
        "total_row_count_match_percent_pass",
        "total_row_count_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same table comparison = 100%",
        None,
        id="total_row_count_match_percent-same-table",
    ),
    # total_sum_match_percent
    pytest.param(
        "total_sum_match_percent_pass",
        "total_sum_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
            "reference_column": "score",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same column comparison = 100%",
        "score",
        id="total_sum_match_percent-same-column",
    ),
    # total_min_match_percent
    pytest.param(
        "total_min_match_percent_pass",
        "total_min_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
            "reference_column": "score",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same column min = 100%",
        "score",
        id="total_min_match_percent-same-column",
    ),
    # total_max_match_percent
    pytest.param(
        "total_max_match_percent_pass",
        "total_max_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
            "reference_column": "score",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same column max = 100%",
        "score",
        id="total_max_match_percent-same-column",
    ),
    # total_average_match_percent
    pytest.param(
        "total_average_match_percent_pass",
        "total_average_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
            "reference_column": "score",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same column avg = 100%",
        "score",
        id="total_average_match_percent-same-column",
    ),
    # total_not_null_count_match_percent
    pytest.param(
        "total_not_null_count_match_percent_pass",
        "total_not_null_count_match_percent",
        {
            "reference_schema": "public",
            "reference_table": "test_data_quality",
            "reference_column": "score",
        },
        {"error": {"min_percent": 100.0}},
        True,
        "Same column not_null count = 100%",
        "score",
        id="total_not_null_count_match_percent-same-column",
    ),
]


class TestCrossTableChecks:
    """Phase 8: Cross-table comparison checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc,target_column",
        CROSS_TABLE_CHECK_CASES,
    )
    async def test_cross_table_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
        target_column: str | None,
    ):
        """Test cross-table comparison checks."""
        check_data = {
            "name": f"pytest-crosstable-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if target_column:
            check_data["target_column"] = target_column

        check = await check_factory(check_data)
        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 9: Table-Level Misc Checks (NEW)
# =============================================================================

TABLE_LEVEL_MISC_CHECK_CASES = [
    # table_availability
    pytest.param(
        "table_availability_pass",
        "table_availability",
        {},
        {},
        True,
        "Table is accessible",
        id="table_availability-pass",
    ),
    # sql_condition_passed_percent_on_table
    pytest.param(
        "sql_condition_passed_percent_on_table_pass",
        "sql_condition_passed_percent_on_table",
        {"sql_condition": "score >= 0"},
        {"error": {"min_percent": 95.0}},
        True,
        "All scores >= 0",
        id="sql_condition_passed_percent_on_table-pass",
    ),
    pytest.param(
        "sql_condition_passed_percent_on_table_fail",
        "sql_condition_passed_percent_on_table",
        {"sql_condition": "score > 90"},
        {"error": {"min_percent": 50.0}},
        False,
        "Few scores > 90",
        id="sql_condition_passed_percent_on_table-fail",
    ),
]


class TestTableLevelMiscChecks:
    """Phase 9: Table-level miscellaneous checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        TABLE_LEVEL_MISC_CHECK_CASES,
    )
    async def test_table_level_misc_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test table-level miscellaneous checks."""
        check = await check_factory({
            "name": f"pytest-tablemisc-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 10a: Text Length Percent Checks (4 tests)
# =============================================================================

TEXT_LENGTH_PERCENT_CHECK_CASES = [
    # (test_id, check_type, column, params, rule_params, expected_pass, description)
    pytest.param(
        "text_length_below_min_percent_pass",
        "text_length_below_min_length_percent",
        "short_code",
        {"min_length": 1},
        {"error": {"max_percent": 10.0}},
        True,
        "Short codes - few below min length 1",
        id="text_length_below_min_percent-pass",
    ),
    pytest.param(
        "text_length_below_min_percent_fail",
        "text_length_below_min_length_percent",
        "short_code",
        {"min_length": 5},
        {"error": {"max_percent": 4.0}},
        False,
        "Short codes - many below min length 5",
        id="text_length_below_min_percent-fail",
    ),
    pytest.param(
        "text_length_above_max_percent_pass",
        "text_length_above_max_length_percent",
        "short_code",
        {"max_length": 100},
        {"error": {"max_percent": 5.0}},
        True,
        "Short codes - none above max length 100",
        id="text_length_above_max_percent-pass",
    ),
    pytest.param(
        "text_length_above_max_percent_fail",
        "text_length_above_max_length_percent",
        "short_code",
        {"max_length": 3},
        {"error": {"max_percent": 5.0}},
        False,
        "Short codes - many above max length 3",
        id="text_length_above_max_percent-fail",
    ),
]


class TestTextLengthPercentChecks:
    """Phase 10a: Text length percent checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,column,params,rule_params,expected_pass,desc",
        TEXT_LENGTH_PERCENT_CHECK_CASES,
    )
    async def test_text_length_percent_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        column: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test text length percent checks."""
        check = await check_factory({
            "name": f"pytest-textlenpct-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 10b: Column-level Custom SQL Checks (10 tests)
# =============================================================================

COLUMN_CUSTOM_SQL_CHECK_CASES = [
    # (test_id, check_type, column, params, rule_params, expected_pass, description)
    pytest.param(
        "sql_condition_failed_on_column_pass",
        "sql_condition_failed_on_column",
        "score",
        {"sql_condition": "score >= 0"},
        {"error": {"max_count": 0}},
        True,
        "All scores >= 0",
        id="sql_condition_failed_on_column-pass",
    ),
    pytest.param(
        "sql_condition_failed_on_column_fail",
        "sql_condition_failed_on_column",
        "score",
        {"sql_condition": "score > 90"},
        {"error": {"max_count": 5}},
        False,
        "Many scores <= 90",
        id="sql_condition_failed_on_column-fail",
    ),
    pytest.param(
        "sql_condition_passed_percent_on_column_pass",
        "sql_condition_passed_percent_on_column",
        "score",
        {"sql_condition": "score >= 0"},
        {"error": {"min_percent": 95.0}},
        True,
        "All scores >= 0",
        id="sql_condition_passed_percent_on_column-pass",
    ),
    pytest.param(
        "sql_condition_passed_percent_on_column_fail",
        "sql_condition_passed_percent_on_column",
        "score",
        {"sql_condition": "score > 95"},
        {"error": {"min_percent": 50.0}},
        False,
        "Few scores > 95",
        id="sql_condition_passed_percent_on_column-fail",
    ),
    pytest.param(
        "sql_aggregate_expression_on_column_pass",
        "sql_aggregate_expression_on_column",
        "score",
        {"sql_expression": "AVG(score)"},
        {"error": {"min_value": 50.0, "max_value": 90.0}},
        True,
        "Average score in range 50-90",
        id="sql_aggregate_expression_on_column-pass",
    ),
    pytest.param(
        "sql_aggregate_expression_on_column_fail",
        "sql_aggregate_expression_on_column",
        "score",
        {"sql_expression": "AVG(score)"},
        {"error": {"min_value": 95.0, "max_value": 100.0}},
        False,
        "Average score not in range 95-100",
        id="sql_aggregate_expression_on_column-fail",
    ),
    pytest.param(
        "sql_invalid_value_count_on_column_pass",
        "sql_invalid_value_count_on_column",
        "short_code",
        {"invalid_values": "'XXX', 'YYY', 'ZZZ'"},
        {"error": {"max_count": 0}},
        True,
        "No invalid short codes XXX/YYY/ZZZ",
        id="sql_invalid_value_count_on_column-pass",
    ),
    pytest.param(
        "sql_invalid_value_count_on_column_fail",
        "sql_invalid_value_count_on_column",
        "short_code",
        {"invalid_values": "'AB', 'ABC', 'A'"},
        {"error": {"max_count": 0}},
        False,
        "Some short codes match invalid list",
        id="sql_invalid_value_count_on_column-fail",
    ),
    pytest.param(
        "import_custom_result_on_column_pass",
        "import_custom_result_on_column",
        "score",
        {"imported_value": 95.0},
        {"error": {"min_value": 90.0, "max_value": 100.0}},
        True,
        "Imported value 95.0 in range 90-100",
        id="import_custom_result_on_column-pass",
    ),
    pytest.param(
        "import_custom_result_on_column_fail",
        "import_custom_result_on_column",
        "score",
        {"imported_value": 50.0},
        {"error": {"min_value": 90.0, "max_value": 100.0}},
        False,
        "Imported value 50.0 not in range 90-100",
        id="import_custom_result_on_column-fail",
    ),
]


class TestColumnCustomSQLChecks:
    """Phase 10b: Column-level custom SQL checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,column,params,rule_params,expected_pass,desc",
        COLUMN_CUSTOM_SQL_CHECK_CASES,
    )
    async def test_column_custom_sql_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        column: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test column-level custom SQL checks."""
        check = await check_factory({
            "name": f"pytest-colsql-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "target_column": column,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 10c: Table-level Custom SQL Checks (2 tests)
# =============================================================================

TABLE_CUSTOM_SQL_CHECK_CASES = [
    # (test_id, check_type, params, rule_params, expected_pass, description)
    pytest.param(
        "sql_invalid_record_count_on_table_pass",
        "sql_invalid_record_count_on_table",
        {"sql_condition": "score < 0"},
        {"error": {"max_count": 0}},
        True,
        "No records with score < 0",
        id="sql_invalid_record_count_on_table-pass",
    ),
    pytest.param(
        "sql_invalid_record_count_on_table_fail",
        "sql_invalid_record_count_on_table",
        {"sql_condition": "score > 50"},
        {"error": {"max_count": 2}},
        False,
        "Multiple records with score > 50",
        id="sql_invalid_record_count_on_table-fail",
    ),
]


class TestTableCustomSQLChecks:
    """Phase 10c: Table-level custom SQL checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        TABLE_CUSTOM_SQL_CHECK_CASES,
    )
    async def test_table_custom_sql_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test table-level custom SQL checks."""
        check = await check_factory({
            "name": f"pytest-tblsql-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 10d: Schema Detection Checks (6 tests)
# =============================================================================

SCHEMA_DETECTION_CHECK_CASES = [
    # (test_id, check_type, params, rule_params, expected_pass, description)
    pytest.param(
        "column_list_changed_pass",
        "column_list_changed",
        {"expected_hash": ""},  # Empty hash means baseline not set - passes as 0
        {"error": {"forbidden_value": 1}},
        True,
        "No baseline set - check passes",
        id="column_list_changed-no-baseline",
    ),
    pytest.param(
        "column_list_changed_fail",
        "column_list_changed",
        {"expected_hash": "invalid_hash_will_not_match"},
        {"error": {"forbidden_value": 1}},
        False,
        "Hash mismatch - columns changed",
        id="column_list_changed-hash-mismatch",
    ),
    pytest.param(
        "column_list_or_order_changed_pass",
        "column_list_or_order_changed",
        {"expected_hash": ""},  # Empty hash means baseline not set
        {"error": {"forbidden_value": 1}},
        True,
        "No baseline set - check passes",
        id="column_list_or_order_changed-no-baseline",
    ),
    pytest.param(
        "column_list_or_order_changed_fail",
        "column_list_or_order_changed",
        {"expected_hash": "invalid_hash_will_not_match"},
        {"error": {"forbidden_value": 1}},
        False,
        "Hash mismatch - column order changed",
        id="column_list_or_order_changed-hash-mismatch",
    ),
    pytest.param(
        "column_types_changed_pass",
        "column_types_changed",
        {"expected_hash": ""},  # Empty hash means baseline not set
        {"error": {"forbidden_value": 1}},
        True,
        "No baseline set - check passes",
        id="column_types_changed-no-baseline",
    ),
    pytest.param(
        "column_types_changed_fail",
        "column_types_changed",
        {"expected_hash": "invalid_hash_will_not_match"},
        {"error": {"forbidden_value": 1}},
        False,
        "Hash mismatch - column types changed",
        id="column_types_changed-hash-mismatch",
    ),
]


class TestSchemaDetectionChecks:
    """Phase 10d: Schema detection checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        SCHEMA_DETECTION_CHECK_CASES,
    )
    async def test_schema_detection_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test schema detection checks."""
        check = await check_factory({
            "name": f"pytest-schema-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 11a: Import External Results Checks (2 tests)
# =============================================================================

IMPORT_TABLE_CHECK_CASES = [
    # (test_id, check_type, params, rule_params, expected_pass, description)
    pytest.param(
        "import_custom_result_on_table_pass",
        "import_custom_result_on_table",
        {"imported_value": 95.0},
        {"error": {"min_value": 90.0, "max_value": 100.0}},
        True,
        "Imported value 95.0 in range 90-100",
        id="import_custom_result_on_table-pass",
    ),
    pytest.param(
        "import_custom_result_on_table_fail",
        "import_custom_result_on_table",
        {"imported_value": 50.0},
        {"error": {"min_value": 90.0, "max_value": 100.0}},
        False,
        "Imported value 50.0 not in range 90-100",
        id="import_custom_result_on_table-fail",
    ),
]


class TestImportTableChecks:
    """Phase 11a: Import external results table-level checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,params,rule_params,expected_pass,desc",
        IMPORT_TABLE_CHECK_CASES,
    )
    async def test_import_table_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test import external results table-level checks."""
        check = await check_factory({
            "name": f"pytest-import-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        })

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Phase 11b: Generic Change Detection Checks (14 tests)
# =============================================================================

GENERIC_CHANGE_CHECK_CASES = [
    # (test_id, check_type, column, params, rule_params, expected_pass, description)
    pytest.param(
        "row_count_change_pass",
        "row_count_change",
        None,  # Table-level
        {"baseline_count": 20},
        {"error": {"max_change_percent": 10.0}},
        True,
        "Row count matches baseline (20)",
        id="row_count_change-pass",
    ),
    pytest.param(
        "row_count_change_fail",
        "row_count_change",
        None,
        {"baseline_count": 100},
        {"error": {"max_change_percent": 10.0}},
        False,
        "Row count (20) differs from baseline (100) by 80%",
        id="row_count_change-fail",
    ),
    pytest.param(
        "nulls_percent_change_pass",
        "nulls_percent_change",
        "email",
        {"baseline_percent": 10.0},
        {"error": {"max_value": 5.0}},
        True,
        "Null percent close to baseline",
        id="nulls_percent_change-pass",
    ),
    pytest.param(
        "nulls_percent_change_fail",
        "nulls_percent_change",
        "email",
        {"baseline_percent": 50.0},
        {"error": {"max_value": 5.0}},
        False,
        "Null percent differs from baseline by >5%",
        id="nulls_percent_change-fail",
    ),
    pytest.param(
        "distinct_count_change_pass",
        "distinct_count_change",
        "category_id",
        {"baseline_count": 7},
        {"error": {"max_change_percent": 50.0}},
        True,
        "Distinct count close to baseline",
        id="distinct_count_change-pass",
    ),
    pytest.param(
        "distinct_count_change_fail",
        "distinct_count_change",
        "category_id",
        {"baseline_count": 100},
        {"error": {"max_change_percent": 10.0}},
        False,
        "Distinct count differs from baseline",
        id="distinct_count_change-fail",
    ),
    pytest.param(
        "distinct_percent_change_pass",
        "distinct_percent_change",
        "email",
        {"baseline_percent": 90.0},
        {"error": {"max_value": 15.0}},
        True,
        "Distinct percent close to baseline",
        id="distinct_percent_change-pass",
    ),
    pytest.param(
        "distinct_percent_change_fail",
        "distinct_percent_change",
        "email",
        {"baseline_percent": 10.0},
        {"error": {"max_value": 5.0}},
        False,
        "Distinct percent differs from baseline",
        id="distinct_percent_change-fail",
    ),
    pytest.param(
        "mean_change_pass",
        "mean_change",
        "score",
        {"baseline_value": 70.0},
        {"error": {"max_change_percent": 20.0}},
        True,
        "Mean close to baseline",
        id="mean_change-pass",
    ),
    pytest.param(
        "mean_change_fail",
        "mean_change",
        "score",
        {"baseline_value": 10.0},
        {"error": {"max_change_percent": 10.0}},
        False,
        "Mean differs from baseline",
        id="mean_change-fail",
    ),
    pytest.param(
        "median_change_pass",
        "median_change",
        "score",
        {"baseline_value": 75.0},
        {"error": {"max_change_percent": 20.0}},
        True,
        "Median close to baseline",
        id="median_change-pass",
    ),
    pytest.param(
        "median_change_fail",
        "median_change",
        "score",
        {"baseline_value": 10.0},
        {"error": {"max_change_percent": 10.0}},
        False,
        "Median differs from baseline",
        id="median_change-fail",
    ),
    pytest.param(
        "sum_change_pass",
        "sum_change",
        "score",
        {"baseline_value": 1400.0},
        {"error": {"max_change_percent": 20.0}},
        True,
        "Sum close to baseline",
        id="sum_change-pass",
    ),
    pytest.param(
        "sum_change_fail",
        "sum_change",
        "score",
        {"baseline_value": 100.0},
        {"error": {"max_change_percent": 10.0}},
        False,
        "Sum differs from baseline",
        id="sum_change-fail",
    ),
]


class TestGenericChangeDetectionChecks:
    """Phase 11b: Generic change detection checks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,column,params,rule_params,expected_pass,desc",
        GENERIC_CHANGE_CHECK_CASES,
    )
    async def test_generic_change_detection_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        column: str | None,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test generic change detection checks."""
        check_config = {
            "name": f"pytest-change-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if column:
            check_config["target_column"] = column

        check = await check_factory(check_config)

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Anomaly Detection Checks (Phase 12 - 10 tests)
# =============================================================================

ANOMALY_CHECK_CASES = [
    pytest.param(
        "row_count_anomaly",
        "row_count_anomaly",
        None,
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Row count anomaly (no history = passed)",
        id="row_count_anomaly",
    ),
    pytest.param(
        "data_freshness_anomaly",
        "data_freshness_anomaly",
        "event_date",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Data freshness anomaly (no history = passed)",
        id="data_freshness_anomaly",
    ),
    pytest.param(
        "nulls_percent_anomaly",
        "nulls_percent_anomaly",
        "email",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Nulls percent anomaly (no history = passed)",
        id="nulls_percent_anomaly",
    ),
    pytest.param(
        "distinct_count_anomaly",
        "distinct_count_anomaly",
        "email",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Distinct count anomaly (no history = passed)",
        id="distinct_count_anomaly",
    ),
    pytest.param(
        "distinct_percent_anomaly",
        "distinct_percent_anomaly",
        "email",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Distinct percent anomaly (no history = passed)",
        id="distinct_percent_anomaly",
    ),
    pytest.param(
        "sum_anomaly",
        "sum_anomaly",
        "score",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Sum anomaly (no history = passed)",
        id="sum_anomaly",
    ),
    pytest.param(
        "mean_anomaly",
        "mean_anomaly",
        "score",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Mean anomaly (no history = passed)",
        id="mean_anomaly",
    ),
    pytest.param(
        "median_anomaly",
        "median_anomaly",
        "score",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Median anomaly (no history = passed)",
        id="median_anomaly",
    ),
    pytest.param(
        "min_anomaly",
        "min_anomaly",
        "score",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Min anomaly (no history = passed)",
        id="min_anomaly",
    ),
    pytest.param(
        "max_anomaly",
        "max_anomaly",
        "score",
        {},
        {"error": {"anomaly_percent": 0.05}},
        True,
        "Max anomaly (no history = passed)",
        id="max_anomaly",
    ),
]


class TestAnomalyChecks:
    """Phase 12a: Anomaly detection checks.

    All anomaly checks pass on first run because there is no historical data,
    which triggers the 'insufficient history' path -> PASSED.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "test_id,check_type,column,params,rule_params,expected_pass,desc",
        ANOMALY_CHECK_CASES,
    )
    async def test_anomaly_check(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        test_id: str,
        check_type: str,
        column: str | None,
        params: dict,
        rule_params: dict,
        expected_pass: bool,
        desc: str,
    ):
        """Test anomaly detection checks."""
        check_config = {
            "name": f"pytest-anomaly-{test_id}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": params,
            "rule_parameters": rule_params,
        }
        if column:
            check_config["target_column"] = column

        check = await check_factory(check_config)

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") == expected_pass, f"{desc}: {result}"


# =============================================================================
# Cross-Source Comparison Checks (Phase 12 - 9+ tests)
# =============================================================================


class TestCrossSourceChecks:
    """Phase 12b: Cross-source comparison checks.

    Creates a second connection to the same PostgreSQL database and compares
    the same table via both connections -> 100% match -> PASSED.
    """

    @pytest_asyncio.fixture
    async def reference_connection_id(self, api_client: httpx.AsyncClient):
        """Create a second connection for cross-source tests."""
        conn_data = {
            "name": f"pytest-ref-{uuid.uuid4().hex[:8]}",
            "description": "Reference connection for cross-source tests",
            "connection_type": "postgresql",
            "config": {
                "host": os.getenv("DQ_PG_HOST", "localhost"),
                "port": int(os.getenv("DQ_PG_PORT", "5433")),
                "database": os.getenv("DQ_PG_DATABASE", "dq_platform"),
                "user": os.getenv("DQ_PG_USER", "postgres"),
                "password": os.getenv("DQ_PG_PASSWORD", "postgres"),
            },
        }
        response = await api_client.post("/connections", json=conn_data)
        if response.status_code != 201:
            pytest.fail(f"Failed to create ref connection: {response.status_code} - {response.text}")

        conn_id = response.json()["id"]
        yield conn_id
        await api_client.delete(f"/connections/{conn_id}")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "check_type,column,desc",
        [
            pytest.param("row_count_match", None, "Row count match", id="row_count_match"),
            pytest.param("column_count_match", None, "Column count match", id="column_count_match"),
            pytest.param("sum_match", "score", "Sum match", id="sum_match"),
            pytest.param("min_match", "score", "Min match", id="min_match"),
            pytest.param("max_match", "score", "Max match", id="max_match"),
            pytest.param("mean_match", "score", "Mean match", id="mean_match"),
            pytest.param("not_null_count_match", "email", "Not null count match", id="not_null_count_match"),
            pytest.param("null_count_match", "email", "Null count match", id="null_count_match"),
            pytest.param("distinct_count_match", "email", "Distinct count match", id="distinct_count_match"),
        ],
    )
    async def test_cross_source_match_pass(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        reference_connection_id: str,
        check_type: str,
        column: str | None,
        desc: str,
    ):
        """Test cross-source checks: same table via both connections -> 100% match."""
        check_config = {
            "name": f"pytest-xsrc-{check_type}",
            "check_type": check_type,
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": {
                "reference_connection_id": reference_connection_id,
            },
            "rule_parameters": {"error": {"min_percent": 100.0}},
        }
        if column:
            check_config["target_column"] = column
            check_config["parameters"]["reference_column"] = column

        check = await check_factory(check_config)

        result = await run_check_and_wait(api_client, check["id"])

        assert result.get("passed") is True, f"{desc}: {result}"

    @pytest.mark.asyncio
    async def test_cross_source_row_count_mismatch(
        self,
        api_client: httpx.AsyncClient,
        check_factory,
        reference_connection_id: str,
    ):
        """Test cross-source check fails when comparing different tables."""
        check_config = {
            "name": "pytest-xsrc-mismatch",
            "check_type": "row_count_match",
            "check_mode": "monitoring",
            "target_table": DEFAULT_TABLE,
            "target_schema": DEFAULT_SCHEMA,
            "parameters": {
                "reference_connection_id": reference_connection_id,
                "reference_table": "test_categories",
            },
            "rule_parameters": {"error": {"min_percent": 100.0}},
        }

        check = await check_factory(check_config)

        result = await run_check_and_wait(api_client, check["id"])

        # Different tables should have different row counts -> match < 100%
        assert result.get("passed") is False, f"Expected mismatch: {result}"