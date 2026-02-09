"""Test GX-based check engine components."""

import pytest

from dq_platform.checks.gx_registry import (
    GX_EXPECTATION_MAP,
    build_expectation,
    get_check_description,
    is_column_level_check,
)
from dq_platform.models.check import CheckType


class TestGXRegistry:
    """Test GX expectation registry."""

    def test_all_check_types_registered(self) -> None:
        """Verify all CheckType values have GX mappings."""
        for check_type in CheckType:
            assert check_type in GX_EXPECTATION_MAP, f"Missing mapping for {check_type}"

    def test_is_column_level_check_table_level(self) -> None:
        """Test table-level checks are identified correctly."""
        table_level_checks = [
            CheckType.ROW_COUNT,
            CheckType.ROW_COUNT_MIN,
            CheckType.ROW_COUNT_MAX,
            CheckType.SCHEMA_COLUMN_EXISTS,
            CheckType.SCHEMA_COLUMN_COUNT,
            CheckType.TABLE_AVAILABILITY,
            # New table-level checks
            CheckType.ROW_COUNT_EXACT,
            CheckType.ROW_COUNT_COMPARE,
            CheckType.SCHEMA_COLUMN_LIST,
            CheckType.SCHEMA_COLUMN_ORDER,
            CheckType.COMPOSITE_KEY_UNIQUE,
            CheckType.MULTICOLUMN_UNIQUE,
        ]
        for check_type in table_level_checks:
            assert is_column_level_check(check_type) is False, f"{check_type} should be table-level"

    def test_is_column_level_check_column_level(self) -> None:
        """Test column-level checks are identified correctly."""
        column_level_checks = [
            CheckType.NULL_COUNT,
            CheckType.NULL_PERCENT,
            CheckType.NOT_NULL,
            CheckType.UNIQUE,
            CheckType.DISTINCT_COUNT,
            CheckType.DUPLICATE_COUNT,
            CheckType.DATA_FRESHNESS,
            CheckType.CUSTOM_SQL,
            CheckType.VALUE_RANGE,
            CheckType.REGEX_PATTERN,
            CheckType.ALLOWED_VALUES,
            CheckType.COLUMN_PAIR_COMPARISON,
            # New column-level checks
            CheckType.COMPLETENESS_PERCENT,
            CheckType.COLUMN_MIN,
            CheckType.COLUMN_MAX,
            CheckType.COLUMN_MEAN,
            CheckType.COLUMN_MEDIAN,
            CheckType.COLUMN_STDDEV,
            CheckType.COLUMN_SUM,
            CheckType.COLUMN_QUANTILE,
            CheckType.TEXT_LENGTH_RANGE,
            CheckType.TEXT_LENGTH_EXACT,
            CheckType.REGEX_NOT_MATCH,
            CheckType.LIKE_PATTERN,
            CheckType.FORBIDDEN_VALUES,
            CheckType.COLUMN_TYPE,
            CheckType.DATE_PARSEABLE,
            CheckType.JSON_PARSEABLE,
            CheckType.DATETIME_FORMAT,
            CheckType.UNIQUENESS_PERCENT,
            CheckType.DISTINCT_VALUES_IN_SET,
            CheckType.MOST_COMMON_VALUE,
            CheckType.VALUES_INCREASING,
            CheckType.VALUES_DECREASING,
            CheckType.COLUMN_PAIR_EQUAL,
        ]
        for check_type in column_level_checks:
            assert is_column_level_check(check_type) is True, f"{check_type} should be column-level"

    def test_get_check_description(self) -> None:
        """Test descriptions are available for all check types."""
        for check_type in CheckType:
            desc = get_check_description(check_type)
            assert isinstance(desc, str)
            assert len(desc) > 0


class TestBuildExpectation:
    """Test expectation building from check types."""

    def test_build_row_count(self) -> None:
        """Test building row count expectation."""
        expectation = build_expectation(
            CheckType.ROW_COUNT,
            {"min_value": 10, "max_value": 100},
        )
        assert expectation is not None
        assert "ExpectTableRowCountToBeBetween" in type(expectation).__name__

    def test_build_row_count_min(self) -> None:
        """Test building minimum row count expectation."""
        expectation = build_expectation(
            CheckType.ROW_COUNT_MIN,
            {"min_value": 10},
        )
        assert expectation is not None

    def test_build_row_count_max(self) -> None:
        """Test building maximum row count expectation."""
        expectation = build_expectation(
            CheckType.ROW_COUNT_MAX,
            {"max_value": 1000},
        )
        assert expectation is not None

    def test_build_not_null(self) -> None:
        """Test building not null expectation."""
        expectation = build_expectation(
            CheckType.NOT_NULL,
            {},
            column="email",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToNotBeNull" in type(expectation).__name__

    def test_build_unique(self) -> None:
        """Test building uniqueness expectation."""
        expectation = build_expectation(
            CheckType.UNIQUE,
            {},
            column="user_id",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeUnique" in type(expectation).__name__

    def test_build_null_percent(self) -> None:
        """Test building null percentage expectation."""
        expectation = build_expectation(
            CheckType.NULL_PERCENT,
            {"max_percent": 5},
            column="email",
        )
        assert expectation is not None

    def test_build_distinct_count(self) -> None:
        """Test building distinct count expectation."""
        expectation = build_expectation(
            CheckType.DISTINCT_COUNT,
            {"min_value": 5, "max_value": 100},
            column="status",
        )
        assert expectation is not None

    def test_build_schema_column_exists(self) -> None:
        """Test building column exists expectation."""
        expectation = build_expectation(
            CheckType.SCHEMA_COLUMN_EXISTS,
            {"column_name": "email"},
        )
        assert expectation is not None
        assert "ExpectColumnToExist" in type(expectation).__name__

    def test_build_schema_column_count(self) -> None:
        """Test building column count expectation."""
        expectation = build_expectation(
            CheckType.SCHEMA_COLUMN_COUNT,
            {"expected_value": 10},
        )
        assert expectation is not None

    def test_build_table_availability(self) -> None:
        """Test building table availability expectation."""
        expectation = build_expectation(
            CheckType.TABLE_AVAILABILITY,
            {},
        )
        assert expectation is not None

    def test_column_level_check_requires_column(self) -> None:
        """Test that column-level checks require column parameter."""
        with pytest.raises(ValueError, match="requires a column parameter"):
            build_expectation(
                CheckType.NOT_NULL,
                {},
                column=None,
            )

    def test_unknown_check_type_raises_error(self) -> None:
        """Test that unknown check type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown check type"):
            is_column_level_check("invalid_type")  # type: ignore

    def test_build_value_range(self) -> None:
        """Test building value range expectation."""
        expectation = build_expectation(
            CheckType.VALUE_RANGE,
            {"min_value": 0, "max_value": 150},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeBetween" in type(expectation).__name__

    def test_build_regex_pattern(self) -> None:
        """Test building regex pattern expectation."""
        expectation = build_expectation(
            CheckType.REGEX_PATTERN,
            {"pattern": r"^[\w\.-]+@[\w\.-]+\.\w+$"},
            column="email",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToMatchRegex" in type(expectation).__name__

    def test_build_allowed_values(self) -> None:
        """Test building allowed values expectation."""
        expectation = build_expectation(
            CheckType.ALLOWED_VALUES,
            {"allowed_values": ["active", "inactive", "pending"]},
            column="status",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeInSet" in type(expectation).__name__

    def test_build_column_pair_comparison(self) -> None:
        """Test building column pair comparison expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_PAIR_COMPARISON,
            {"column_b": "created_at"},
            column="updated_at",
        )
        assert expectation is not None
        assert "ExpectColumnPairValuesAToBeGreaterThanB" in type(expectation).__name__

    # --- Volume checks ---

    def test_build_row_count_exact(self) -> None:
        """Test building exact row count expectation."""
        expectation = build_expectation(
            CheckType.ROW_COUNT_EXACT,
            {"value": 1000},
        )
        assert expectation is not None
        assert "ExpectTableRowCountToEqual" in type(expectation).__name__

    def test_build_row_count_compare(self) -> None:
        """Test building row count comparison expectation."""
        expectation = build_expectation(
            CheckType.ROW_COUNT_COMPARE,
            {"other_table_name": "backup_users"},
        )
        assert expectation is not None
        assert "ExpectTableRowCountToEqualOtherTable" in type(expectation).__name__

    # --- Schema checks ---

    def test_build_schema_column_list(self) -> None:
        """Test building column set validation expectation."""
        expectation = build_expectation(
            CheckType.SCHEMA_COLUMN_LIST,
            {"column_set": ["id", "name", "email"]},
        )
        assert expectation is not None
        assert "ExpectTableColumnsToMatchSet" in type(expectation).__name__

    def test_build_schema_column_order(self) -> None:
        """Test building column order validation expectation."""
        expectation = build_expectation(
            CheckType.SCHEMA_COLUMN_ORDER,
            {"column_list": ["id", "name", "email"]},
        )
        assert expectation is not None
        assert "ExpectTableColumnsToMatchOrderedList" in type(expectation).__name__

    # --- Completeness checks ---

    def test_build_completeness_percent(self) -> None:
        """Test building completeness percentage expectation."""
        expectation = build_expectation(
            CheckType.COMPLETENESS_PERCENT,
            {"min_value": 0.9, "max_value": 1.0},
            column="email",
        )
        assert expectation is not None

    # --- Numeric/Statistical checks ---

    def test_build_column_min(self) -> None:
        """Test building column min expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_MIN,
            {"min_value": 0, "max_value": 10},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnMinToBeBetween" in type(expectation).__name__

    def test_build_column_max(self) -> None:
        """Test building column max expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_MAX,
            {"min_value": 100, "max_value": 150},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnMaxToBeBetween" in type(expectation).__name__

    def test_build_column_mean(self) -> None:
        """Test building column mean expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_MEAN,
            {"min_value": 25, "max_value": 35},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnMeanToBeBetween" in type(expectation).__name__

    def test_build_column_median(self) -> None:
        """Test building column median expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_MEDIAN,
            {"min_value": 20, "max_value": 40},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnMedianToBeBetween" in type(expectation).__name__

    def test_build_column_stddev(self) -> None:
        """Test building column stddev expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_STDDEV,
            {"min_value": 0, "max_value": 15},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnStdevToBeBetween" in type(expectation).__name__

    def test_build_column_sum(self) -> None:
        """Test building column sum expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_SUM,
            {"min_value": 1000, "max_value": 10000},
            column="amount",
        )
        assert expectation is not None
        assert "ExpectColumnSumToBeBetween" in type(expectation).__name__

    def test_build_column_quantile(self) -> None:
        """Test building column quantile expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_QUANTILE,
            {"quantile_ranges": {"quantiles": [0.25, 0.5, 0.75], "value_ranges": [[0, 10], [10, 20], [20, 30]]}},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnQuantileValuesToBeBetween" in type(expectation).__name__

    # --- Text checks ---

    def test_build_text_length_range(self) -> None:
        """Test building text length range expectation."""
        expectation = build_expectation(
            CheckType.TEXT_LENGTH_RANGE,
            {"min_value": 1, "max_value": 255},
            column="name",
        )
        assert expectation is not None
        assert "ExpectColumnValueLengthsToBeBetween" in type(expectation).__name__

    def test_build_text_length_exact(self) -> None:
        """Test building exact text length expectation."""
        expectation = build_expectation(
            CheckType.TEXT_LENGTH_EXACT,
            {"value": 10},
            column="phone_code",
        )
        assert expectation is not None
        assert "ExpectColumnValueLengthsToEqual" in type(expectation).__name__

    # --- Pattern checks ---

    def test_build_regex_not_match(self) -> None:
        """Test building regex NOT match expectation."""
        expectation = build_expectation(
            CheckType.REGEX_NOT_MATCH,
            {"pattern": r"^\d{3}-\d{2}-\d{4}$"},  # SSN pattern to block
            column="notes",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToNotMatchRegex" in type(expectation).__name__

    def test_build_like_pattern(self) -> None:
        """Test building SQL LIKE pattern expectation."""
        expectation = build_expectation(
            CheckType.LIKE_PATTERN,
            {"like_pattern": "%@%.%"},
            column="email",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToMatchLikePattern" in type(expectation).__name__

    def test_build_forbidden_values(self) -> None:
        """Test building forbidden values expectation."""
        expectation = build_expectation(
            CheckType.FORBIDDEN_VALUES,
            {"forbidden_values": ["N/A", "NULL", "undefined"]},
            column="status",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToNotBeInSet" in type(expectation).__name__

    # --- Datatype checks ---

    def test_build_column_type(self) -> None:
        """Test building column type expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_TYPE,
            {"type_": "int"},
            column="age",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeOfType" in type(expectation).__name__

    def test_build_date_parseable(self) -> None:
        """Test building date parseable expectation."""
        expectation = build_expectation(
            CheckType.DATE_PARSEABLE,
            {},
            column="created_at",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeDateutilParseable" in type(expectation).__name__

    def test_build_json_parseable(self) -> None:
        """Test building JSON parseable expectation."""
        expectation = build_expectation(
            CheckType.JSON_PARSEABLE,
            {},
            column="config",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeJsonParseable" in type(expectation).__name__

    def test_build_datetime_format(self) -> None:
        """Test building datetime format expectation."""
        expectation = build_expectation(
            CheckType.DATETIME_FORMAT,
            {"strftime_format": "%Y-%m-%d %H:%M:%S"},
            column="timestamp",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToMatchStrftimeFormat" in type(expectation).__name__

    # --- Uniqueness checks ---

    def test_build_uniqueness_percent(self) -> None:
        """Test building uniqueness percentage expectation."""
        expectation = build_expectation(
            CheckType.UNIQUENESS_PERCENT,
            {"min_value": 0.8, "max_value": 1.0},
            column="email",
        )
        assert expectation is not None
        assert "ExpectColumnProportionOfUniqueValuesToBeBetween" in type(expectation).__name__

    def test_build_distinct_values_in_set(self) -> None:
        """Test building distinct values in set expectation."""
        expectation = build_expectation(
            CheckType.DISTINCT_VALUES_IN_SET,
            {"value_set": ["active", "inactive", "pending"]},
            column="status",
        )
        assert expectation is not None
        assert "ExpectColumnDistinctValuesToBeInSet" in type(expectation).__name__

    def test_build_most_common_value(self) -> None:
        """Test building most common value expectation."""
        expectation = build_expectation(
            CheckType.MOST_COMMON_VALUE,
            {"value_set": ["active", "pending"]},
            column="status",
        )
        assert expectation is not None
        assert "ExpectColumnMostCommonValueToBeInSet" in type(expectation).__name__

    # --- Ordering checks ---

    def test_build_values_increasing(self) -> None:
        """Test building values increasing expectation."""
        expectation = build_expectation(
            CheckType.VALUES_INCREASING,
            {"strictly": True},
            column="sequence_id",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeIncreasing" in type(expectation).__name__

    def test_build_values_decreasing(self) -> None:
        """Test building values decreasing expectation."""
        expectation = build_expectation(
            CheckType.VALUES_DECREASING,
            {"strictly": False},
            column="countdown",
        )
        assert expectation is not None
        assert "ExpectColumnValuesToBeDecreasing" in type(expectation).__name__

    # --- Multi-column checks ---

    def test_build_column_pair_equal(self) -> None:
        """Test building column pair equality expectation."""
        expectation = build_expectation(
            CheckType.COLUMN_PAIR_EQUAL,
            {"column_b": "billing_address"},
            column="shipping_address",
        )
        assert expectation is not None
        assert "ExpectColumnPairValuesToBeEqual" in type(expectation).__name__

    def test_build_composite_key_unique(self) -> None:
        """Test building composite key uniqueness expectation."""
        expectation = build_expectation(
            CheckType.COMPOSITE_KEY_UNIQUE,
            {"column_list": ["org_id", "user_id"]},
        )
        assert expectation is not None
        assert "ExpectCompoundColumnsToBeUnique" in type(expectation).__name__

    def test_build_multicolumn_unique(self) -> None:
        """Test building multi-column uniqueness expectation."""
        expectation = build_expectation(
            CheckType.MULTICOLUMN_UNIQUE,
            {"column_list": ["first_name", "last_name", "email"]},
        )
        assert expectation is not None
        # Uses ExpectCompoundColumnsToBeUnique under the hood
        assert "ExpectCompoundColumnsToBeUnique" in type(expectation).__name__
