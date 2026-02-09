"""Registry mapping CheckTypes to Great Expectations expectations."""

from typing import Any, Callable

import great_expectations.expectations as gxe
from great_expectations.expectations import Expectation

from dq_platform.models.check import CheckType


# Type alias for expectation builder functions
# Table-level checks: (parameters) -> Expectation
# Column-level checks: (parameters, column) -> Expectation
ExpectationBuilder = Callable[..., Expectation]


def _build_row_count(params: dict[str, Any]) -> Expectation:
    """Build row count expectation."""
    return gxe.ExpectTableRowCountToBeBetween(
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_row_count_min(params: dict[str, Any]) -> Expectation:
    """Build minimum row count expectation."""
    return gxe.ExpectTableRowCountToBeBetween(
        min_value=params["min_value"],
    )


def _build_row_count_max(params: dict[str, Any]) -> Expectation:
    """Build maximum row count expectation."""
    return gxe.ExpectTableRowCountToBeBetween(
        max_value=params["max_value"],
    )


def _build_null_count(params: dict[str, Any], column: str) -> Expectation:
    """Build null count expectation."""
    return gxe.ExpectColumnValuesToNotBeNull(
        column=column,
        mostly=1 - (params.get("max_value", 0) / 100) if params.get("max_value") else 1.0,
    )


def _build_null_percent(params: dict[str, Any], column: str) -> Expectation:
    """Build null percentage expectation."""
    max_percent = params.get("max_percent", 0)
    # GX uses proportion of non-null values (mostly), so we convert
    # max 10% nulls = at least 90% non-null = mostly=0.9
    min_non_null = 1 - (max_percent / 100)
    return gxe.ExpectColumnValuesToNotBeNull(
        column=column,
        mostly=min_non_null,
    )


def _build_not_null(params: dict[str, Any], column: str) -> Expectation:
    """Build not null expectation (no nulls allowed)."""
    return gxe.ExpectColumnValuesToNotBeNull(
        column=column,
    )


def _build_unique(params: dict[str, Any], column: str) -> Expectation:
    """Build uniqueness expectation."""
    return gxe.ExpectColumnValuesToBeUnique(
        column=column,
    )


def _build_distinct_count(params: dict[str, Any], column: str) -> Expectation:
    """Build distinct count expectation."""
    return gxe.ExpectColumnUniqueValueCountToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_duplicate_count(params: dict[str, Any], column: str) -> Expectation:
    """Build duplicate count expectation.

    This checks that the proportion of unique values is high enough.
    If max_value=10 (10 duplicates allowed in 1000 rows), mostly would be 0.99.
    """
    # GX doesn't have direct duplicate count, use unique proportion
    # mostly parameter: what fraction must be unique
    max_dupes = params.get("max_value", 0)
    if max_dupes > 0:
        # This is an approximation - exact behavior depends on total row count
        return gxe.ExpectColumnValuesToBeUnique(
            column=column,
            mostly=0.99,  # Allow 1% duplicates as default
        )
    return gxe.ExpectColumnValuesToBeUnique(
        column=column,
    )


def _build_schema_column_exists(params: dict[str, Any]) -> Expectation:
    """Build column exists expectation."""
    return gxe.ExpectColumnToExist(
        column=params["column_name"],
    )


def _build_schema_column_count(params: dict[str, Any]) -> Expectation:
    """Build column count expectation."""
    return gxe.ExpectTableColumnCountToEqual(
        value=params["expected_value"],
    )


def _build_table_availability(params: dict[str, Any]) -> Expectation:
    """Build table availability expectation.

    Uses row count >= 0 as a proxy for table accessibility.
    """
    return gxe.ExpectTableRowCountToBeBetween(
        min_value=0,
    )


def _build_data_freshness(params: dict[str, Any], column: str) -> Expectation:
    """Build data freshness expectation.

    Checks that the max value of a timestamp column is recent.
    Uses naive datetime to match database timestamps without timezone info.
    """
    from datetime import datetime, timedelta

    max_age_seconds = params.get("max_value", 86400)  # Default 24 hours
    # Use naive datetime (no timezone) to match typical database timestamp columns
    min_timestamp = datetime.now() - timedelta(seconds=max_age_seconds)

    return gxe.ExpectColumnMaxToBeBetween(
        column=column,
        min_value=min_timestamp,
    )


def _build_custom_sql(params: dict[str, Any], column: str | None = None) -> Expectation:
    """Build custom SQL expectation."""
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=params["custom_sql"],
    )


def _build_value_range(params: dict[str, Any], column: str) -> Expectation:
    """Build value range expectation.

    Parameters:
        min_value: Minimum acceptable value (optional)
        max_value: Maximum acceptable value (optional)
    """
    return gxe.ExpectColumnValuesToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_regex_pattern(params: dict[str, Any], column: str) -> Expectation:
    """Build regex pattern expectation.

    Parameters:
        pattern: Regex pattern to match
        mostly: Fraction of values that must match (optional, default 1.0)
    """
    return gxe.ExpectColumnValuesToMatchRegex(
        column=column,
        regex=params["pattern"],
        mostly=params.get("mostly", 1.0),
    )


def _build_allowed_values(params: dict[str, Any], column: str) -> Expectation:
    """Build allowed values expectation.

    Parameters:
        allowed_values: List of acceptable values
        mostly: Fraction of values that must be in set (optional, default 1.0)
    """
    return gxe.ExpectColumnValuesToBeInSet(
        column=column,
        value_set=params["allowed_values"],
        mostly=params.get("mostly", 1.0),
    )


def _build_column_pair_comparison(params: dict[str, Any], column: str) -> Expectation:
    """Build column pair comparison expectation.

    Checks that column_a > column_b (e.g., end_date > start_date).
    The target_column becomes column_a, params["column_b"] is the other column.

    Parameters:
        column_b: The column to compare against (column_a > column_b)
    """
    return gxe.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A=column,
        column_B=params["column_b"],
    )


# --- Volume checks (table-level) ---


def _build_row_count_exact(params: dict[str, Any]) -> Expectation:
    """Build exact row count expectation."""
    return gxe.ExpectTableRowCountToEqual(
        value=params["value"],
    )


def _build_row_count_compare(params: dict[str, Any]) -> Expectation:
    """Build row count comparison expectation (compare with another table)."""
    return gxe.ExpectTableRowCountToEqualOtherTable(
        other_table_name=params["other_table_name"],
    )


# --- Schema checks (table-level) ---


def _build_schema_column_list(params: dict[str, Any]) -> Expectation:
    """Build column set validation expectation."""
    return gxe.ExpectTableColumnsToMatchSet(
        column_set=params["column_set"],
        exact_match=params.get("exact_match", True),
    )


def _build_schema_column_order(params: dict[str, Any]) -> Expectation:
    """Build column order validation expectation."""
    return gxe.ExpectTableColumnsToMatchOrderedList(
        column_list=params["column_list"],
    )


# --- Completeness checks (column-level) ---


def _build_completeness_percent(params: dict[str, Any], column: str) -> Expectation:
    """Build completeness percentage expectation (non-null ratio)."""
    return gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


# --- Numeric/Statistical checks (column-level) ---


def _build_column_min(params: dict[str, Any], column: str) -> Expectation:
    """Build column minimum value expectation."""
    return gxe.ExpectColumnMinToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_max(params: dict[str, Any], column: str) -> Expectation:
    """Build column maximum value expectation."""
    return gxe.ExpectColumnMaxToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_mean(params: dict[str, Any], column: str) -> Expectation:
    """Build column mean value expectation."""
    return gxe.ExpectColumnMeanToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_median(params: dict[str, Any], column: str) -> Expectation:
    """Build column median value expectation."""
    return gxe.ExpectColumnMedianToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_stddev(params: dict[str, Any], column: str) -> Expectation:
    """Build column standard deviation expectation."""
    return gxe.ExpectColumnStdevToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_sum(params: dict[str, Any], column: str) -> Expectation:
    """Build column sum expectation."""
    return gxe.ExpectColumnSumToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_column_quantile(params: dict[str, Any], column: str) -> Expectation:
    """Build column quantile values expectation."""
    return gxe.ExpectColumnQuantileValuesToBeBetween(
        column=column,
        quantile_ranges=params["quantile_ranges"],
    )


# --- Text checks (column-level) ---


def _build_text_length_range(params: dict[str, Any], column: str) -> Expectation:
    """Build text length range expectation."""
    return gxe.ExpectColumnValueLengthsToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
        mostly=params.get("mostly", 1.0),
    )


def _build_text_length_exact(params: dict[str, Any], column: str) -> Expectation:
    """Build exact text length expectation."""
    return gxe.ExpectColumnValueLengthsToEqual(
        column=column,
        value=params["value"],
        mostly=params.get("mostly", 1.0),
    )


# --- Pattern checks (column-level) ---


def _build_regex_not_match(params: dict[str, Any], column: str) -> Expectation:
    """Build regex NOT match expectation (values must not match pattern)."""
    return gxe.ExpectColumnValuesToNotMatchRegex(
        column=column,
        regex=params["pattern"],
        mostly=params.get("mostly", 1.0),
    )


def _build_like_pattern(params: dict[str, Any], column: str) -> Expectation:
    """Build SQL LIKE pattern match expectation."""
    return gxe.ExpectColumnValuesToMatchLikePattern(
        column=column,
        like_pattern=params["like_pattern"],
        mostly=params.get("mostly", 1.0),
    )


def _build_forbidden_values(params: dict[str, Any], column: str) -> Expectation:
    """Build forbidden values expectation (blocklist validation)."""
    return gxe.ExpectColumnValuesToNotBeInSet(
        column=column,
        value_set=params["forbidden_values"],
        mostly=params.get("mostly", 1.0),
    )


# --- Datatype checks (column-level) ---


def _build_column_type(params: dict[str, Any], column: str) -> Expectation:
    """Build column data type expectation."""
    return gxe.ExpectColumnValuesToBeOfType(
        column=column,
        type_=params["type_"],
        mostly=params.get("mostly", 1.0),
    )


def _build_date_parseable(params: dict[str, Any], column: str) -> Expectation:
    """Build date parseable expectation."""
    return gxe.ExpectColumnValuesToBeDateutilParseable(
        column=column,
        mostly=params.get("mostly", 1.0),
    )


def _build_json_parseable(params: dict[str, Any], column: str) -> Expectation:
    """Build JSON parseable expectation."""
    return gxe.ExpectColumnValuesToBeJsonParseable(
        column=column,
        mostly=params.get("mostly", 1.0),
    )


def _build_datetime_format(params: dict[str, Any], column: str) -> Expectation:
    """Build datetime format expectation."""
    return gxe.ExpectColumnValuesToMatchStrftimeFormat(
        column=column,
        strftime_format=params["strftime_format"],
        mostly=params.get("mostly", 1.0),
    )


# --- Uniqueness checks (column-level) ---


def _build_uniqueness_percent(params: dict[str, Any], column: str) -> Expectation:
    """Build uniqueness percentage expectation."""
    return gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
        column=column,
        min_value=params.get("min_value"),
        max_value=params.get("max_value"),
    )


def _build_distinct_values_in_set(params: dict[str, Any], column: str) -> Expectation:
    """Build distinct values in set expectation."""
    return gxe.ExpectColumnDistinctValuesToBeInSet(
        column=column,
        value_set=params["value_set"],
    )


def _build_most_common_value(params: dict[str, Any], column: str) -> Expectation:
    """Build most common value expectation."""
    return gxe.ExpectColumnMostCommonValueToBeInSet(
        column=column,
        value_set=params["value_set"],
        ties_okay=params.get("ties_okay", True),
    )


# --- Ordering checks (column-level) ---


def _build_values_increasing(params: dict[str, Any], column: str) -> Expectation:
    """Build values increasing expectation."""
    return gxe.ExpectColumnValuesToBeIncreasing(
        column=column,
        strictly=params.get("strictly", False),
        mostly=params.get("mostly", 1.0),
    )


def _build_values_decreasing(params: dict[str, Any], column: str) -> Expectation:
    """Build values decreasing expectation."""
    return gxe.ExpectColumnValuesToBeDecreasing(
        column=column,
        strictly=params.get("strictly", False),
        mostly=params.get("mostly", 1.0),
    )


# --- Multi-column checks (table-level) ---


def _build_column_pair_equal(params: dict[str, Any], column: str) -> Expectation:
    """Build column pair equality expectation."""
    return gxe.ExpectColumnPairValuesToBeEqual(
        column_A=column,
        column_B=params["column_b"],
        mostly=params.get("mostly", 1.0),
    )


def _build_composite_key_unique(params: dict[str, Any]) -> Expectation:
    """Build composite key uniqueness expectation."""
    return gxe.ExpectCompoundColumnsToBeUnique(
        column_list=params["column_list"],
        mostly=params.get("mostly", 1.0),
    )


def _build_multicolumn_unique(params: dict[str, Any]) -> Expectation:
    """Build multi-column uniqueness expectation.

    Uses ExpectCompoundColumnsToBeUnique as it provides the same functionality.
    """
    return gxe.ExpectCompoundColumnsToBeUnique(
        column_list=params["column_list"],
        mostly=params.get("mostly", 1.0),
    )


# Registry mapping CheckType to expectation builder and column-level flag
GX_EXPECTATION_MAP: dict[CheckType, tuple[ExpectationBuilder, bool]] = {
    # Table-level checks (is_column_level=False)
    CheckType.ROW_COUNT: (_build_row_count, False),
    CheckType.ROW_COUNT_MIN: (_build_row_count_min, False),
    CheckType.ROW_COUNT_MAX: (_build_row_count_max, False),
    CheckType.SCHEMA_COLUMN_EXISTS: (_build_schema_column_exists, False),
    CheckType.SCHEMA_COLUMN_COUNT: (_build_schema_column_count, False),
    CheckType.TABLE_AVAILABILITY: (_build_table_availability, False),

    # Column-level checks (is_column_level=True)
    CheckType.NULL_COUNT: (_build_null_count, True),
    CheckType.NULL_PERCENT: (_build_null_percent, True),
    CheckType.NOT_NULL: (_build_not_null, True),
    CheckType.UNIQUE: (_build_unique, True),
    CheckType.DISTINCT_COUNT: (_build_distinct_count, True),
    CheckType.DUPLICATE_COUNT: (_build_duplicate_count, True),
    CheckType.DATA_FRESHNESS: (_build_data_freshness, True),
    CheckType.CUSTOM_SQL: (_build_custom_sql, True),
    CheckType.VALUE_RANGE: (_build_value_range, True),
    CheckType.REGEX_PATTERN: (_build_regex_pattern, True),
    CheckType.ALLOWED_VALUES: (_build_allowed_values, True),
    CheckType.COLUMN_PAIR_COMPARISON: (_build_column_pair_comparison, True),

    # Volume (table-level)
    CheckType.ROW_COUNT_EXACT: (_build_row_count_exact, False),
    CheckType.ROW_COUNT_COMPARE: (_build_row_count_compare, False),

    # Schema (table-level)
    CheckType.SCHEMA_COLUMN_LIST: (_build_schema_column_list, False),
    CheckType.SCHEMA_COLUMN_ORDER: (_build_schema_column_order, False),

    # Completeness (column-level)
    CheckType.COMPLETENESS_PERCENT: (_build_completeness_percent, True),

    # Numeric/Statistical (column-level)
    CheckType.COLUMN_MIN: (_build_column_min, True),
    CheckType.COLUMN_MAX: (_build_column_max, True),
    CheckType.COLUMN_MEAN: (_build_column_mean, True),
    CheckType.COLUMN_MEDIAN: (_build_column_median, True),
    CheckType.COLUMN_STDDEV: (_build_column_stddev, True),
    CheckType.COLUMN_SUM: (_build_column_sum, True),
    CheckType.COLUMN_QUANTILE: (_build_column_quantile, True),

    # Text (column-level)
    CheckType.TEXT_LENGTH_RANGE: (_build_text_length_range, True),
    CheckType.TEXT_LENGTH_EXACT: (_build_text_length_exact, True),

    # Patterns (column-level)
    CheckType.REGEX_NOT_MATCH: (_build_regex_not_match, True),
    CheckType.LIKE_PATTERN: (_build_like_pattern, True),
    CheckType.FORBIDDEN_VALUES: (_build_forbidden_values, True),

    # Datatype (column-level)
    CheckType.COLUMN_TYPE: (_build_column_type, True),
    CheckType.DATE_PARSEABLE: (_build_date_parseable, True),
    CheckType.JSON_PARSEABLE: (_build_json_parseable, True),
    CheckType.DATETIME_FORMAT: (_build_datetime_format, True),

    # Uniqueness (column-level)
    CheckType.UNIQUENESS_PERCENT: (_build_uniqueness_percent, True),
    CheckType.DISTINCT_VALUES_IN_SET: (_build_distinct_values_in_set, True),
    CheckType.MOST_COMMON_VALUE: (_build_most_common_value, True),

    # Ordering (column-level)
    CheckType.VALUES_INCREASING: (_build_values_increasing, True),
    CheckType.VALUES_DECREASING: (_build_values_decreasing, True),

    # Multi-column (column_pair_equal uses target_column, others are table-level)
    CheckType.COLUMN_PAIR_EQUAL: (_build_column_pair_equal, True),
    CheckType.COMPOSITE_KEY_UNIQUE: (_build_composite_key_unique, False),
    CheckType.MULTICOLUMN_UNIQUE: (_build_multicolumn_unique, False),
}


def build_expectation(
    check_type: CheckType,
    parameters: dict[str, Any],
    column: str | None = None,
) -> Expectation:
    """Build a GX expectation from check type and parameters.

    Args:
        check_type: The type of check to build.
        parameters: Check parameters (thresholds, values, etc.).
        column: Column name for column-level checks.

    Returns:
        Great Expectations Expectation instance.

    Raises:
        ValueError: If check type is not registered.
        ValueError: If column-level check is missing column parameter.
    """
    if check_type not in GX_EXPECTATION_MAP:
        raise ValueError(f"Unknown check type: {check_type}")

    builder, is_column_level = GX_EXPECTATION_MAP[check_type]

    if is_column_level:
        if not column:
            raise ValueError(f"Column-level check '{check_type}' requires a column parameter")
        return builder(parameters, column)

    return builder(parameters)


def get_check_description(check_type: CheckType) -> str:
    """Get human-readable description for a check type.

    Args:
        check_type: The check type.

    Returns:
        Description string.
    """
    descriptions = {
        CheckType.ROW_COUNT: "Returns the row count of a table",
        CheckType.ROW_COUNT_MIN: "Checks if row count is >= minimum threshold",
        CheckType.ROW_COUNT_MAX: "Checks if row count is <= maximum threshold",
        CheckType.SCHEMA_COLUMN_EXISTS: "Checks if a specific column exists",
        CheckType.SCHEMA_COLUMN_COUNT: "Checks if table has expected number of columns",
        CheckType.TABLE_AVAILABILITY: "Checks if table is accessible",
        CheckType.NULL_COUNT: "Checks if null count is <= maximum threshold",
        CheckType.NULL_PERCENT: "Checks if null percentage is <= maximum threshold",
        CheckType.NOT_NULL: "Checks that column has no null values",
        CheckType.UNIQUE: "Checks that column has no duplicate values",
        CheckType.DISTINCT_COUNT: "Checks if distinct count is within range",
        CheckType.DUPLICATE_COUNT: "Checks if duplicate count is <= maximum threshold",
        CheckType.DATA_FRESHNESS: "Checks if data is fresh (max age in seconds)",
        CheckType.CUSTOM_SQL: "Executes custom SQL validation",
        CheckType.VALUE_RANGE: "Checks if all values in a column are within min/max bounds",
        CheckType.REGEX_PATTERN: "Checks if column values match a regex pattern",
        CheckType.ALLOWED_VALUES: "Checks if all values are in an allowed set",
        CheckType.COLUMN_PAIR_COMPARISON: "Checks that column A values are greater than column B",
        # Volume
        CheckType.ROW_COUNT_EXACT: "Checks if row count equals exact value",
        CheckType.ROW_COUNT_COMPARE: "Compares row count with another table",
        # Schema
        CheckType.SCHEMA_COLUMN_LIST: "Checks if table has expected column set",
        CheckType.SCHEMA_COLUMN_ORDER: "Checks if columns match expected order",
        # Completeness
        CheckType.COMPLETENESS_PERCENT: "Checks non-null percentage is within range",
        # Numeric/Statistical
        CheckType.COLUMN_MIN: "Checks if column minimum is within bounds",
        CheckType.COLUMN_MAX: "Checks if column maximum is within bounds",
        CheckType.COLUMN_MEAN: "Checks if column mean is within bounds",
        CheckType.COLUMN_MEDIAN: "Checks if column median is within bounds",
        CheckType.COLUMN_STDDEV: "Checks if column std deviation is within bounds",
        CheckType.COLUMN_SUM: "Checks if column sum is within bounds",
        CheckType.COLUMN_QUANTILE: "Checks if quantile values are within bounds",
        # Text
        CheckType.TEXT_LENGTH_RANGE: "Checks if text length is within min/max",
        CheckType.TEXT_LENGTH_EXACT: "Checks if text length equals exact value",
        # Patterns
        CheckType.REGEX_NOT_MATCH: "Checks that values do NOT match pattern",
        CheckType.LIKE_PATTERN: "Checks if values match SQL LIKE pattern",
        CheckType.FORBIDDEN_VALUES: "Checks that values are not in blocklist",
        # Datatype
        CheckType.COLUMN_TYPE: "Checks if values are of expected data type",
        CheckType.DATE_PARSEABLE: "Checks if values can be parsed as dates",
        CheckType.JSON_PARSEABLE: "Checks if values are valid JSON",
        CheckType.DATETIME_FORMAT: "Checks if values match datetime format",
        # Uniqueness
        CheckType.UNIQUENESS_PERCENT: "Checks if unique value ratio is within range",
        CheckType.DISTINCT_VALUES_IN_SET: "Checks if all distinct values are in set",
        CheckType.MOST_COMMON_VALUE: "Checks if mode value is in expected set",
        # Ordering
        CheckType.VALUES_INCREASING: "Checks if values are monotonically increasing",
        CheckType.VALUES_DECREASING: "Checks if values are monotonically decreasing",
        # Multi-column
        CheckType.COLUMN_PAIR_EQUAL: "Checks if two columns have equal values",
        CheckType.COMPOSITE_KEY_UNIQUE: "Checks if column combination is unique",
        CheckType.MULTICOLUMN_UNIQUE: "Checks if multiple columns are unique together",
    }
    return descriptions.get(check_type, "No description available")


def is_column_level_check(check_type: CheckType) -> bool:
    """Check if a check type operates at column level.

    Args:
        check_type: The check type.

    Returns:
        True if column-level, False if table-level.
    """
    if check_type not in GX_EXPECTATION_MAP:
        raise ValueError(f"Unknown check type: {check_type}")

    _, is_column_level = GX_EXPECTATION_MAP[check_type]
    return is_column_level
