"""DQOps-style check definitions combining sensors and rules.

This module defines high-level check types that map to sensor + rule combinations,
following the DQOps architecture pattern.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from dq_platform.checks.rules import RuleType
from dq_platform.checks.sensors import SensorType


class DQOpsCheckType(str, Enum):
    """DQOps-style check types."""

    # Volume checks (table-level)
    ROW_COUNT = "row_count"
    ROW_COUNT_CHANGE_1_DAY = "row_count_change_1_day"
    ROW_COUNT_CHANGE_7_DAYS = "row_count_change_7_days"
    ROW_COUNT_CHANGE_30_DAYS = "row_count_change_30_days"

    # Schema checks (table-level)
    COLUMN_COUNT = "column_count"
    COLUMN_EXISTS = "column_exists"
    COLUMN_COUNT_CHANGED = "column_count_changed"

    # Timeliness checks (table-level)
    DATA_FRESHNESS = "data_freshness"
    DATA_STALENESS = "data_staleness"

    # Null/Completeness checks (column-level)
    NULLS_COUNT = "nulls_count"
    NULLS_PERCENT = "nulls_percent"
    NOT_NULLS_COUNT = "not_nulls_count"
    NOT_NULLS_PERCENT = "not_nulls_percent"
    EMPTY_COLUMN_FOUND = "empty_column_found"

    # Uniqueness checks (column-level)
    DISTINCT_COUNT = "distinct_count"
    DISTINCT_PERCENT = "distinct_percent"
    DUPLICATE_COUNT = "duplicate_count"
    DUPLICATE_PERCENT = "duplicate_percent"

    # Numeric/Statistical checks (column-level)
    NUMBER_BELOW_MIN_VALUE = "number_below_min_value"
    NUMBER_ABOVE_MAX_VALUE = "number_above_max_value"
    NUMBER_BELOW_MIN_VALUE_PERCENT = "number_below_min_value_percent"
    NUMBER_ABOVE_MAX_VALUE_PERCENT = "number_above_max_value_percent"
    NUMBER_IN_RANGE_PERCENT = "number_in_range_percent"
    INTEGER_IN_RANGE_PERCENT = "integer_in_range_percent"
    MIN_IN_RANGE = "min_in_range"
    MAX_IN_RANGE = "max_in_range"
    SUM_IN_RANGE = "sum_in_range"
    MEAN_IN_RANGE = "mean_in_range"
    MEDIAN_IN_RANGE = "median_in_range"
    SAMPLE_STDDEV_IN_RANGE = "sample_stddev_in_range"
    POPULATION_STDDEV_IN_RANGE = "population_stddev_in_range"
    SAMPLE_VARIANCE_IN_RANGE = "sample_variance_in_range"
    POPULATION_VARIANCE_IN_RANGE = "population_variance_in_range"
    PERCENTILE_IN_RANGE = "percentile_in_range"
    PERCENTILE_10_IN_RANGE = "percentile_10_in_range"
    PERCENTILE_25_IN_RANGE = "percentile_25_in_range"
    PERCENTILE_75_IN_RANGE = "percentile_75_in_range"
    PERCENTILE_90_IN_RANGE = "percentile_90_in_range"
    NEGATIVE_VALUES = "negative_values"
    NEGATIVE_VALUES_PERCENT = "negative_values_percent"
    NON_NEGATIVE_VALUES = "non_negative_values"
    NON_NEGATIVE_VALUES_PERCENT = "non_negative_values_percent"

    # Text/Pattern checks (column-level)
    TEXT_MIN_LENGTH = "text_min_length"
    TEXT_MAX_LENGTH = "text_max_length"
    TEXT_MEAN_LENGTH = "text_mean_length"
    TEXT_LENGTH_BELOW_MIN_LENGTH = "text_length_below_min_length"
    TEXT_LENGTH_ABOVE_MAX_LENGTH = "text_length_above_max_length"
    TEXT_LENGTH_IN_RANGE_PERCENT = "text_length_in_range_percent"
    EMPTY_TEXT_FOUND = "empty_text_found"
    EMPTY_TEXT_PERCENT = "empty_text_percent"
    WHITESPACE_TEXT_FOUND = "whitespace_text_found"
    WHITESPACE_TEXT_PERCENT = "whitespace_text_percent"
    NULL_PLACEHOLDER_TEXT_FOUND = "null_placeholder_text_found"
    NULL_PLACEHOLDER_TEXT_PERCENT = "null_placeholder_text_percent"
    TEXT_SURROUNDED_BY_WHITESPACE_FOUND = "text_surrounded_by_whitespace_found"
    TEXT_SURROUNDED_BY_WHITESPACE_PERCENT = "text_surrounded_by_whitespace_percent"
    TEXTS_NOT_MATCHING_REGEX_PERCENT = "texts_not_matching_regex_percent"
    TEXT_MATCHING_REGEX_PERCENT = "text_matching_regex_percent"
    TEXT_NOT_MATCHING_REGEX_FOUND = "text_not_matching_regex_found"
    MIN_WORD_COUNT = "min_word_count"
    MAX_WORD_COUNT = "max_word_count"

    # Geographic checks (column-level)
    INVALID_LATITUDE = "invalid_latitude"
    INVALID_LONGITUDE = "invalid_longitude"
    VALID_LATITUDE_PERCENT = "valid_latitude_percent"
    VALID_LONGITUDE_PERCENT = "valid_longitude_percent"

    # Boolean checks (column-level)
    TRUE_PERCENT = "true_percent"
    FALSE_PERCENT = "false_percent"

    # DateTime checks (column-level)
    DATE_VALUES_IN_FUTURE_PERCENT = "date_values_in_future_percent"
    DATE_IN_RANGE_PERCENT = "date_in_range_percent"

    # Pattern/Format checks (column-level)
    INVALID_EMAIL_FORMAT_FOUND = "invalid_email_format_found"
    INVALID_EMAIL_FORMAT_PERCENT = "invalid_email_format_percent"
    INVALID_UUID_FORMAT_FOUND = "invalid_uuid_format_found"
    INVALID_UUID_FORMAT_PERCENT = "invalid_uuid_format_percent"
    INVALID_IP4_FORMAT_FOUND = "invalid_ip4_format_found"
    INVALID_IP4_FORMAT_PERCENT = "invalid_ip4_format_percent"
    INVALID_IP6_FORMAT_FOUND = "invalid_ip6_format_found"
    INVALID_IP6_FORMAT_PERCENT = "invalid_ip6_format_percent"
    INVALID_USA_PHONE_FORMAT_FOUND = "invalid_usa_phone_format_found"
    INVALID_USA_PHONE_FORMAT_PERCENT = "invalid_usa_phone_format_percent"
    INVALID_USA_ZIPCODE_FORMAT_FOUND = "invalid_usa_zipcode_format_found"
    INVALID_USA_ZIPCODE_FORMAT_PERCENT = "invalid_usa_zipcode_format_percent"

    # Phase 6: PII Detection checks
    CONTAINS_USA_PHONE_PERCENT = "contains_usa_phone_percent"
    CONTAINS_EMAIL_PERCENT = "contains_email_percent"
    CONTAINS_USA_ZIPCODE_PERCENT = "contains_usa_zipcode_percent"
    CONTAINS_IP4_PERCENT = "contains_ip4_percent"
    CONTAINS_IP6_PERCENT = "contains_ip6_percent"

    # Phase 4: Accepted Values & Domain checks
    TEXT_FOUND_IN_SET_PERCENT = "text_found_in_set_percent"
    NUMBER_FOUND_IN_SET_PERCENT = "number_found_in_set_percent"
    EXPECTED_TEXT_VALUES_IN_USE_COUNT = "expected_text_values_in_use_count"
    EXPECTED_NUMBERS_IN_USE_COUNT = "expected_numbers_in_use_count"
    EXPECTED_TEXTS_IN_TOP_VALUES_COUNT = "expected_texts_in_top_values_count"
    TEXT_VALID_COUNTRY_CODE_PERCENT = "text_valid_country_code_percent"
    TEXT_VALID_CURRENCY_CODE_PERCENT = "text_valid_currency_code_percent"

    # Phase 5: Date Pattern & Data Type Detection checks
    TEXT_NOT_MATCHING_DATE_PATTERN_FOUND = "text_not_matching_date_pattern_found"
    TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT = "text_not_matching_date_pattern_percent"
    TEXT_MATCH_DATE_FORMAT_PERCENT = "text_match_date_format_percent"
    TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT = "text_not_matching_name_pattern_percent"
    TEXT_PARSABLE_TO_BOOLEAN_PERCENT = "text_parsable_to_boolean_percent"
    TEXT_PARSABLE_TO_INTEGER_PERCENT = "text_parsable_to_integer_percent"
    TEXT_PARSABLE_TO_FLOAT_PERCENT = "text_parsable_to_float_percent"
    TEXT_PARSABLE_TO_DATE_PERCENT = "text_parsable_to_date_percent"
    DETECTED_DATATYPE_IN_TEXT = "detected_datatype_in_text"
    DETECTED_DATATYPE_IN_TEXT_CHANGED = "detected_datatype_in_text_changed"

    # Phase 7: Change Detection checks
    NULLS_PERCENT_CHANGE_1_DAY = "nulls_percent_change_1_day"
    NULLS_PERCENT_CHANGE_7_DAYS = "nulls_percent_change_7_days"
    NULLS_PERCENT_CHANGE_30_DAYS = "nulls_percent_change_30_days"
    DISTINCT_COUNT_CHANGE_1_DAY = "distinct_count_change_1_day"
    DISTINCT_COUNT_CHANGE_7_DAYS = "distinct_count_change_7_days"
    DISTINCT_COUNT_CHANGE_30_DAYS = "distinct_count_change_30_days"
    DISTINCT_PERCENT_CHANGE_1_DAY = "distinct_percent_change_1_day"
    DISTINCT_PERCENT_CHANGE_7_DAYS = "distinct_percent_change_7_days"
    DISTINCT_PERCENT_CHANGE_30_DAYS = "distinct_percent_change_30_days"
    MEAN_CHANGE_1_DAY = "mean_change_1_day"
    MEAN_CHANGE_7_DAYS = "mean_change_7_days"
    MEAN_CHANGE_30_DAYS = "mean_change_30_days"
    MEDIAN_CHANGE_1_DAY = "median_change_1_day"
    MEDIAN_CHANGE_7_DAYS = "median_change_7_days"
    MEDIAN_CHANGE_30_DAYS = "median_change_30_days"
    SUM_CHANGE_1_DAY = "sum_change_1_day"
    SUM_CHANGE_7_DAYS = "sum_change_7_days"
    SUM_CHANGE_30_DAYS = "sum_change_30_days"

    # Phase 8: Cross-Table Comparison checks
    TOTAL_ROW_COUNT_MATCH_PERCENT = "total_row_count_match_percent"
    TOTAL_SUM_MATCH_PERCENT = "total_sum_match_percent"
    TOTAL_MIN_MATCH_PERCENT = "total_min_match_percent"
    TOTAL_MAX_MATCH_PERCENT = "total_max_match_percent"
    TOTAL_AVERAGE_MATCH_PERCENT = "total_average_match_percent"
    TOTAL_NOT_NULL_COUNT_MATCH_PERCENT = "total_not_null_count_match_percent"

    # Phase 9: Table-Level Misc checks
    TABLE_AVAILABILITY = "table_availability"
    DATA_INGESTION_DELAY = "data_ingestion_delay"
    RELOAD_LAG = "reload_lag"
    SQL_CONDITION_PASSED_PERCENT_ON_TABLE = "sql_condition_passed_percent_on_table"
    COLUMN_TYPE_CHANGED = "column_type_changed"

    # Referential integrity checks (column-level)
    FOREIGN_KEY_NOT_FOUND = "foreign_key_not_found"
    FOREIGN_KEY_FOUND_PERCENT = "foreign_key_found_percent"

    # Table-level uniqueness checks
    DUPLICATE_RECORD_COUNT = "duplicate_record_count"
    DUPLICATE_RECORD_PERCENT = "duplicate_record_percent"

    # Custom SQL checks
    SQL_CONDITION_FAILED_ON_TABLE = "sql_condition_failed_on_table"
    SQL_AGGREGATE_EXPRESSION_ON_TABLE = "sql_aggregate_expression_on_table"

    # Phase 10: Text length percent checks (column-level)
    TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT = "text_length_below_min_length_percent"
    TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT = "text_length_above_max_length_percent"

    # Phase 10: Column-level custom SQL checks
    SQL_CONDITION_FAILED_ON_COLUMN = "sql_condition_failed_on_column"
    SQL_CONDITION_PASSED_PERCENT_ON_COLUMN = "sql_condition_passed_percent_on_column"
    SQL_AGGREGATE_EXPRESSION_ON_COLUMN = "sql_aggregate_expression_on_column"
    SQL_INVALID_VALUE_COUNT_ON_COLUMN = "sql_invalid_value_count_on_column"
    IMPORT_CUSTOM_RESULT_ON_COLUMN = "import_custom_result_on_column"

    # Phase 10: Table-level custom SQL checks
    SQL_INVALID_RECORD_COUNT_ON_TABLE = "sql_invalid_record_count_on_table"

    # Phase 10: Schema detection checks (table-level)
    COLUMN_LIST_CHANGED = "column_list_changed"
    COLUMN_LIST_OR_ORDER_CHANGED = "column_list_or_order_changed"
    COLUMN_TYPES_CHANGED = "column_types_changed"

    # Phase 11: Import external results (table-level)
    IMPORT_CUSTOM_RESULT_ON_TABLE = "import_custom_result_on_table"

    # Phase 11: Generic change detection checks
    ROW_COUNT_CHANGE = "row_count_change"
    NULLS_PERCENT_CHANGE = "nulls_percent_change"
    DISTINCT_COUNT_CHANGE = "distinct_count_change"
    DISTINCT_PERCENT_CHANGE = "distinct_percent_change"
    MEAN_CHANGE = "mean_change"
    MEDIAN_CHANGE = "median_change"
    SUM_CHANGE = "sum_change"

    # Phase 12: Anomaly detection checks
    ROW_COUNT_ANOMALY = "row_count_anomaly"
    DATA_FRESHNESS_ANOMALY = "data_freshness_anomaly"
    NULLS_PERCENT_ANOMALY = "nulls_percent_anomaly"
    DISTINCT_COUNT_ANOMALY = "distinct_count_anomaly"
    DISTINCT_PERCENT_ANOMALY = "distinct_percent_anomaly"
    SUM_ANOMALY = "sum_anomaly"
    MEAN_ANOMALY = "mean_anomaly"
    MEDIAN_ANOMALY = "median_anomaly"
    MIN_ANOMALY = "min_anomaly"
    MAX_ANOMALY = "max_anomaly"

    # Phase 12: Cross-source comparison checks
    ROW_COUNT_MATCH = "row_count_match"
    COLUMN_COUNT_MATCH = "column_count_match"
    SUM_MATCH = "sum_match"
    MIN_MATCH = "min_match"
    MAX_MATCH = "max_match"
    MEAN_MATCH = "mean_match"
    NOT_NULL_COUNT_MATCH = "not_null_count_match"
    NULL_COUNT_MATCH = "null_count_match"
    DISTINCT_COUNT_MATCH = "distinct_count_match"


@dataclass
class DQOpsCheck:
    """A DQOps-style check definition."""

    name: str
    description: str
    category: str  # e.g., "volume", "nulls", "uniqueness"
    sensor_type: SensorType
    rule_type: RuleType
    is_column_level: bool
    default_params: dict[str, Any] | None = None


# =============================================================================
# Volume Checks (Table-level)
# =============================================================================

ROW_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT,
    description="Check that table row count is within expected range",
    category="volume",
    sensor_type=SensorType.ROW_COUNT,
    rule_type=RuleType.MIN_MAX_COUNT,
    is_column_level=False,
    default_params={"min_count": 1},
)

ROW_COUNT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_CHANGE_1_DAY,
    description="Check that row count change from yesterday is within limit",
    category="volume",
    sensor_type=SensorType.ROW_COUNT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=False,
    default_params={"max_change_percent": 10.0},
)

ROW_COUNT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_CHANGE_7_DAYS,
    description="Check that row count change from 7 days ago is within limit",
    category="volume",
    sensor_type=SensorType.ROW_COUNT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=False,
    default_params={"max_change_percent": 20.0},
)

ROW_COUNT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_CHANGE_30_DAYS,
    description="Check that row count change from 30 days ago is within limit",
    category="volume",
    sensor_type=SensorType.ROW_COUNT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=False,
    default_params={"max_change_percent": 50.0},
)

# =============================================================================
# Schema Checks (Table-level)
# =============================================================================

COLUMN_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_COUNT,
    description="Check that table has expected number of columns",
    category="schema",
    sensor_type=SensorType.COLUMN_COUNT,
    rule_type=RuleType.MIN_MAX_COUNT,
    is_column_level=False,
)

COLUMN_EXISTS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_EXISTS,
    description="Check that a column exists in the table",
    category="schema",
    sensor_type=SensorType.COLUMN_EXISTS,
    rule_type=RuleType.IS_TRUE,
    is_column_level=True,
)

COLUMN_COUNT_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_COUNT_CHANGED,
    description="Check that column count has not changed unexpectedly",
    category="schema",
    sensor_type=SensorType.COLUMN_COUNT,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=False,
)

# =============================================================================
# Timeliness Checks (Table-level)
# =============================================================================

DATA_FRESHNESS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_FRESHNESS,
    description="Check that data is fresh (max age in seconds)",
    category="timeliness",
    sensor_type=SensorType.DATA_FRESHNESS,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 86400},  # 24 hours
)

DATA_STALENESS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_STALENESS,
    description="Check that data is not stale (max staleness in seconds)",
    category="timeliness",
    sensor_type=SensorType.DATA_STALENESS,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=False,
    default_params={"max_value": 86400},  # 24 hours
)

# =============================================================================
# Null/Completeness Checks (Column-level)
# =============================================================================

NULLS_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_COUNT,
    description="Check that null count is within limit",
    category="nulls",
    sensor_type=SensorType.NULLS_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NULLS_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT,
    description="Check that null percentage is within limit",
    category="nulls",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

NOT_NULLS_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULLS_COUNT,
    description="Check that non-null count meets minimum",
    category="nulls",
    sensor_type=SensorType.NOT_NULLS_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1},
)

NOT_NULLS_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULLS_PERCENT,
    description="Check that non-null percentage meets minimum",
    category="nulls",
    sensor_type=SensorType.NOT_NULLS_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

EMPTY_COLUMN_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_COLUMN_FOUND,
    description="Check that column is not completely empty",
    category="nulls",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 99.99},  # Allow at most 99.99% nulls
)

# =============================================================================
# Uniqueness Checks (Column-level)
# =============================================================================

DISTINCT_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT,
    description="Check that distinct count is within range",
    category="uniqueness",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.MIN_MAX_COUNT,
    is_column_level=True,
)

DISTINCT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT,
    description="Check that distinct percentage is within range",
    category="uniqueness",
    sensor_type=SensorType.DISTINCT_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

DUPLICATE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_COUNT,
    description="Check that duplicate count is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

DUPLICATE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_PERCENT,
    description="Check that duplicate percentage is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

# =============================================================================
# Numeric/Statistical Checks (Column-level)
# =============================================================================

MIN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_IN_RANGE,
    description="Check that column minimum is within range",
    category="numeric",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MAX_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_IN_RANGE,
    description="Check that column maximum is within range",
    category="numeric",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

SUM_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_IN_RANGE,
    description="Check that column sum is within range",
    category="numeric",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MEAN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_IN_RANGE,
    description="Check that column mean is within range",
    category="numeric",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MEDIAN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_IN_RANGE,
    description="Check that column median is within range",
    category="numeric",
    sensor_type=SensorType.MEDIAN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

NUMBER_BELOW_MIN_VALUE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_BELOW_MIN_VALUE,
    description="Check that count of values below min is within limit",
    category="numeric",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_VALUE,
    is_column_level=True,
)

NUMBER_ABOVE_MAX_VALUE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE,
    description="Check that count of values above max is within limit",
    category="numeric",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
)

NUMBER_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_IN_RANGE_PERCENT,
    description="Check that percentage of numbers in range meets minimum",
    category="numeric",
    sensor_type=SensorType.NUMBER_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_value": 0, "max_value": 100},
)

# =============================================================================
# Text/Pattern Checks (Column-level)
# =============================================================================

TEXT_MIN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MIN_LENGTH,
    description="Check that minimum text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MIN_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_MAX_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MAX_LENGTH,
    description="Check that maximum text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MAX_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_MEAN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MEAN_LENGTH,
    description="Check that average text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MEAN_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_LENGTH_BELOW_MIN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH,
    description="Check that count of texts below min length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_BELOW_MIN,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_LENGTH_ABOVE_MAX_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH,
    description="Check that count of texts above max length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_ABOVE_MAX,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_LENGTH_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_IN_RANGE_PERCENT,
    description="Check that percentage of texts with length in range meets minimum",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_length": 1, "max_length": 255},
)

EMPTY_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_TEXT_FOUND,
    description="Check that empty string count is within limit",
    category="text",
    sensor_type=SensorType.EMPTY_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

WHITESPACE_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.WHITESPACE_TEXT_FOUND,
    description="Check that whitespace-only text count is within limit",
    category="text",
    sensor_type=SensorType.WHITESPACE_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_NOT_MATCHING_REGEX_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_REGEX_FOUND,
    description="Check that count of values not matching regex is within limit",
    category="text",
    sensor_type=SensorType.REGEX_NOT_MATCH_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "regex_pattern": ".*"},
)

# =============================================================================
# Geographic Checks (Column-level)
# =============================================================================

INVALID_LATITUDE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_LATITUDE,
    description="Check that invalid latitude count is within limit",
    category="geographic",
    sensor_type=SensorType.INVALID_LATITUDE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_LONGITUDE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_LONGITUDE,
    description="Check that invalid longitude count is within limit",
    category="geographic",
    sensor_type=SensorType.INVALID_LONGITUDE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

# =============================================================================
# Boolean Checks (Column-level)
# =============================================================================

TRUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TRUE_PERCENT,
    description="Check that true percentage is within range",
    category="boolean",
    sensor_type=SensorType.TRUE_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

FALSE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FALSE_PERCENT,
    description="Check that false percentage is within range",
    category="boolean",
    sensor_type=SensorType.FALSE_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

# =============================================================================
# DateTime Checks (Column-level)
# =============================================================================

DATE_VALUES_IN_FUTURE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATE_VALUES_IN_FUTURE_PERCENT,
    description="Check that percentage of future dates is within limit",
    category="datetime",
    sensor_type=SensorType.FUTURE_DATE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

DATE_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATE_IN_RANGE_PERCENT,
    description="Check that percentage of dates is within valid range",
    category="datetime",
    sensor_type=SensorType.DATE_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_date": "1900-01-01", "max_date": "2099-12-31"},
)

# =============================================================================
# Pattern/Format Checks (Column-level)
# =============================================================================

INVALID_EMAIL_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_EMAIL_FORMAT_FOUND,
    description="Check that count of invalid email formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_EMAIL_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_EMAIL_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_EMAIL_FORMAT_PERCENT,
    description="Check that percentage of invalid email formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_EMAIL_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_UUID_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_UUID_FORMAT_FOUND,
    description="Check that count of invalid UUID formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_UUID_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_UUID_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_UUID_FORMAT_PERCENT,
    description="Check that percentage of invalid UUID formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_UUID_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_IP4_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP4_FORMAT_FOUND,
    description="Check that count of invalid IPv4 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP4_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_IP4_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP4_FORMAT_PERCENT,
    description="Check that percentage of invalid IPv4 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP4_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_IP6_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP6_FORMAT_FOUND,
    description="Check that count of invalid IPv6 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP6_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_IP6_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP6_FORMAT_PERCENT,
    description="Check that percentage of invalid IPv6 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP6_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_USA_PHONE_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_PHONE_FORMAT_FOUND,
    description="Check that count of invalid USA phone formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_PHONE_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_USA_PHONE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_PHONE_FORMAT_PERCENT,
    description="Check that percentage of invalid USA phone formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_PHONE_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_USA_ZIPCODE_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_FOUND,
    description="Check that count of invalid USA zipcode formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_USA_ZIPCODE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_PERCENT,
    description="Check that percentage of invalid USA zipcode formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

# =============================================================================
# Referential Integrity Checks (Column-level)
# =============================================================================

FOREIGN_KEY_NOT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FOREIGN_KEY_NOT_FOUND,
    description="Check that count of foreign keys not found in reference table is within limit",
    category="referential",
    sensor_type=SensorType.FOREIGN_KEY_NOT_FOUND_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "reference_schema": "public", "reference_column": "id"},
)

FOREIGN_KEY_FOUND_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FOREIGN_KEY_FOUND_PERCENT,
    description="Check that percentage of foreign keys found in reference table meets minimum",
    category="referential",
    sensor_type=SensorType.FOREIGN_KEY_FOUND_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Table-Level Uniqueness Checks
# =============================================================================

DUPLICATE_RECORD_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_RECORD_COUNT,
    description="Check that count of fully duplicate rows is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_RECORD_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=False,
    default_params={"max_count": 0},
)

DUPLICATE_RECORD_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_RECORD_PERCENT,
    description="Check that percentage of fully duplicate rows is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_RECORD_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=False,
    default_params={"max_percent": 0.0},
)

# =============================================================================
# Custom SQL Checks
# =============================================================================

SQL_CONDITION_FAILED_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_FAILED_ON_TABLE,
    description="Check that rows failing custom SQL condition are within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_FAILED_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=False,
    default_params={"max_count": 0, "sql_condition": "1=1"},
)

SQL_AGGREGATE_EXPRESSION_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_AGGREGATE_EXPRESSION_ON_TABLE,
    description="Check that custom SQL aggregate result is within range",
    category="custom_sql",
    sensor_type=SensorType.SQL_AGGREGATE_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=False,
    default_params={"sql_expression": "COUNT(*)"},
)

# =============================================================================
# Phase 1: Whitespace & Text Checks
# =============================================================================

EMPTY_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_TEXT_PERCENT,
    description="Check that empty text percentage is within limit",
    category="text",
    sensor_type=SensorType.EMPTY_TEXT_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

WHITESPACE_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.WHITESPACE_TEXT_PERCENT,
    description="Check that whitespace-only text percentage is within limit",
    category="text",
    sensor_type=SensorType.WHITESPACE_TEXT_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

NULL_PLACEHOLDER_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_PLACEHOLDER_TEXT_FOUND,
    description="Check that null placeholder text count is within limit",
    category="text",
    sensor_type=SensorType.NULL_PLACEHOLDER_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NULL_PLACEHOLDER_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_PLACEHOLDER_TEXT_PERCENT,
    description="Check that null placeholder text percentage is within limit",
    category="text",
    sensor_type=SensorType.NULL_PLACEHOLDER_TEXT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

TEXT_SURROUNDED_BY_WHITESPACE_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_FOUND,
    description="Check that text surrounded by whitespace count is within limit",
    category="text",
    sensor_type=SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_SURROUNDED_BY_WHITESPACE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_PERCENT,
    description="Check that text surrounded by whitespace percentage is within limit",
    category="text",
    sensor_type=SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

TEXTS_NOT_MATCHING_REGEX_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXTS_NOT_MATCHING_REGEX_PERCENT,
    description="Check that percentage of texts not matching regex is within limit",
    category="text",
    sensor_type=SensorType.REGEX_NOT_MATCH_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "regex_pattern": ".*"},
)

TEXT_MATCHING_REGEX_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MATCHING_REGEX_PERCENT,
    description="Check that percentage of texts matching regex meets minimum",
    category="text",
    sensor_type=SensorType.REGEX_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "regex_pattern": ".*"},
)

MIN_WORD_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_WORD_COUNT,
    description="Check that minimum word count meets expected value",
    category="text",
    sensor_type=SensorType.TEXT_WORD_COUNT_MIN,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MAX_WORD_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_WORD_COUNT,
    description="Check that maximum word count is within expected range",
    category="text",
    sensor_type=SensorType.TEXT_WORD_COUNT_MAX,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

# =============================================================================
# Phase 2: Geographic & Numeric Percent Checks
# =============================================================================

VALID_LATITUDE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.VALID_LATITUDE_PERCENT,
    description="Check that valid latitude percentage meets minimum",
    category="geographic",
    sensor_type=SensorType.VALID_LATITUDE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

VALID_LONGITUDE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.VALID_LONGITUDE_PERCENT,
    description="Check that valid longitude percentage meets minimum",
    category="geographic",
    sensor_type=SensorType.VALID_LONGITUDE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

NUMBER_BELOW_MIN_VALUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_BELOW_MIN_VALUE_PERCENT,
    description="Check that percentage of values below min is within limit",
    category="numeric",
    sensor_type=SensorType.NUMBER_BELOW_MIN_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "min_value": 0},
)

NUMBER_ABOVE_MAX_VALUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE_PERCENT,
    description="Check that percentage of values above max is within limit",
    category="numeric",
    sensor_type=SensorType.NUMBER_ABOVE_MAX_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "max_value": 100},
)

NEGATIVE_VALUES_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NEGATIVE_VALUES,
    description="Check that negative value count is within limit",
    category="numeric",
    sensor_type=SensorType.NEGATIVE_VALUE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NEGATIVE_VALUES_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NEGATIVE_VALUES_PERCENT,
    description="Check that negative value percentage is within limit",
    category="numeric",
    sensor_type=SensorType.NEGATIVE_VALUE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

NON_NEGATIVE_VALUES_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NON_NEGATIVE_VALUES,
    description="Check that non-negative value count meets minimum",
    category="numeric",
    sensor_type=SensorType.NON_NEGATIVE_VALUE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1},
)

NON_NEGATIVE_VALUES_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NON_NEGATIVE_VALUES_PERCENT,
    description="Check that non-negative value percentage meets minimum",
    category="numeric",
    sensor_type=SensorType.NON_NEGATIVE_VALUE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

INTEGER_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INTEGER_IN_RANGE_PERCENT,
    description="Check that percentage of integers in range meets minimum",
    category="numeric",
    sensor_type=SensorType.INTEGER_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_value": 0, "max_value": 100},
)

# =============================================================================
# Phase 3: Statistical & Percentile Checks
# =============================================================================

SAMPLE_STDDEV_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SAMPLE_STDDEV_IN_RANGE,
    description="Check that sample standard deviation is within expected range",
    category="statistical",
    sensor_type=SensorType.STDDEV_SAMPLE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

POPULATION_STDDEV_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.POPULATION_STDDEV_IN_RANGE,
    description="Check that population standard deviation is within expected range",
    category="statistical",
    sensor_type=SensorType.STDDEV_POPULATION,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

SAMPLE_VARIANCE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SAMPLE_VARIANCE_IN_RANGE,
    description="Check that sample variance is within expected range",
    category="statistical",
    sensor_type=SensorType.VARIANCE_SAMPLE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

POPULATION_VARIANCE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.POPULATION_VARIANCE_IN_RANGE,
    description="Check that population variance is within expected range",
    category="statistical",
    sensor_type=SensorType.VARIANCE_POPULATION,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_IN_RANGE,
    description="Check that percentile value is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"percentile": 0.5},
)

PERCENTILE_10_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_10_IN_RANGE,
    description="Check that 10th percentile is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_10,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_25_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_25_IN_RANGE,
    description="Check that 25th percentile (Q1) is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_25,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_75_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_75_IN_RANGE,
    description="Check that 75th percentile (Q3) is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_75,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_90_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_90_IN_RANGE,
    description="Check that 90th percentile is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_90,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

# =============================================================================
# Phase 4: Accepted Values & Domain Checks
# =============================================================================

TEXT_FOUND_IN_SET_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_FOUND_IN_SET_PERCENT,
    description="Check that percentage of texts in expected set meets minimum",
    category="accepted_values",
    sensor_type=SensorType.TEXT_IN_SET_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "expected_values": []},
)

NUMBER_FOUND_IN_SET_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_FOUND_IN_SET_PERCENT,
    description="Check that percentage of numbers in expected set meets minimum",
    category="accepted_values",
    sensor_type=SensorType.NUMBER_IN_SET_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "expected_values": []},
)

EXPECTED_TEXT_VALUES_IN_USE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_TEXT_VALUES_IN_USE_COUNT,
    description="Check that expected text values are actually used",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_TEXT_IN_USE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": []},
)

EXPECTED_NUMBERS_IN_USE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_NUMBERS_IN_USE_COUNT,
    description="Check that expected numbers are actually used",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_NUMBER_IN_USE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": []},
)

EXPECTED_TEXTS_IN_TOP_VALUES_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_TEXTS_IN_TOP_VALUES_COUNT,
    description="Check that expected texts appear in top N values",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_TEXTS_TOP_N_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": [], "top_n": 10},
)

TEXT_VALID_COUNTRY_CODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_VALID_COUNTRY_CODE_PERCENT,
    description="Check that valid country code percentage meets minimum",
    category="accepted_values",
    sensor_type=SensorType.VALID_COUNTRY_CODE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_VALID_CURRENCY_CODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_VALID_CURRENCY_CODE_PERCENT,
    description="Check that valid currency code percentage meets minimum",
    category="accepted_values",
    sensor_type=SensorType.VALID_CURRENCY_CODE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

# =============================================================================
# Phase 5: Date Pattern & Data Type Detection Checks
# =============================================================================

TEXT_NOT_MATCHING_DATE_PATTERN_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_FOUND,
    description="Check that count of texts not matching date pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_DATE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT,
    description="Check that percentage of texts not matching date pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_DATE_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_MATCH_DATE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MATCH_DATE_FORMAT_PERCENT,
    description="Check that percentage of texts matching date format meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_MATCH_DATE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT,
    description="Check that percentage of texts not matching name pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_NAME_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

TEXT_PARSABLE_TO_BOOLEAN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_BOOLEAN_PERCENT,
    description="Check that percentage of texts parsable to boolean meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_BOOLEAN_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_INTEGER_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_INTEGER_PERCENT,
    description="Check that percentage of texts parsable to integer meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_INTEGER_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_FLOAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_FLOAT_PERCENT,
    description="Check that percentage of texts parsable to float meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_FLOAT_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_DATE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_DATE_PERCENT,
    description="Check that percentage of texts parsable to date meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_DATE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

DETECTED_DATATYPE_IN_TEXT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT,
    description="Check that detected datatype matches expected",
    category="datatype",
    sensor_type=SensorType.DETECTED_DATATYPE,
    rule_type=RuleType.EQUAL_TO,
    is_column_level=True,
    default_params={"expected_value": 5},
)

DETECTED_DATATYPE_IN_TEXT_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT_CHANGED,
    description="Check that detected datatype has not changed",
    category="datatype",
    sensor_type=SensorType.DETECTED_DATATYPE_CHANGED,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=True,
    default_params={"forbidden_value": 1},
)

# =============================================================================
# Phase 6: PII Detection Checks
# =============================================================================

CONTAINS_USA_PHONE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_USA_PHONE_PERCENT,
    description="Check that percentage of values containing phone numbers is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_PHONE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_EMAIL_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_EMAIL_PERCENT,
    description="Check that percentage of values containing emails is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_EMAIL_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_USA_ZIPCODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_USA_ZIPCODE_PERCENT,
    description="Check that percentage of values containing zipcodes is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_ZIPCODE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_IP4_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_IP4_PERCENT,
    description="Check that percentage of values containing IPv4 is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_IP4_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_IP6_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_IP6_PERCENT,
    description="Check that percentage of values containing IPv6 is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_IP6_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

# =============================================================================
# Phase 7: Change Detection Checks
# =============================================================================

NULLS_PERCENT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_1_DAY,
    description="Check that nulls percent change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

NULLS_PERCENT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_7_DAYS,
    description="Check that nulls percent change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

NULLS_PERCENT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_30_DAYS,
    description="Check that nulls percent change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

DISTINCT_COUNT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_1_DAY,
    description="Check that distinct count change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

DISTINCT_COUNT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_7_DAYS,
    description="Check that distinct count change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

DISTINCT_COUNT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_30_DAYS,
    description="Check that distinct count change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

DISTINCT_PERCENT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_1_DAY,
    description="Check that distinct percent change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

DISTINCT_PERCENT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_7_DAYS,
    description="Check that distinct percent change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

DISTINCT_PERCENT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_30_DAYS,
    description="Check that distinct percent change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

MEAN_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_1_DAY,
    description="Check that mean change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

MEAN_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_7_DAYS,
    description="Check that mean change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

MEAN_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_30_DAYS,
    description="Check that mean change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

MEDIAN_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_1_DAY,
    description="Check that median change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

MEDIAN_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_7_DAYS,
    description="Check that median change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

MEDIAN_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_30_DAYS,
    description="Check that median change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

SUM_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_1_DAY,
    description="Check that sum change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

SUM_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_7_DAYS,
    description="Check that sum change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

SUM_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_30_DAYS,
    description="Check that sum change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

# =============================================================================
# Phase 8: Cross-Table Comparison Checks
# =============================================================================

TOTAL_ROW_COUNT_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_ROW_COUNT_MATCH_PERCENT,
    description="Check that row count matches reference table within tolerance",
    category="comparison",
    sensor_type=SensorType.ROW_COUNT_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 95.0, "reference_schema": "public"},
)

TOTAL_SUM_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_SUM_MATCH_PERCENT,
    description="Check that column sum matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.SUM_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_MIN_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_MIN_MATCH_PERCENT,
    description="Check that column minimum matches reference column",
    category="comparison",
    sensor_type=SensorType.MIN_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_MAX_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_MAX_MATCH_PERCENT,
    description="Check that column maximum matches reference column",
    category="comparison",
    sensor_type=SensorType.MAX_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_AVERAGE_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_AVERAGE_MATCH_PERCENT,
    description="Check that column average matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.AVERAGE_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={
        "min_percent": 95.0,
        "reference_schema": "public",
        "reference_column": "id",
        "tolerance_percent": 0.01,
    },
)

TOTAL_NOT_NULL_COUNT_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_NOT_NULL_COUNT_MATCH_PERCENT,
    description="Check that not-null count matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.NOT_NULL_COUNT_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Phase 9: Table-Level Misc Checks
# =============================================================================

TABLE_AVAILABILITY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TABLE_AVAILABILITY,
    description="Check that table is accessible",
    category="availability",
    sensor_type=SensorType.TABLE_AVAILABILITY,
    rule_type=RuleType.IS_TRUE,
    is_column_level=False,
)

DATA_INGESTION_DELAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_INGESTION_DELAY,
    description="Check that data ingestion delay is within acceptable limit",
    category="timeliness",
    sensor_type=SensorType.DATA_INGESTION_DELAY,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 3600},  # 1 hour
)

RELOAD_LAG_CHECK = DQOpsCheck(
    name=DQOpsCheckType.RELOAD_LAG,
    description="Check that table reload lag is within acceptable limit",
    category="timeliness",
    sensor_type=SensorType.RELOAD_LAG,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=False,
    default_params={"max_value": 86400},  # 24 hours
)

SQL_CONDITION_PASSED_PERCENT_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_TABLE,
    description="Check that percentage of rows passing SQL condition meets minimum",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_PASSED_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 95.0, "sql_condition": "1=1"},
)

COLUMN_TYPE_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_TYPE_CHANGED,
    description="Check that column type has not changed unexpectedly",
    category="schema",
    sensor_type=SensorType.COLUMN_TYPE_CHANGED,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=True,
    default_params={"forbidden_value": 1, "expected_type": "character varying"},
)


# =============================================================================
# Phase 10: Text Length Percent Checks (Column-level)
# =============================================================================

TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT,
    description="Check that percentage of text values below minimum length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_BELOW_MIN_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "min_length": 1},
)

TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT,
    description="Check that percentage of text values above maximum length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_ABOVE_MAX_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "max_length": 255},
)


# =============================================================================
# Phase 10: Column-level Custom SQL Checks
# =============================================================================

SQL_CONDITION_FAILED_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_FAILED_ON_COLUMN,
    description="Check that count of column values failing SQL condition is within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_FAILED_ON_COLUMN_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "sql_condition": "1=1"},
)

SQL_CONDITION_PASSED_PERCENT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_COLUMN,
    description="Check that percentage of column values passing SQL condition meets minimum",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_PASSED_ON_COLUMN_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "sql_condition": "1=1"},
)

SQL_AGGREGATE_EXPRESSION_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_AGGREGATE_EXPRESSION_ON_COLUMN,
    description="Check that SQL aggregate expression on column returns expected value",
    category="custom_sql",
    sensor_type=SensorType.SQL_AGGREGATE_ON_COLUMN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"sql_expression": "COUNT(*)"},
)

SQL_INVALID_VALUE_COUNT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_INVALID_VALUE_COUNT_ON_COLUMN,
    description="Check that count of invalid column values is within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_INVALID_VALUE_ON_COLUMN_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "invalid_values": "''"},
)

IMPORT_CUSTOM_RESULT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    description="Import and validate external data quality result for column",
    category="custom_sql",
    sensor_type=SensorType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"imported_value": 0.0},
)


# =============================================================================
# Phase 10: Table-level Custom SQL Checks
# =============================================================================

SQL_INVALID_RECORD_COUNT_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_INVALID_RECORD_COUNT_ON_TABLE,
    description="Check that count of invalid records matching SQL condition is within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_INVALID_RECORD_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=False,
    default_params={"max_count": 0, "sql_condition": "1=0"},
)


# =============================================================================
# Phase 10: Schema Detection Checks (Table-level)
# =============================================================================

COLUMN_LIST_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_LIST_CHANGED,
    description="Check that table column list has not changed (columns added/removed)",
    category="schema",
    sensor_type=SensorType.COLUMN_LIST_HASH,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=False,
    default_params={"forbidden_value": 1, "expected_hash": ""},
)

COLUMN_LIST_OR_ORDER_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_LIST_OR_ORDER_CHANGED,
    description="Check that table column list and order has not changed",
    category="schema",
    sensor_type=SensorType.COLUMN_LIST_OR_ORDER_HASH,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=False,
    default_params={"forbidden_value": 1, "expected_hash": ""},
)

COLUMN_TYPES_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_TYPES_CHANGED,
    description="Check that table column types have not changed",
    category="schema",
    sensor_type=SensorType.COLUMN_TYPES_HASH,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=False,
    default_params={"forbidden_value": 1, "expected_hash": ""},
)


# =============================================================================
# Phase 11: Import External Results Check (Table-level)
# =============================================================================

IMPORT_CUSTOM_RESULT_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.IMPORT_CUSTOM_RESULT_ON_TABLE,
    description="Import and validate external data quality result for table",
    category="custom_sql",
    sensor_type=SensorType.IMPORT_CUSTOM_RESULT_ON_TABLE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=False,
    default_params={"imported_value": 0.0},
)


# =============================================================================
# Phase 11: Generic Change Detection Checks
# =============================================================================

ROW_COUNT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_CHANGE,
    description="Check that row count change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.ROW_COUNT_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=False,
    default_params={"max_change_percent": 10.0, "baseline_count": 0},
)

NULLS_PERCENT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE,
    description="Check that null percentage change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 5.0, "baseline_percent": 0.0},
)

DISTINCT_COUNT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE,
    description="Check that distinct count change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_count": 0},
)

DISTINCT_PERCENT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE,
    description="Check that distinct percent change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 5.0, "baseline_percent": 0.0},
)

MEAN_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE,
    description="Check that mean value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.MEAN_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)

MEDIAN_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE,
    description="Check that median value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.MEDIAN_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)

SUM_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE,
    description="Check that sum value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.SUM_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)


# =============================================================================
# Anomaly Detection Checks (Phase 12)
# =============================================================================

ROW_COUNT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_ANOMALY,
    description="Detect anomalous row count using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.ROW_COUNT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=False,
    default_params={"anomaly_percent": 0.05},
)

DATA_FRESHNESS_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_FRESHNESS_ANOMALY,
    description="Detect anomalous data freshness using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DATA_FRESHNESS,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

NULLS_PERCENT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_ANOMALY,
    description="Detect anomalous null percentage using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

DISTINCT_COUNT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_ANOMALY,
    description="Detect anomalous distinct count using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

DISTINCT_PERCENT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_ANOMALY,
    description="Detect anomalous distinct percentage using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DISTINCT_PERCENT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

SUM_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_ANOMALY,
    description="Detect anomalous sum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MEAN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_ANOMALY,
    description="Detect anomalous mean value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MEDIAN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_ANOMALY,
    description="Detect anomalous median value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MEDIAN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MIN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_ANOMALY,
    description="Detect anomalous minimum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MAX_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_ANOMALY,
    description="Detect anomalous maximum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)


# =============================================================================
# Cross-Source Comparison Checks (Phase 12)
# =============================================================================

ROW_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_MATCH,
    description="Compare row count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.ROW_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 100.0},
)

COLUMN_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_COUNT_MATCH,
    description="Compare column count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.COLUMN_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 100.0},
)

SUM_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_MATCH,
    description="Compare sum of values between source and reference connection",
    category="comparison",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MIN_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_MATCH,
    description="Compare minimum value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MAX_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_MATCH,
    description="Compare maximum value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MEAN_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_MATCH,
    description="Compare mean value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

NOT_NULL_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULL_COUNT_MATCH,
    description="Compare not-null count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.NOT_NULLS_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

NULL_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_COUNT_MATCH,
    description="Compare null count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.NULLS_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

DISTINCT_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_MATCH,
    description="Compare distinct count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)


# =============================================================================
# Check Registry
# =============================================================================

CHECK_REGISTRY: dict[DQOpsCheckType, DQOpsCheck] = {
    # Volume
    DQOpsCheckType.ROW_COUNT: ROW_COUNT_CHECK,
    DQOpsCheckType.ROW_COUNT_CHANGE_1_DAY: ROW_COUNT_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.ROW_COUNT_CHANGE_7_DAYS: ROW_COUNT_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.ROW_COUNT_CHANGE_30_DAYS: ROW_COUNT_CHANGE_30_DAYS_CHECK,
    # Schema
    DQOpsCheckType.COLUMN_COUNT: COLUMN_COUNT_CHECK,
    DQOpsCheckType.COLUMN_EXISTS: COLUMN_EXISTS_CHECK,
    DQOpsCheckType.COLUMN_COUNT_CHANGED: COLUMN_COUNT_CHANGED_CHECK,
    # Timeliness
    DQOpsCheckType.DATA_FRESHNESS: DATA_FRESHNESS_CHECK,
    DQOpsCheckType.DATA_STALENESS: DATA_STALENESS_CHECK,
    # Nulls
    DQOpsCheckType.NULLS_COUNT: NULLS_COUNT_CHECK,
    DQOpsCheckType.NULLS_PERCENT: NULLS_PERCENT_CHECK,
    DQOpsCheckType.NOT_NULLS_COUNT: NOT_NULLS_COUNT_CHECK,
    DQOpsCheckType.NOT_NULLS_PERCENT: NOT_NULLS_PERCENT_CHECK,
    DQOpsCheckType.EMPTY_COLUMN_FOUND: EMPTY_COLUMN_FOUND_CHECK,
    # Uniqueness
    DQOpsCheckType.DISTINCT_COUNT: DISTINCT_COUNT_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT: DISTINCT_PERCENT_CHECK,
    DQOpsCheckType.DUPLICATE_COUNT: DUPLICATE_COUNT_CHECK,
    DQOpsCheckType.DUPLICATE_PERCENT: DUPLICATE_PERCENT_CHECK,
    # Numeric
    DQOpsCheckType.MIN_IN_RANGE: MIN_IN_RANGE_CHECK,
    DQOpsCheckType.MAX_IN_RANGE: MAX_IN_RANGE_CHECK,
    DQOpsCheckType.SUM_IN_RANGE: SUM_IN_RANGE_CHECK,
    DQOpsCheckType.MEAN_IN_RANGE: MEAN_IN_RANGE_CHECK,
    DQOpsCheckType.MEDIAN_IN_RANGE: MEDIAN_IN_RANGE_CHECK,
    # Text
    DQOpsCheckType.TEXT_MIN_LENGTH: TEXT_MIN_LENGTH_CHECK,
    DQOpsCheckType.TEXT_MAX_LENGTH: TEXT_MAX_LENGTH_CHECK,
    DQOpsCheckType.TEXT_MEAN_LENGTH: TEXT_MEAN_LENGTH_CHECK,
    DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH: TEXT_LENGTH_BELOW_MIN_LENGTH_CHECK,
    DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH: TEXT_LENGTH_ABOVE_MAX_LENGTH_CHECK,
    DQOpsCheckType.TEXT_LENGTH_IN_RANGE_PERCENT: TEXT_LENGTH_IN_RANGE_PERCENT_CHECK,
    DQOpsCheckType.EMPTY_TEXT_FOUND: EMPTY_TEXT_FOUND_CHECK,
    DQOpsCheckType.WHITESPACE_TEXT_FOUND: WHITESPACE_TEXT_FOUND_CHECK,
    DQOpsCheckType.TEXT_NOT_MATCHING_REGEX_FOUND: TEXT_NOT_MATCHING_REGEX_FOUND_CHECK,
    # Geographic
    DQOpsCheckType.INVALID_LATITUDE: INVALID_LATITUDE_CHECK,
    DQOpsCheckType.INVALID_LONGITUDE: INVALID_LONGITUDE_CHECK,
    # Boolean
    DQOpsCheckType.TRUE_PERCENT: TRUE_PERCENT_CHECK,
    DQOpsCheckType.FALSE_PERCENT: FALSE_PERCENT_CHECK,
    # DateTime
    DQOpsCheckType.DATE_VALUES_IN_FUTURE_PERCENT: DATE_VALUES_IN_FUTURE_PERCENT_CHECK,
    DQOpsCheckType.DATE_IN_RANGE_PERCENT: DATE_IN_RANGE_PERCENT_CHECK,
    # Pattern/Format
    DQOpsCheckType.INVALID_EMAIL_FORMAT_FOUND: INVALID_EMAIL_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_EMAIL_FORMAT_PERCENT: INVALID_EMAIL_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.INVALID_UUID_FORMAT_FOUND: INVALID_UUID_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_UUID_FORMAT_PERCENT: INVALID_UUID_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.INVALID_IP4_FORMAT_FOUND: INVALID_IP4_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_IP4_FORMAT_PERCENT: INVALID_IP4_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.INVALID_IP6_FORMAT_FOUND: INVALID_IP6_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_IP6_FORMAT_PERCENT: INVALID_IP6_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.INVALID_USA_PHONE_FORMAT_FOUND: INVALID_USA_PHONE_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_USA_PHONE_FORMAT_PERCENT: INVALID_USA_PHONE_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_FOUND: INVALID_USA_ZIPCODE_FORMAT_FOUND_CHECK,
    DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_PERCENT: INVALID_USA_ZIPCODE_FORMAT_PERCENT_CHECK,
    # Referential Integrity
    DQOpsCheckType.FOREIGN_KEY_NOT_FOUND: FOREIGN_KEY_NOT_FOUND_CHECK,
    DQOpsCheckType.FOREIGN_KEY_FOUND_PERCENT: FOREIGN_KEY_FOUND_PERCENT_CHECK,
    # Table-level Uniqueness
    DQOpsCheckType.DUPLICATE_RECORD_COUNT: DUPLICATE_RECORD_COUNT_CHECK,
    DQOpsCheckType.DUPLICATE_RECORD_PERCENT: DUPLICATE_RECORD_PERCENT_CHECK,
    # Phase 1: Whitespace & Text
    DQOpsCheckType.EMPTY_TEXT_PERCENT: EMPTY_TEXT_PERCENT_CHECK,
    DQOpsCheckType.WHITESPACE_TEXT_PERCENT: WHITESPACE_TEXT_PERCENT_CHECK,
    DQOpsCheckType.NULL_PLACEHOLDER_TEXT_FOUND: NULL_PLACEHOLDER_TEXT_FOUND_CHECK,
    DQOpsCheckType.NULL_PLACEHOLDER_TEXT_PERCENT: NULL_PLACEHOLDER_TEXT_PERCENT_CHECK,
    DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_FOUND: TEXT_SURROUNDED_BY_WHITESPACE_FOUND_CHECK,
    DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_PERCENT: TEXT_SURROUNDED_BY_WHITESPACE_PERCENT_CHECK,
    DQOpsCheckType.TEXTS_NOT_MATCHING_REGEX_PERCENT: TEXTS_NOT_MATCHING_REGEX_PERCENT_CHECK,
    DQOpsCheckType.TEXT_MATCHING_REGEX_PERCENT: TEXT_MATCHING_REGEX_PERCENT_CHECK,
    DQOpsCheckType.MIN_WORD_COUNT: MIN_WORD_COUNT_CHECK,
    DQOpsCheckType.MAX_WORD_COUNT: MAX_WORD_COUNT_CHECK,
    # Phase 2: Geographic
    DQOpsCheckType.VALID_LATITUDE_PERCENT: VALID_LATITUDE_PERCENT_CHECK,
    DQOpsCheckType.VALID_LONGITUDE_PERCENT: VALID_LONGITUDE_PERCENT_CHECK,
    # Phase 2: Numeric
    DQOpsCheckType.NUMBER_BELOW_MIN_VALUE: NUMBER_BELOW_MIN_VALUE_CHECK,
    DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE: NUMBER_ABOVE_MAX_VALUE_CHECK,
    DQOpsCheckType.NUMBER_BELOW_MIN_VALUE_PERCENT: NUMBER_BELOW_MIN_VALUE_PERCENT_CHECK,
    DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE_PERCENT: NUMBER_ABOVE_MAX_VALUE_PERCENT_CHECK,
    DQOpsCheckType.NUMBER_IN_RANGE_PERCENT: NUMBER_IN_RANGE_PERCENT_CHECK,
    DQOpsCheckType.NEGATIVE_VALUES: NEGATIVE_VALUES_CHECK,
    DQOpsCheckType.NEGATIVE_VALUES_PERCENT: NEGATIVE_VALUES_PERCENT_CHECK,
    DQOpsCheckType.NON_NEGATIVE_VALUES: NON_NEGATIVE_VALUES_CHECK,
    DQOpsCheckType.NON_NEGATIVE_VALUES_PERCENT: NON_NEGATIVE_VALUES_PERCENT_CHECK,
    DQOpsCheckType.INTEGER_IN_RANGE_PERCENT: INTEGER_IN_RANGE_PERCENT_CHECK,
    # Phase 3: Statistical
    DQOpsCheckType.SAMPLE_STDDEV_IN_RANGE: SAMPLE_STDDEV_IN_RANGE_CHECK,
    DQOpsCheckType.POPULATION_STDDEV_IN_RANGE: POPULATION_STDDEV_IN_RANGE_CHECK,
    DQOpsCheckType.SAMPLE_VARIANCE_IN_RANGE: SAMPLE_VARIANCE_IN_RANGE_CHECK,
    DQOpsCheckType.POPULATION_VARIANCE_IN_RANGE: POPULATION_VARIANCE_IN_RANGE_CHECK,
    DQOpsCheckType.PERCENTILE_IN_RANGE: PERCENTILE_IN_RANGE_CHECK,
    DQOpsCheckType.PERCENTILE_10_IN_RANGE: PERCENTILE_10_IN_RANGE_CHECK,
    DQOpsCheckType.PERCENTILE_25_IN_RANGE: PERCENTILE_25_IN_RANGE_CHECK,
    DQOpsCheckType.PERCENTILE_75_IN_RANGE: PERCENTILE_75_IN_RANGE_CHECK,
    DQOpsCheckType.PERCENTILE_90_IN_RANGE: PERCENTILE_90_IN_RANGE_CHECK,
    # Phase 4: Accepted Values
    DQOpsCheckType.TEXT_FOUND_IN_SET_PERCENT: TEXT_FOUND_IN_SET_PERCENT_CHECK,
    DQOpsCheckType.NUMBER_FOUND_IN_SET_PERCENT: NUMBER_FOUND_IN_SET_PERCENT_CHECK,
    DQOpsCheckType.EXPECTED_TEXT_VALUES_IN_USE_COUNT: EXPECTED_TEXT_VALUES_IN_USE_COUNT_CHECK,
    DQOpsCheckType.EXPECTED_NUMBERS_IN_USE_COUNT: EXPECTED_NUMBERS_IN_USE_COUNT_CHECK,
    DQOpsCheckType.EXPECTED_TEXTS_IN_TOP_VALUES_COUNT: EXPECTED_TEXTS_IN_TOP_VALUES_COUNT_CHECK,
    DQOpsCheckType.TEXT_VALID_COUNTRY_CODE_PERCENT: TEXT_VALID_COUNTRY_CODE_PERCENT_CHECK,
    DQOpsCheckType.TEXT_VALID_CURRENCY_CODE_PERCENT: TEXT_VALID_CURRENCY_CODE_PERCENT_CHECK,
    # Phase 5: Date Pattern & Data Type
    DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_FOUND: TEXT_NOT_MATCHING_DATE_PATTERN_FOUND_CHECK,
    DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT: TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT_CHECK,
    DQOpsCheckType.TEXT_MATCH_DATE_FORMAT_PERCENT: TEXT_MATCH_DATE_FORMAT_PERCENT_CHECK,
    DQOpsCheckType.TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT: TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT_CHECK,
    DQOpsCheckType.TEXT_PARSABLE_TO_BOOLEAN_PERCENT: TEXT_PARSABLE_TO_BOOLEAN_PERCENT_CHECK,
    DQOpsCheckType.TEXT_PARSABLE_TO_INTEGER_PERCENT: TEXT_PARSABLE_TO_INTEGER_PERCENT_CHECK,
    DQOpsCheckType.TEXT_PARSABLE_TO_FLOAT_PERCENT: TEXT_PARSABLE_TO_FLOAT_PERCENT_CHECK,
    DQOpsCheckType.TEXT_PARSABLE_TO_DATE_PERCENT: TEXT_PARSABLE_TO_DATE_PERCENT_CHECK,
    DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT: DETECTED_DATATYPE_IN_TEXT_CHECK,
    DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT_CHANGED: DETECTED_DATATYPE_IN_TEXT_CHANGED_CHECK,
    # Phase 6: PII Detection
    DQOpsCheckType.CONTAINS_USA_PHONE_PERCENT: CONTAINS_USA_PHONE_PERCENT_CHECK,
    DQOpsCheckType.CONTAINS_EMAIL_PERCENT: CONTAINS_EMAIL_PERCENT_CHECK,
    DQOpsCheckType.CONTAINS_USA_ZIPCODE_PERCENT: CONTAINS_USA_ZIPCODE_PERCENT_CHECK,
    DQOpsCheckType.CONTAINS_IP4_PERCENT: CONTAINS_IP4_PERCENT_CHECK,
    DQOpsCheckType.CONTAINS_IP6_PERCENT: CONTAINS_IP6_PERCENT_CHECK,
    # Phase 7: Change Detection
    DQOpsCheckType.NULLS_PERCENT_CHANGE_1_DAY: NULLS_PERCENT_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.NULLS_PERCENT_CHANGE_7_DAYS: NULLS_PERCENT_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.NULLS_PERCENT_CHANGE_30_DAYS: NULLS_PERCENT_CHANGE_30_DAYS_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_CHANGE_1_DAY: DISTINCT_COUNT_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_CHANGE_7_DAYS: DISTINCT_COUNT_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_CHANGE_30_DAYS: DISTINCT_COUNT_CHANGE_30_DAYS_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT_CHANGE_1_DAY: DISTINCT_PERCENT_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT_CHANGE_7_DAYS: DISTINCT_PERCENT_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT_CHANGE_30_DAYS: DISTINCT_PERCENT_CHANGE_30_DAYS_CHECK,
    DQOpsCheckType.MEAN_CHANGE_1_DAY: MEAN_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.MEAN_CHANGE_7_DAYS: MEAN_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.MEAN_CHANGE_30_DAYS: MEAN_CHANGE_30_DAYS_CHECK,
    DQOpsCheckType.MEDIAN_CHANGE_1_DAY: MEDIAN_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.MEDIAN_CHANGE_7_DAYS: MEDIAN_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.MEDIAN_CHANGE_30_DAYS: MEDIAN_CHANGE_30_DAYS_CHECK,
    DQOpsCheckType.SUM_CHANGE_1_DAY: SUM_CHANGE_1_DAY_CHECK,
    DQOpsCheckType.SUM_CHANGE_7_DAYS: SUM_CHANGE_7_DAYS_CHECK,
    DQOpsCheckType.SUM_CHANGE_30_DAYS: SUM_CHANGE_30_DAYS_CHECK,
    # Phase 8: Cross-Table
    DQOpsCheckType.TOTAL_ROW_COUNT_MATCH_PERCENT: TOTAL_ROW_COUNT_MATCH_PERCENT_CHECK,
    DQOpsCheckType.TOTAL_SUM_MATCH_PERCENT: TOTAL_SUM_MATCH_PERCENT_CHECK,
    DQOpsCheckType.TOTAL_MIN_MATCH_PERCENT: TOTAL_MIN_MATCH_PERCENT_CHECK,
    DQOpsCheckType.TOTAL_MAX_MATCH_PERCENT: TOTAL_MAX_MATCH_PERCENT_CHECK,
    DQOpsCheckType.TOTAL_AVERAGE_MATCH_PERCENT: TOTAL_AVERAGE_MATCH_PERCENT_CHECK,
    DQOpsCheckType.TOTAL_NOT_NULL_COUNT_MATCH_PERCENT: TOTAL_NOT_NULL_COUNT_MATCH_PERCENT_CHECK,
    # Phase 9: Table-Level Misc
    DQOpsCheckType.TABLE_AVAILABILITY: TABLE_AVAILABILITY_CHECK,
    DQOpsCheckType.DATA_INGESTION_DELAY: DATA_INGESTION_DELAY_CHECK,
    DQOpsCheckType.RELOAD_LAG: RELOAD_LAG_CHECK,
    DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_TABLE: SQL_CONDITION_PASSED_PERCENT_ON_TABLE_CHECK,
    DQOpsCheckType.COLUMN_TYPE_CHANGED: COLUMN_TYPE_CHANGED_CHECK,
    # Custom SQL
    DQOpsCheckType.SQL_CONDITION_FAILED_ON_TABLE: SQL_CONDITION_FAILED_ON_TABLE_CHECK,
    DQOpsCheckType.SQL_AGGREGATE_EXPRESSION_ON_TABLE: SQL_AGGREGATE_EXPRESSION_ON_TABLE_CHECK,
    # Phase 10: Text Length Percent
    DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT: TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT_CHECK,
    DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT: TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT_CHECK,
    # Phase 10: Column-level Custom SQL
    DQOpsCheckType.SQL_CONDITION_FAILED_ON_COLUMN: SQL_CONDITION_FAILED_ON_COLUMN_CHECK,
    DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_COLUMN: SQL_CONDITION_PASSED_PERCENT_ON_COLUMN_CHECK,
    DQOpsCheckType.SQL_AGGREGATE_EXPRESSION_ON_COLUMN: SQL_AGGREGATE_EXPRESSION_ON_COLUMN_CHECK,
    DQOpsCheckType.SQL_INVALID_VALUE_COUNT_ON_COLUMN: SQL_INVALID_VALUE_COUNT_ON_COLUMN_CHECK,
    DQOpsCheckType.IMPORT_CUSTOM_RESULT_ON_COLUMN: IMPORT_CUSTOM_RESULT_ON_COLUMN_CHECK,
    # Phase 10: Table-level Custom SQL
    DQOpsCheckType.SQL_INVALID_RECORD_COUNT_ON_TABLE: SQL_INVALID_RECORD_COUNT_ON_TABLE_CHECK,
    # Phase 10: Schema Detection
    DQOpsCheckType.COLUMN_LIST_CHANGED: COLUMN_LIST_CHANGED_CHECK,
    DQOpsCheckType.COLUMN_LIST_OR_ORDER_CHANGED: COLUMN_LIST_OR_ORDER_CHANGED_CHECK,
    DQOpsCheckType.COLUMN_TYPES_CHANGED: COLUMN_TYPES_CHANGED_CHECK,
    # Phase 11: Import External Results
    DQOpsCheckType.IMPORT_CUSTOM_RESULT_ON_TABLE: IMPORT_CUSTOM_RESULT_ON_TABLE_CHECK,
    # Phase 11: Generic Change Detection
    DQOpsCheckType.ROW_COUNT_CHANGE: ROW_COUNT_CHANGE_CHECK,
    DQOpsCheckType.NULLS_PERCENT_CHANGE: NULLS_PERCENT_CHANGE_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_CHANGE: DISTINCT_COUNT_CHANGE_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT_CHANGE: DISTINCT_PERCENT_CHANGE_CHECK,
    DQOpsCheckType.MEAN_CHANGE: MEAN_CHANGE_CHECK,
    DQOpsCheckType.MEDIAN_CHANGE: MEDIAN_CHANGE_CHECK,
    DQOpsCheckType.SUM_CHANGE: SUM_CHANGE_CHECK,
    # Phase 12: Anomaly Detection
    DQOpsCheckType.ROW_COUNT_ANOMALY: ROW_COUNT_ANOMALY_CHECK,
    DQOpsCheckType.DATA_FRESHNESS_ANOMALY: DATA_FRESHNESS_ANOMALY_CHECK,
    DQOpsCheckType.NULLS_PERCENT_ANOMALY: NULLS_PERCENT_ANOMALY_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_ANOMALY: DISTINCT_COUNT_ANOMALY_CHECK,
    DQOpsCheckType.DISTINCT_PERCENT_ANOMALY: DISTINCT_PERCENT_ANOMALY_CHECK,
    DQOpsCheckType.SUM_ANOMALY: SUM_ANOMALY_CHECK,
    DQOpsCheckType.MEAN_ANOMALY: MEAN_ANOMALY_CHECK,
    DQOpsCheckType.MEDIAN_ANOMALY: MEDIAN_ANOMALY_CHECK,
    DQOpsCheckType.MIN_ANOMALY: MIN_ANOMALY_CHECK,
    DQOpsCheckType.MAX_ANOMALY: MAX_ANOMALY_CHECK,
    # Phase 12: Cross-Source Comparison
    DQOpsCheckType.ROW_COUNT_MATCH: ROW_COUNT_MATCH_CHECK,
    DQOpsCheckType.COLUMN_COUNT_MATCH: COLUMN_COUNT_MATCH_CHECK,
    DQOpsCheckType.SUM_MATCH: SUM_MATCH_CHECK,
    DQOpsCheckType.MIN_MATCH: MIN_MATCH_CHECK,
    DQOpsCheckType.MAX_MATCH: MAX_MATCH_CHECK,
    DQOpsCheckType.MEAN_MATCH: MEAN_MATCH_CHECK,
    DQOpsCheckType.NOT_NULL_COUNT_MATCH: NOT_NULL_COUNT_MATCH_CHECK,
    DQOpsCheckType.NULL_COUNT_MATCH: NULL_COUNT_MATCH_CHECK,
    DQOpsCheckType.DISTINCT_COUNT_MATCH: DISTINCT_COUNT_MATCH_CHECK,
}


def get_check(check_type: DQOpsCheckType) -> DQOpsCheck:
    """Get a check definition by type.

    Args:
        check_type: The type of check to retrieve.

    Returns:
        The check definition.

    Raises:
        ValueError: If check type is not registered.
    """
    if check_type not in CHECK_REGISTRY:
        raise ValueError(f"Unknown check type: {check_type}")
    return CHECK_REGISTRY[check_type]


def list_checks() -> list[DQOpsCheck]:
    """List all registered checks.

    Returns:
        List of all checks.
    """
    return list(CHECK_REGISTRY.values())


def get_column_level_checks() -> list[DQOpsCheck]:
    """Get all column-level checks.

    Returns:
        List of column-level checks.
    """
    return [c for c in CHECK_REGISTRY.values() if c.is_column_level]


def get_table_level_checks() -> list[DQOpsCheck]:
    """Get all table-level checks.

    Returns:
        List of table-level checks.
    """
    return [c for c in CHECK_REGISTRY.values() if not c.is_column_level]


def get_checks_by_category(category: str) -> list[DQOpsCheck]:
    """Get all checks in a category.

    Args:
        category: The category to filter by (e.g., "volume", "nulls").

    Returns:
        List of checks in the category.
    """
    return [c for c in CHECK_REGISTRY.values() if c.category == category]
