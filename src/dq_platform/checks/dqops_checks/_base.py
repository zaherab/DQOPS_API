"""Core types for DQOps-style check definitions."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from dq_platform.checks.rules import RuleType as RuleType
from dq_platform.checks.sensors import SensorType as SensorType


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
