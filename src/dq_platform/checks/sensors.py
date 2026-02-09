"""Sensor SQL templates for DQOps-style checks.

Sensors are Jinja2 SQL templates that measure data characteristics.
They return a single numeric value that is then evaluated by rules.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from jinja2 import Template


class SensorType(str, Enum):
    """Types of sensors for data quality checks."""

    # Volume sensors (table-level)
    ROW_COUNT = "row_count"
    ROW_COUNT_CHANGE_1_DAY = "row_count_change_1_day"
    ROW_COUNT_CHANGE_7_DAYS = "row_count_change_7_days"
    ROW_COUNT_CHANGE_30_DAYS = "row_count_change_30_days"

    # Schema sensors (table-level)
    COLUMN_COUNT = "column_count"
    COLUMN_EXISTS = "column_exists"

    # Timeliness sensors (table-level)
    DATA_FRESHNESS = "data_freshness"
    DATA_STALENESS = "data_staleness"

    # Null/Completeness sensors (column-level)
    NULLS_COUNT = "nulls_count"
    NULLS_PERCENT = "nulls_percent"
    NOT_NULLS_COUNT = "not_nulls_count"
    NOT_NULLS_PERCENT = "not_nulls_percent"

    # Uniqueness sensors (column-level)
    DISTINCT_COUNT = "distinct_count"
    DISTINCT_PERCENT = "distinct_percent"
    DUPLICATE_COUNT = "duplicate_count"
    DUPLICATE_PERCENT = "duplicate_percent"

    # Numeric/Statistical sensors (column-level)
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    SUM_VALUE = "sum_value"
    MEAN_VALUE = "mean_value"
    MEDIAN_VALUE = "median_value"
    STDDEV_SAMPLE = "stddev_sample"
    STDDEV_POPULATION = "stddev_population"
    VARIANCE_SAMPLE = "variance_sample"
    VARIANCE_POPULATION = "variance_population"
    PERCENTILE = "percentile"

    # Text/Pattern sensors (column-level)
    TEXT_MIN_LENGTH = "text_min_length"
    TEXT_MAX_LENGTH = "text_max_length"
    TEXT_MEAN_LENGTH = "text_mean_length"
    TEXT_LENGTH_BELOW_MIN = "text_length_below_min"
    TEXT_LENGTH_ABOVE_MAX = "text_length_above_max"
    TEXT_LENGTH_IN_RANGE_PERCENT = "text_length_in_range_percent"
    EMPTY_TEXT_COUNT = "empty_text_count"
    WHITESPACE_TEXT_COUNT = "whitespace_text_count"
    REGEX_MATCH_COUNT = "regex_match_count"
    REGEX_NOT_MATCH_COUNT = "regex_not_match_count"

    # Geographic sensors (column-level)
    INVALID_LATITUDE_COUNT = "invalid_latitude_count"
    INVALID_LONGITUDE_COUNT = "invalid_longitude_count"
    VALID_LATITUDE_PERCENT = "valid_latitude_percent"
    VALID_LONGITUDE_PERCENT = "valid_longitude_percent"

    # Numeric validation sensors (column-level)
    NUMBER_BELOW_MIN_PERCENT = "number_below_min_percent"
    NUMBER_ABOVE_MAX_PERCENT = "number_above_max_percent"
    NEGATIVE_VALUE_COUNT = "negative_value_count"
    NEGATIVE_VALUE_PERCENT = "negative_value_percent"
    NON_NEGATIVE_VALUE_COUNT = "non_negative_value_count"
    NON_NEGATIVE_VALUE_PERCENT = "non_negative_value_percent"
    INTEGER_IN_RANGE_PERCENT = "integer_in_range_percent"
    NUMBER_IN_RANGE_PERCENT = "number_in_range_percent"

    # Percentile sensors (column-level)
    PERCENTILE_10 = "percentile_10"
    PERCENTILE_25 = "percentile_25"
    PERCENTILE_75 = "percentile_75"
    PERCENTILE_90 = "percentile_90"

    # Boolean sensors (column-level)
    TRUE_COUNT = "true_count"
    TRUE_PERCENT = "true_percent"
    FALSE_COUNT = "false_count"
    FALSE_PERCENT = "false_percent"

    # DateTime sensors (column-level)
    FUTURE_DATE_COUNT = "future_date_count"
    FUTURE_DATE_PERCENT = "future_date_percent"
    DATE_IN_RANGE_COUNT = "date_in_range_count"
    DATE_IN_RANGE_PERCENT = "date_in_range_percent"

    # Whitespace/Text sensors (column-level)
    EMPTY_TEXT_PERCENT = "empty_text_percent"
    WHITESPACE_TEXT_PERCENT = "whitespace_text_percent"
    NULL_PLACEHOLDER_TEXT_COUNT = "null_placeholder_text_count"
    TEXT_SURROUNDED_WHITESPACE_COUNT = "text_surrounded_whitespace_count"
    REGEX_NOT_MATCH_PERCENT = "regex_not_match_percent"
    REGEX_MATCH_PERCENT = "regex_match_percent"
    TEXT_WORD_COUNT_MIN = "text_word_count_min"
    TEXT_WORD_COUNT_MAX = "text_word_count_max"

    # Pattern/Format sensors (column-level)
    INVALID_EMAIL_FORMAT_COUNT = "invalid_email_format_count"
    INVALID_UUID_FORMAT_COUNT = "invalid_uuid_format_count"
    INVALID_IP4_FORMAT_COUNT = "invalid_ip4_format_count"
    INVALID_IP6_FORMAT_COUNT = "invalid_ip6_format_count"
    INVALID_PHONE_FORMAT_COUNT = "invalid_phone_format_count"
    INVALID_ZIPCODE_FORMAT_COUNT = "invalid_zipcode_format_count"

    # PII detection sensors (column-level)
    CONTAINS_PHONE_PERCENT = "contains_phone_percent"
    CONTAINS_EMAIL_PERCENT = "contains_email_percent"
    CONTAINS_ZIPCODE_PERCENT = "contains_zipcode_percent"
    CONTAINS_IP4_PERCENT = "contains_ip4_percent"
    CONTAINS_IP6_PERCENT = "contains_ip6_percent"

    # Accepted values sensors (column-level)
    TEXT_IN_SET_PERCENT = "text_in_set_percent"
    NUMBER_IN_SET_PERCENT = "number_in_set_percent"
    EXPECTED_TEXT_IN_USE_COUNT = "expected_text_in_use_count"
    EXPECTED_NUMBER_IN_USE_COUNT = "expected_number_in_use_count"
    EXPECTED_TEXTS_TOP_N_COUNT = "expected_texts_top_n_count"
    VALID_COUNTRY_CODE_PERCENT = "valid_country_code_percent"
    VALID_CURRENCY_CODE_PERCENT = "valid_currency_code_percent"

    # Date pattern sensors (column-level)
    TEXT_NOT_MATCHING_DATE_COUNT = "text_not_matching_date_count"
    TEXT_MATCH_DATE_PERCENT = "text_match_date_percent"
    TEXT_NOT_MATCHING_NAME_PERCENT = "text_not_matching_name_percent"
    TEXT_PARSABLE_BOOLEAN_PERCENT = "text_parsable_boolean_percent"
    TEXT_PARSABLE_INTEGER_PERCENT = "text_parsable_integer_percent"
    TEXT_PARSABLE_FLOAT_PERCENT = "text_parsable_float_percent"
    TEXT_PARSABLE_DATE_PERCENT = "text_parsable_date_percent"
    DETECTED_DATATYPE = "detected_datatype"
    DETECTED_DATATYPE_CHANGED = "detected_datatype_changed"

    # Change detection sensors (column-level)
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

    # Referential integrity sensors (column-level)
    FOREIGN_KEY_NOT_FOUND_COUNT = "foreign_key_not_found_count"
    FOREIGN_KEY_FOUND_PERCENT = "foreign_key_found_percent"

    # Cross-table comparison sensors
    ROW_COUNT_MATCH_PERCENT = "row_count_match_percent"
    SUM_MATCH_PERCENT = "sum_match_percent"
    MIN_MATCH_PERCENT = "min_match_percent"
    MAX_MATCH_PERCENT = "max_match_percent"
    AVERAGE_MATCH_PERCENT = "average_match_percent"
    NOT_NULL_COUNT_MATCH_PERCENT = "not_null_count_match_percent"

    # Table-level duplicate sensors
    DUPLICATE_RECORD_COUNT = "duplicate_record_count"
    DUPLICATE_RECORD_PERCENT = "duplicate_record_percent"

    # Table-level misc sensors
    TABLE_AVAILABILITY = "table_availability"
    DATA_INGESTION_DELAY = "data_ingestion_delay"
    RELOAD_LAG = "reload_lag"
    SQL_CONDITION_PASSED_PERCENT = "sql_condition_passed_percent"
    COLUMN_TYPE_CHANGED = "column_type_changed"

    # Custom SQL sensors
    SQL_CONDITION_FAILED_COUNT = "sql_condition_failed_count"
    SQL_AGGREGATE_VALUE = "sql_aggregate_value"

    # Text length percent sensors (column-level)
    TEXT_LENGTH_BELOW_MIN_PERCENT = "text_length_below_min_percent"
    TEXT_LENGTH_ABOVE_MAX_PERCENT = "text_length_above_max_percent"

    # Column-level custom SQL sensors
    SQL_CONDITION_FAILED_ON_COLUMN_COUNT = "sql_condition_failed_on_column_count"
    SQL_CONDITION_PASSED_ON_COLUMN_PERCENT = "sql_condition_passed_on_column_percent"
    SQL_AGGREGATE_ON_COLUMN_VALUE = "sql_aggregate_on_column_value"
    SQL_INVALID_VALUE_ON_COLUMN_COUNT = "sql_invalid_value_on_column_count"
    IMPORT_CUSTOM_RESULT_ON_COLUMN = "import_custom_result_on_column"

    # Table-level custom SQL sensors
    SQL_INVALID_RECORD_COUNT = "sql_invalid_record_count"

    # Schema detection sensors (table-level)
    COLUMN_LIST_HASH = "column_list_hash"
    COLUMN_LIST_OR_ORDER_HASH = "column_list_or_order_hash"
    COLUMN_TYPES_HASH = "column_types_hash"

    # Table-level import sensor
    IMPORT_CUSTOM_RESULT_ON_TABLE = "import_custom_result_on_table"

    # Generic change detection sensors (use baseline comparison)
    ROW_COUNT_CHANGE = "row_count_change"
    NULLS_PERCENT_CHANGE = "nulls_percent_change"
    DISTINCT_COUNT_CHANGE = "distinct_count_change"
    DISTINCT_PERCENT_CHANGE = "distinct_percent_change"
    MEAN_CHANGE = "mean_change"
    MEDIAN_CHANGE = "median_change"
    SUM_CHANGE = "sum_change"


@dataclass
class Sensor:
    """A sensor definition with SQL template."""

    name: str
    description: str
    is_column_level: bool
    template: str
    default_params: dict[str, Any] | None = None

    def render(self, params: dict[str, Any]) -> str:
        """Render the SQL template with parameters."""
        template = Template(self.template)
        return template.render(**params)


# =============================================================================
# Volume Sensors (Table-level)
# =============================================================================

ROW_COUNT_SENSOR = Sensor(
    name=SensorType.ROW_COUNT,
    description="Count of rows in the table",
    is_column_level=False,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

ROW_COUNT_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.ROW_COUNT_CHANGE_1_DAY,
    description="Percentage change in row count compared to 1 day ago",
    is_column_level=False,
    template="""
WITH current_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

ROW_COUNT_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.ROW_COUNT_CHANGE_7_DAYS,
    description="Percentage change in row count compared to 7 days ago",
    is_column_level=False,
    template="""
WITH current_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

ROW_COUNT_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.ROW_COUNT_CHANGE_30_DAYS,
    description="Percentage change in row count compared to 30 days ago",
    is_column_level=False,
    template="""
WITH current_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

# =============================================================================
# Phase 7: Change Detection Sensors (Column-level)
# =============================================================================

# Nulls percent change sensors
NULLS_PERCENT_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.NULLS_PERCENT_CHANGE_1_DAY,
    description="Percentage change in nulls percent compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

NULLS_PERCENT_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.NULLS_PERCENT_CHANGE_7_DAYS,
    description="Percentage change in nulls percent compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

NULLS_PERCENT_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.NULLS_PERCENT_CHANGE_30_DAYS,
    description="Percentage change in nulls percent compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

# Distinct count change sensors
DISTINCT_COUNT_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.DISTINCT_COUNT_CHANGE_1_DAY,
    description="Percentage change in distinct count compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_count AS (
    SELECT COUNT(DISTINCT {{ column_name }}) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

DISTINCT_COUNT_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.DISTINCT_COUNT_CHANGE_7_DAYS,
    description="Percentage change in distinct count compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_count AS (
    SELECT COUNT(DISTINCT {{ column_name }}) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

DISTINCT_COUNT_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.DISTINCT_COUNT_CHANGE_30_DAYS,
    description="Percentage change in distinct count compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_count AS (
    SELECT COUNT(DISTINCT {{ column_name }}) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_count AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.cnt - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_count c
CROSS JOIN previous_count p
""",
)

# Distinct percent change sensors
DISTINCT_PERCENT_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.DISTINCT_PERCENT_CHANGE_1_DAY,
    description="Percentage change in distinct percent compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (COUNT(DISTINCT {{ column_name }})::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

DISTINCT_PERCENT_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.DISTINCT_PERCENT_CHANGE_7_DAYS,
    description="Percentage change in distinct percent compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (COUNT(DISTINCT {{ column_name }})::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

DISTINCT_PERCENT_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.DISTINCT_PERCENT_CHANGE_30_DAYS,
    description="Percentage change in distinct percent compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE (COUNT(DISTINCT {{ column_name }})::FLOAT / COUNT(*)) * 100
        END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_pct AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.pct - p.result_value)::FLOAT / p.result_value) * 100
    END as sensor_value
FROM current_pct c
CROSS JOIN previous_pct p
""",
)

# Mean change sensors
MEAN_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.MEAN_CHANGE_1_DAY,
    description="Percentage change in mean compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_mean AS (
    SELECT AVG({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_mean AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_mean c
CROSS JOIN previous_mean p
""",
)

MEAN_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.MEAN_CHANGE_7_DAYS,
    description="Percentage change in mean compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_mean AS (
    SELECT AVG({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_mean AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_mean c
CROSS JOIN previous_mean p
""",
)

MEAN_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.MEAN_CHANGE_30_DAYS,
    description="Percentage change in mean compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_mean AS (
    SELECT AVG({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_mean AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_mean c
CROSS JOIN previous_mean p
""",
)

# Median change sensors
MEDIAN_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.MEDIAN_CHANGE_1_DAY,
    description="Percentage change in median compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_median AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_median c
CROSS JOIN previous_median p
""",
)

MEDIAN_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.MEDIAN_CHANGE_7_DAYS,
    description="Percentage change in median compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_median AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_median c
CROSS JOIN previous_median p
""",
)

MEDIAN_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.MEDIAN_CHANGE_30_DAYS,
    description="Percentage change in median compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_median AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_median c
CROSS JOIN previous_median p
""",
)

# Sum change sensors
SUM_CHANGE_1_DAY_SENSOR = Sensor(
    name=SensorType.SUM_CHANGE_1_DAY,
    description="Percentage change in sum compared to 1 day ago",
    is_column_level=True,
    template="""
WITH current_sum AS (
    SELECT SUM({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_sum AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_sum c
CROSS JOIN previous_sum p
""",
)

SUM_CHANGE_7_DAYS_SENSOR = Sensor(
    name=SensorType.SUM_CHANGE_7_DAYS,
    description="Percentage change in sum compared to 7 days ago",
    is_column_level=True,
    template="""
WITH current_sum AS (
    SELECT SUM({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_sum AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '6 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_sum c
CROSS JOIN previous_sum p
""",
)

SUM_CHANGE_30_DAYS_SENSOR = Sensor(
    name=SensorType.SUM_CHANGE_30_DAYS,
    description="Percentage change in sum compared to 30 days ago",
    is_column_level=True,
    template="""
WITH current_sum AS (
    SELECT SUM({{ column_name }}) as val
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
previous_sum AS (
    SELECT result_value
    FROM check_results
    WHERE check_id = '{{ check_id }}'
      AND executed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND executed_at < CURRENT_TIMESTAMP - INTERVAL '29 days 23 hours'
    ORDER BY executed_at DESC
    LIMIT 1
)
SELECT 
    CASE 
        WHEN p.result_value IS NULL OR p.result_value = 0 THEN NULL
        ELSE ((c.val - p.result_value)::FLOAT / ABS(p.result_value)) * 100
    END as sensor_value
FROM current_sum c
CROSS JOIN previous_sum p
""",
)

# =============================================================================
# Schema Sensors (Table-level)
# =============================================================================

COLUMN_COUNT_SENSOR = Sensor(
    name=SensorType.COLUMN_COUNT,
    description="Count of columns in the table",
    is_column_level=False,
    template="""
SELECT COUNT(*) as sensor_value
FROM information_schema.columns
WHERE table_schema = '{{ schema_name }}'
  AND table_name = '{{ table_name }}'
""",
)

COLUMN_EXISTS_SENSOR = Sensor(
    name=SensorType.COLUMN_EXISTS,
    description="Check if a column exists (1 = exists, 0 = not)",
    is_column_level=True,
    template="""
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_name }}'
      AND table_name = '{{ table_name }}'
      AND column_name = '{{ column_name }}'
) THEN 1 ELSE 0 END as sensor_value
""",
)

# =============================================================================
# Timeliness Sensors (Table-level)
# =============================================================================

DATA_FRESHNESS_SENSOR = Sensor(
    name=SensorType.DATA_FRESHNESS,
    description="Seconds since the most recent row (based on timestamp column)",
    is_column_level=True,
    template="""
SELECT 
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ column_name }})))::BIGINT as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DATA_STALENESS_SENSOR = Sensor(
    name=SensorType.DATA_STALENESS,
    description="Seconds since the last data load (requires load tracking)",
    is_column_level=False,
    template="""
-- This is a placeholder - actual implementation depends on load tracking
SELECT 
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ timestamp_column }})))::BIGINT as sensor_value
FROM {{ schema_name }}.{{ table_name }}
""",
)

# =============================================================================
# Null/Completeness Sensors (Column-level)
# =============================================================================

NULLS_COUNT_SENSOR = Sensor(
    name=SensorType.NULLS_COUNT,
    description="Count of NULL values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

NULLS_PERCENT_SENSOR = Sensor(
    name=SensorType.NULLS_PERCENT,
    description="Percentage of NULL values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

NOT_NULLS_COUNT_SENSOR = Sensor(
    name=SensorType.NOT_NULLS_COUNT,
    description="Count of non-NULL values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

NOT_NULLS_PERCENT_SENSOR = Sensor(
    name=SensorType.NOT_NULLS_PERCENT,
    description="Percentage of non-NULL values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE WHEN {{ column_name }} IS NOT NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Uniqueness Sensors (Column-level)
# =============================================================================

DISTINCT_COUNT_SENSOR = Sensor(
    name=SensorType.DISTINCT_COUNT,
    description="Count of distinct values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(DISTINCT {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DISTINCT_PERCENT_SENSOR = Sensor(
    name=SensorType.DISTINCT_PERCENT,
    description="Percentage of distinct values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (COUNT(DISTINCT {{ column_name }})::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DUPLICATE_COUNT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_COUNT,
    description="Count of duplicate values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) - COUNT(DISTINCT {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DUPLICATE_PERCENT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_PERCENT,
    description="Percentage of duplicate values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE ((COUNT(*) - COUNT(DISTINCT {{ column_name }}))::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Numeric/Statistical Sensors (Column-level)
# =============================================================================

MIN_VALUE_SENSOR = Sensor(
    name=SensorType.MIN_VALUE,
    description="Minimum value in the column",
    is_column_level=True,
    template="""
SELECT MIN({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

MAX_VALUE_SENSOR = Sensor(
    name=SensorType.MAX_VALUE,
    description="Maximum value in the column",
    is_column_level=True,
    template="""
SELECT MAX({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

SUM_VALUE_SENSOR = Sensor(
    name=SensorType.SUM_VALUE,
    description="Sum of values in the column",
    is_column_level=True,
    template="""
SELECT SUM({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

MEAN_VALUE_SENSOR = Sensor(
    name=SensorType.MEAN_VALUE,
    description="Average (mean) of values in the column",
    is_column_level=True,
    template="""
SELECT AVG({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

MEDIAN_VALUE_SENSOR = Sensor(
    name=SensorType.MEDIAN_VALUE,
    description="Median value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

STDDEV_SAMPLE_SENSOR = Sensor(
    name=SensorType.STDDEV_SAMPLE,
    description="Sample standard deviation of values",
    is_column_level=True,
    template="""
SELECT STDDEV_SAMP({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

STDDEV_POPULATION_SENSOR = Sensor(
    name=SensorType.STDDEV_POPULATION,
    description="Population standard deviation of values",
    is_column_level=True,
    template="""
SELECT STDDEV_POP({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

VARIANCE_SAMPLE_SENSOR = Sensor(
    name=SensorType.VARIANCE_SAMPLE,
    description="Sample variance of values",
    is_column_level=True,
    template="""
SELECT VAR_SAMP({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

VARIANCE_POPULATION_SENSOR = Sensor(
    name=SensorType.VARIANCE_POPULATION,
    description="Population variance of values",
    is_column_level=True,
    template="""
SELECT VAR_POP({{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

PERCENTILE_SENSOR = Sensor(
    name=SensorType.PERCENTILE,
    description="Percentile value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT({{ percentile | default(0.5) }}) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"percentile": 0.5},
)

# =============================================================================
# Text/Pattern Sensors (Column-level)
# =============================================================================

TEXT_MIN_LENGTH_SENSOR = Sensor(
    name=SensorType.TEXT_MIN_LENGTH,
    description="Minimum text length in the column",
    is_column_level=True,
    template="""
SELECT MIN(LENGTH({{ column_name }}::TEXT)) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_MAX_LENGTH_SENSOR = Sensor(
    name=SensorType.TEXT_MAX_LENGTH,
    description="Maximum text length in the column",
    is_column_level=True,
    template="""
SELECT MAX(LENGTH({{ column_name }}::TEXT)) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_MEAN_LENGTH_SENSOR = Sensor(
    name=SensorType.TEXT_MEAN_LENGTH,
    description="Average text length in the column",
    is_column_level=True,
    template="""
SELECT AVG(LENGTH({{ column_name }}::TEXT)) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_LENGTH_BELOW_MIN_SENSOR = Sensor(
    name=SensorType.TEXT_LENGTH_BELOW_MIN,
    description="Count of values below minimum text length",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE LENGTH({{ column_name }}::TEXT) < {{ min_length }}
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"min_length": 1},
)

TEXT_LENGTH_ABOVE_MAX_SENSOR = Sensor(
    name=SensorType.TEXT_LENGTH_ABOVE_MAX,
    description="Count of values above maximum text length",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE LENGTH({{ column_name }}::TEXT) > {{ max_length }}
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"max_length": 255},
)

TEXT_LENGTH_IN_RANGE_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_LENGTH_IN_RANGE_PERCENT,
    description="Percentage of text values with length within specified range",
    is_column_level=True,
    template="""
SELECT CASE
    WHEN COUNT(*) = 0 THEN 100.0
    ELSE 100.0 * COUNT(CASE
        WHEN LENGTH({{ column_name }}::TEXT) >= {{ min_length }}
         AND LENGTH({{ column_name }}::TEXT) <= {{ max_length }}
        THEN 1
    END) / COUNT(*)
END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"min_length": 1, "max_length": 255},
)

EMPTY_TEXT_COUNT_SENSOR = Sensor(
    name=SensorType.EMPTY_TEXT_COUNT,
    description="Count of empty strings in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT = ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

WHITESPACE_TEXT_COUNT_SENSOR = Sensor(
    name=SensorType.WHITESPACE_TEXT_COUNT,
    description="Count of whitespace-only values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT ~ '^\\s+$'
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

REGEX_MATCH_COUNT_SENSOR = Sensor(
    name=SensorType.REGEX_MATCH_COUNT,
    description="Count of values matching the regex pattern",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT ~ '{{ regex_pattern }}'
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"regex_pattern": ".*"},
)

REGEX_NOT_MATCH_COUNT_SENSOR = Sensor(
    name=SensorType.REGEX_NOT_MATCH_COUNT,
    description="Count of values NOT matching the regex pattern",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '{{ regex_pattern }}'
  OR {{ column_name }} IS NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"regex_pattern": ".*"},
)

# =============================================================================
# Phase 1: Whitespace & Text Sensors (Column-level)
# =============================================================================

EMPTY_TEXT_PERCENT_SENSOR = Sensor(
    name=SensorType.EMPTY_TEXT_PERCENT,
    description="Percentage of empty strings in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }}::TEXT = '' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

WHITESPACE_TEXT_PERCENT_SENSOR = Sensor(
    name=SensorType.WHITESPACE_TEXT_PERCENT,
    description="Percentage of whitespace-only values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^\\s+$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

NULL_PLACEHOLDER_TEXT_COUNT_SENSOR = Sensor(
    name=SensorType.NULL_PLACEHOLDER_TEXT_COUNT,
    description="Count of null placeholder values (e.g., 'NULL', 'N/A', 'none') in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE UPPER({{ column_name }}::TEXT) IN ('NULL', 'N/A', 'NA', 'NONE', 'NIL', 'EMPTY', 'MISSING')
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"placeholders": ["NULL", "N/A", "NA", "NONE"]},
)

TEXT_SURROUNDED_WHITESPACE_COUNT_SENSOR = Sensor(
    name=SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT,
    description="Count of values surrounded by whitespace",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT ~ '^\\s+.*\\s+$'
   OR {{ column_name }}::TEXT ~ '^\\s+[^\\s]+'
   OR {{ column_name }}::TEXT ~ '[^\\s]+\\s+$'
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

REGEX_NOT_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.REGEX_NOT_MATCH_PERCENT,
    description="Percentage of values NOT matching the regex pattern",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT !~ '{{ regex_pattern }}' OR {{ column_name }} IS NULL 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"regex_pattern": ".*"},
)

REGEX_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.REGEX_MATCH_PERCENT,
    description="Percentage of values matching the regex pattern",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '{{ regex_pattern }}' AND {{ column_name }} IS NOT NULL 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"regex_pattern": ".*"},
)

TEXT_WORD_COUNT_MIN_SENSOR = Sensor(
    name=SensorType.TEXT_WORD_COUNT_MIN,
    description="Minimum word count in the column",
    is_column_level=True,
    template="""
SELECT MIN(array_length(regexp_split_to_array({{ column_name }}::TEXT, '\\s+'), 1)) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

TEXT_WORD_COUNT_MAX_SENSOR = Sensor(
    name=SensorType.TEXT_WORD_COUNT_MAX,
    description="Maximum word count in the column",
    is_column_level=True,
    template="""
SELECT MAX(array_length(regexp_split_to_array({{ column_name }}::TEXT, '\\s+'), 1)) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Geographic Sensors (Column-level)
# =============================================================================

INVALID_LATITUDE_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_LATITUDE_COUNT,
    description="Count of invalid latitude values (outside -90 to 90)",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} < -90 OR {{ column_name }} > 90
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_LONGITUDE_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_LONGITUDE_COUNT,
    description="Count of invalid longitude values (outside -180 to 180)",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} < -180 OR {{ column_name }} > 180
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Phase 2: Geographic Percent Sensors (Column-level)
# =============================================================================

VALID_LATITUDE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_LATITUDE_PERCENT,
    description="Percentage of valid latitude values (within -90 to 90)",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} >= -90 AND {{ column_name }} <= 90 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

VALID_LONGITUDE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_LONGITUDE_PERCENT,
    description="Percentage of valid longitude values (within -180 to 180)",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} >= -180 AND {{ column_name }} <= 180 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Phase 2: Numeric Percent Sensors (Column-level)
# =============================================================================

NUMBER_BELOW_MIN_PERCENT_SENSOR = Sensor(
    name=SensorType.NUMBER_BELOW_MIN_PERCENT,
    description="Percentage of values below minimum threshold",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} < {{ min_value }} THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"min_value": 0},
)

NUMBER_ABOVE_MAX_PERCENT_SENSOR = Sensor(
    name=SensorType.NUMBER_ABOVE_MAX_PERCENT,
    description="Percentage of values above maximum threshold",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} > {{ max_value }} THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"max_value": 100},
)

NEGATIVE_VALUE_COUNT_SENSOR = Sensor(
    name=SensorType.NEGATIVE_VALUE_COUNT,
    description="Count of negative values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} < 0
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

NEGATIVE_VALUE_PERCENT_SENSOR = Sensor(
    name=SensorType.NEGATIVE_VALUE_PERCENT,
    description="Percentage of negative values",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} < 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

NON_NEGATIVE_VALUE_COUNT_SENSOR = Sensor(
    name=SensorType.NON_NEGATIVE_VALUE_COUNT,
    description="Count of non-negative values (>= 0)",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} >= 0
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

NON_NEGATIVE_VALUE_PERCENT_SENSOR = Sensor(
    name=SensorType.NON_NEGATIVE_VALUE_PERCENT,
    description="Percentage of non-negative values (>= 0)",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE WHEN {{ column_name }} >= 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

INTEGER_IN_RANGE_PERCENT_SENSOR = Sensor(
    name=SensorType.INTEGER_IN_RANGE_PERCENT,
    description="Percentage of integer values within range",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} >= {{ min_value }} AND {{ column_name }} <= {{ max_value }} 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"min_value": 0, "max_value": 100},
)

NUMBER_IN_RANGE_PERCENT_SENSOR = Sensor(
    name=SensorType.NUMBER_IN_RANGE_PERCENT,
    description="Percentage of numeric values within range",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} >= {{ min_value }} AND {{ column_name }} <= {{ max_value }} 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"min_value": 0, "max_value": 100},
)

# =============================================================================
# Phase 3: Percentile Sensors (Column-level)
# =============================================================================

PERCENTILE_10_SENSOR = Sensor(
    name=SensorType.PERCENTILE_10,
    description="10th percentile value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

PERCENTILE_25_SENSOR = Sensor(
    name=SensorType.PERCENTILE_25,
    description="25th percentile (Q1) value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

PERCENTILE_75_SENSOR = Sensor(
    name=SensorType.PERCENTILE_75,
    description="75th percentile (Q3) value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

PERCENTILE_90_SENSOR = Sensor(
    name=SensorType.PERCENTILE_90,
    description="90th percentile value in the column",
    is_column_level=True,
    template="""
SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Boolean Sensors (Column-level)
# =============================================================================

TRUE_COUNT_SENSOR = Sensor(
    name=SensorType.TRUE_COUNT,
    description="Count of TRUE values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} = TRUE
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

TRUE_PERCENT_SENSOR = Sensor(
    name=SensorType.TRUE_PERCENT,
    description="Percentage of TRUE values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} = TRUE THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

FALSE_COUNT_SENSOR = Sensor(
    name=SensorType.FALSE_COUNT,
    description="Count of FALSE values in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} = FALSE
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

FALSE_PERCENT_SENSOR = Sensor(
    name=SensorType.FALSE_PERCENT,
    description="Percentage of FALSE values in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} = FALSE THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# DateTime Sensors (Column-level)
# =============================================================================

FUTURE_DATE_COUNT_SENSOR = Sensor(
    name=SensorType.FUTURE_DATE_COUNT,
    description="Count of future dates in the column",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} > CURRENT_TIMESTAMP
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

FUTURE_DATE_PERCENT_SENSOR = Sensor(
    name=SensorType.FUTURE_DATE_PERCENT,
    description="Percentage of future dates in the column",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE WHEN {{ column_name }} > CURRENT_TIMESTAMP THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DATE_IN_RANGE_COUNT_SENSOR = Sensor(
    name=SensorType.DATE_IN_RANGE_COUNT,
    description="Count of dates within specified range",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} >= '{{ min_date }}'::DATE 
  AND {{ column_name }} <= '{{ max_date }}'::DATE
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"min_date": "1900-01-01", "max_date": "2099-12-31"},
)

DATE_IN_RANGE_PERCENT_SENSOR = Sensor(
    name=SensorType.DATE_IN_RANGE_PERCENT,
    description="Percentage of dates within specified range",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} >= '{{ min_date }}'::DATE 
             AND {{ column_name }} <= '{{ max_date }}'::DATE 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"min_date": "1900-01-01", "max_date": "2099-12-31"},
)

# =============================================================================
# Pattern/Format Sensors (Column-level)
# =============================================================================

INVALID_EMAIL_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_EMAIL_FORMAT_COUNT,
    description="Count of invalid email format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_UUID_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_UUID_FORMAT_COUNT,
    description="Count of invalid UUID format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_IP4_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_IP4_FORMAT_COUNT,
    description="Count of invalid IPv4 format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_IP6_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_IP6_FORMAT_COUNT,
    description="Count of invalid IPv6 format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_PHONE_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_PHONE_FORMAT_COUNT,
    description="Count of invalid USA phone format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^\\+?1?[-.]?\\(?[0-9]{3}\\)?[-.]?[0-9]{3}[-.]?[0-9]{4}$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

INVALID_ZIPCODE_FORMAT_COUNT_SENSOR = Sensor(
    name=SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
    description="Count of invalid USA zipcode format values",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '^[0-9]{5}(-[0-9]{4})?$'
  AND {{ column_name }} IS NOT NULL
  AND {{ column_name }}::TEXT != ''
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Phase 6: PII Detection Sensors (Column-level)
# =============================================================================

CONTAINS_PHONE_PERCENT_SENSOR = Sensor(
    name=SensorType.CONTAINS_PHONE_PERCENT,
    description="Percentage of values containing USA phone numbers",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '\\+?1?[-.]?\\(?[0-9]{3}\\)?[-.]?[0-9]{3}[-.]?[0-9]{4}' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

CONTAINS_EMAIL_PERCENT_SENSOR = Sensor(
    name=SensorType.CONTAINS_EMAIL_PERCENT,
    description="Percentage of values containing email addresses",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

CONTAINS_ZIPCODE_PERCENT_SENSOR = Sensor(
    name=SensorType.CONTAINS_ZIPCODE_PERCENT,
    description="Percentage of values containing USA zipcodes",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '[0-9]{5}(-[0-9]{4})?' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

CONTAINS_IP4_PERCENT_SENSOR = Sensor(
    name=SensorType.CONTAINS_IP4_PERCENT,
    description="Percentage of values containing IPv4 addresses",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

CONTAINS_IP6_PERCENT_SENSOR = Sensor(
    name=SensorType.CONTAINS_IP6_PERCENT,
    description="Percentage of values containing IPv6 addresses",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Phase 4: Accepted Values & Domain Sensors (Column-level)
# =============================================================================

TEXT_IN_SET_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_IN_SET_PERCENT,
    description="Percentage of text values found in expected set",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT = ANY(ARRAY{{ expected_values }})
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": []},
)

NUMBER_IN_SET_PERCENT_SENSOR = Sensor(
    name=SensorType.NUMBER_IN_SET_PERCENT,
    description="Percentage of numeric values found in expected set",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }} = ANY(ARRAY{{ expected_values }})
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": []},
)

EXPECTED_TEXT_IN_USE_COUNT_SENSOR = Sensor(
    name=SensorType.EXPECTED_TEXT_IN_USE_COUNT,
    description="Count of expected text values actually found in column",
    is_column_level=True,
    template="""
SELECT COUNT(DISTINCT {{ column_name }}::TEXT) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT = ANY(ARRAY{{ expected_values }})
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": []},
)

EXPECTED_NUMBER_IN_USE_COUNT_SENSOR = Sensor(
    name=SensorType.EXPECTED_NUMBER_IN_USE_COUNT,
    description="Count of expected numeric values actually found in column",
    is_column_level=True,
    template="""
SELECT COUNT(DISTINCT {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} = ANY(ARRAY{{ expected_values }})
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": []},
)

EXPECTED_TEXTS_TOP_N_COUNT_SENSOR = Sensor(
    name=SensorType.EXPECTED_TEXTS_TOP_N_COUNT,
    description="Count of expected values that appear in top N most common values",
    is_column_level=True,
    template="""
SELECT COUNT(DISTINCT {{ column_name }}::TEXT) as sensor_value
FROM (
    SELECT {{ column_name }}::TEXT, COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
    GROUP BY {{ column_name }}
    ORDER BY cnt DESC
    LIMIT {{ top_n }}
) t
WHERE {{ column_name }}::TEXT = ANY(ARRAY{{ expected_values }})
""",
    default_params={"expected_values": [], "top_n": 10},
)

VALID_COUNTRY_CODE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_COUNTRY_CODE_PERCENT,
    description="Percentage of valid ISO country codes",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN UPPER({{ column_name }}::TEXT) ~ '^[A-Z]{2}$' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

VALID_CURRENCY_CODE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_CURRENCY_CODE_PERCENT,
    description="Percentage of valid ISO currency codes",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN UPPER({{ column_name }}::TEXT) ~ '^[A-Z]{3}$' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Phase 5: Date Pattern & Data Type Detection Sensors (Column-level)
# =============================================================================

TEXT_NOT_MATCHING_DATE_COUNT_SENSOR = Sensor(
    name=SensorType.TEXT_NOT_MATCHING_DATE_COUNT,
    description="Count of values not matching date pattern",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }}::TEXT !~ '{{ date_pattern }}'
  AND {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_MATCH_DATE_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_MATCH_DATE_PERCENT,
    description="Percentage of values matching date pattern",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '{{ date_pattern }}' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_NOT_MATCHING_NAME_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_NOT_MATCHING_NAME_PERCENT,
    description="Percentage of values not matching name pattern",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT !~ '^[A-Za-z]+([ ''-][A-Za-z]+)*$' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_PARSABLE_BOOLEAN_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_PARSABLE_BOOLEAN_PERCENT,
    description="Percentage of values parsable to boolean",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN UPPER({{ column_name }}::TEXT) IN ('TRUE', 'FALSE', 'YES', 'NO', '1', '0', 'T', 'F')
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_PARSABLE_INTEGER_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_PARSABLE_INTEGER_PERCENT,
    description="Percentage of values parsable to integer",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+$' 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_PARSABLE_FLOAT_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_PARSABLE_FLOAT_PERCENT,
    description="Percentage of values parsable to float",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+(\\.[0-9]+)?$' 
              OR {{ column_name }}::TEXT ~ '^-?[0-9]*\\.[0-9]+$'
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

TEXT_PARSABLE_DATE_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_PARSABLE_DATE_PERCENT,
    description="Percentage of values parsable to date",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN {{ column_name }}::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              OR {{ column_name }}::TEXT ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
              OR {{ column_name }}::TEXT ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DETECTED_DATATYPE_SENSOR = Sensor(
    name=SensorType.DETECTED_DATATYPE,
    description="Detected data type in text column (1=integer, 2=float, 3=date, 4=boolean, 5=text)",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 5
        WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 1
        WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+(\\.[0-9]+)?$' OR {{ column_name }}::TEXT ~ '^-?[0-9]*\\.[0-9]+$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 2
        WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 3
        WHEN (SUM(CASE WHEN UPPER({{ column_name }}::TEXT) IN ('TRUE','FALSE','YES','NO','1','0') THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 4
        ELSE 5
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DETECTED_DATATYPE_CHANGED_SENSOR = Sensor(
    name=SensorType.DETECTED_DATATYPE_CHANGED,
    description="Flag indicating if detected datatype changed from baseline (1=changed, 0=same)",
    is_column_level=True,
    template="""
WITH current_type AS (
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 5
            WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 1
            WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^-?[0-9]+(\\.[0-9]+)?$' OR {{ column_name }}::TEXT ~ '^-?[0-9]*\\.[0-9]+$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 2
            WHEN (SUM(CASE WHEN {{ column_name }}::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 3
            WHEN (SUM(CASE WHEN UPPER({{ column_name }}::TEXT) IN ('TRUE','FALSE','YES','NO','1','0') THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 >= 95 THEN 4
            ELSE 5
        END as type_code
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
)
SELECT 
    CASE 
        WHEN {{ baseline_datatype }} IS NULL THEN 0
        WHEN type_code != {{ baseline_datatype }} THEN 1
        ELSE 0
    END as sensor_value
FROM current_type
""",
    default_params={"baseline_datatype": None},
)

# =============================================================================
# Referential Integrity Sensors (Column-level)
# =============================================================================

FOREIGN_KEY_NOT_FOUND_COUNT_SENSOR = Sensor(
    name=SensorType.FOREIGN_KEY_NOT_FOUND_COUNT,
    description="Count of values not found in reference table",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }} t
WHERE t.{{ column_name }} IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM {{ reference_schema }}.{{ reference_table }} r
    WHERE r.{{ reference_column }} = t.{{ column_name }}
  )
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

FOREIGN_KEY_FOUND_PERCENT_SENSOR = Sensor(
    name=SensorType.FOREIGN_KEY_FOUND_PERCENT,
    description="Percentage of values found in reference table",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE 
            WHEN EXISTS (
                SELECT 1 FROM {{ reference_schema }}.{{ reference_table }} r
                WHERE r.{{ reference_column }} = t.{{ column_name }}
            ) THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }} t
WHERE t.{{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Table-level Duplicate Sensors
# =============================================================================

DUPLICATE_RECORD_COUNT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_RECORD_COUNT,
    description="Count of duplicate rows (all columns match)",
    is_column_level=False,
    template="""
SELECT COUNT(*) - COUNT(DISTINCT ctid) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

DUPLICATE_RECORD_PERCENT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_RECORD_PERCENT,
    description="Percentage of duplicate rows",
    is_column_level=False,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE ((COUNT(*) - COUNT(DISTINCT ctid))::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)

# =============================================================================
# Custom SQL Sensors
# =============================================================================

SQL_CONDITION_FAILED_COUNT_SENSOR = Sensor(
    name=SensorType.SQL_CONDITION_FAILED_COUNT,
    description="Count of rows failing the custom SQL condition",
    is_column_level=False,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE NOT ({{ sql_condition }})
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

SQL_AGGREGATE_VALUE_SENSOR = Sensor(
    name=SensorType.SQL_AGGREGATE_VALUE,
    description="Result of a custom SQL aggregate expression",
    is_column_level=False,
    template="""
SELECT ({{ sql_expression }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
)


# =============================================================================
# Phase 8: Cross-Table Comparison Sensors
# =============================================================================

ROW_COUNT_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.ROW_COUNT_MATCH_PERCENT,
    description="Percentage of row count matching reference table",
    is_column_level=False,
    template="""
WITH current_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN r.cnt IS NULL OR r.cnt = 0 THEN NULL
        ELSE LEAST((c.cnt::FLOAT / r.cnt) * 100, 100.0)
    END as sensor_value
FROM current_count c
CROSS JOIN reference_count r
""",
    default_params={"reference_schema": "public"},
)

SUM_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.SUM_MATCH_PERCENT,
    description="Percentage of column sum matching reference column",
    is_column_level=True,
    template="""
WITH current_sum AS (
    SELECT SUM({{ column_name }}) as s
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_sum AS (
    SELECT SUM({{ reference_column }}) as s
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN r.s IS NULL OR r.s = 0 THEN NULL
        ELSE LEAST((ABS(c.s)::FLOAT / ABS(r.s)) * 100, 100.0)
    END as sensor_value
FROM current_sum c
CROSS JOIN reference_sum r
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

MIN_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.MIN_MATCH_PERCENT,
    description="Percentage of column minimum matching reference column",
    is_column_level=True,
    template="""
WITH current_min AS (
    SELECT MIN({{ column_name }}) as m
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_min AS (
    SELECT MIN({{ reference_column }}) as m
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN c.m IS NULL OR r.m IS NULL THEN NULL
        WHEN c.m = r.m THEN 100.0
        ELSE 0.0
    END as sensor_value
FROM current_min c
CROSS JOIN reference_min r
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

MAX_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.MAX_MATCH_PERCENT,
    description="Percentage of column maximum matching reference column",
    is_column_level=True,
    template="""
WITH current_max AS (
    SELECT MAX({{ column_name }}) as m
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_max AS (
    SELECT MAX({{ reference_column }}) as m
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN c.m IS NULL OR r.m IS NULL THEN NULL
        WHEN c.m = r.m THEN 100.0
        ELSE 0.0
    END as sensor_value
FROM current_max c
CROSS JOIN reference_max r
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

AVERAGE_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.AVERAGE_MATCH_PERCENT,
    description="Percentage of column average matching reference column",
    is_column_level=True,
    template="""
WITH current_avg AS (
    SELECT AVG({{ column_name }}) as a
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_avg AS (
    SELECT AVG({{ reference_column }}) as a
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN r.a IS NULL OR r.a = 0 THEN NULL
        WHEN ABS(c.a - r.a) / NULLIF(ABS(r.a), 0) <= {{ tolerance_percent | default(0.01) }} THEN 100.0
        ELSE LEAST((1.0 - ABS(c.a - r.a) / NULLIF(ABS(r.a), 0)) * 100, 100.0)
    END as sensor_value
FROM current_avg c
CROSS JOIN reference_avg r
""",
    default_params={"reference_schema": "public", "reference_column": "id", "tolerance_percent": 0.01},
)

NOT_NULL_COUNT_MATCH_PERCENT_SENSOR = Sensor(
    name=SensorType.NOT_NULL_COUNT_MATCH_PERCENT,
    description="Percentage of not-null count matching reference column",
    is_column_level=True,
    template="""
WITH current_count AS (
    SELECT COUNT({{ column_name }}) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
),
reference_count AS (
    SELECT COUNT({{ reference_column }}) as cnt
    FROM {{ reference_schema }}.{{ reference_table }}
)
SELECT 
    CASE 
        WHEN r.cnt IS NULL OR r.cnt = 0 THEN NULL
        ELSE LEAST((c.cnt::FLOAT / r.cnt) * 100, 100.0)
    END as sensor_value
FROM current_count c
CROSS JOIN reference_count r
""",
    default_params={"reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Phase 9: Table-Level Misc Sensors
# =============================================================================

TABLE_AVAILABILITY_SENSOR = Sensor(
    name=SensorType.TABLE_AVAILABILITY,
    description="Check if table is accessible (1 = available, 0 = not)",
    is_column_level=False,
    template="""
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = '{{ schema_name }}'
      AND table_name = '{{ table_name }}'
) THEN 1 ELSE 0 END as sensor_value
""",
)

DATA_INGESTION_DELAY_SENSOR = Sensor(
    name=SensorType.DATA_INGESTION_DELAY,
    description="Seconds since last data ingestion (based on max timestamp)",
    is_column_level=True,
    template="""
SELECT 
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ column_name }})))::BIGINT as sensor_value
FROM {{ schema_name }}.{{ table_name }}
""",
)

RELOAD_LAG_SENSOR = Sensor(
    name=SensorType.RELOAD_LAG,
    description="Seconds since table was last reloaded/updated",
    is_column_level=False,
    template="""
SELECT 
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(pg_stat_user_tables.last_vacuum)))::BIGINT as sensor_value
FROM pg_stat_user_tables
WHERE schemaname = '{{ schema_name }}'
  AND relname = '{{ table_name }}'
""",
)

SQL_CONDITION_PASSED_PERCENT_SENSOR = Sensor(
    name=SensorType.SQL_CONDITION_PASSED_PERCENT,
    description="Percentage of rows passing the custom SQL condition",
    is_column_level=False,
    template="""
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE WHEN ({{ sql_condition }}) THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"sql_condition": "1=1"},
)

COLUMN_TYPE_CHANGED_SENSOR = Sensor(
    name=SensorType.COLUMN_TYPE_CHANGED,
    description="Flag indicating if column type changed from expected (1=changed, 0=same)",
    is_column_level=True,
    template="""
SELECT 
    CASE 
        WHEN data_type = '{{ expected_type }}' THEN 0
        ELSE 1
    END as sensor_value
FROM information_schema.columns
WHERE table_schema = '{{ schema_name }}'
  AND table_name = '{{ table_name }}'
  AND column_name = '{{ column_name }}'
""",
    default_params={"expected_type": "character varying"},
)


# =============================================================================
# Phase 10: New DQOps Checks - Text Length Percent Sensors
# =============================================================================

TEXT_LENGTH_BELOW_MIN_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_LENGTH_BELOW_MIN_PERCENT,
    description="Percentage of text values below minimum length",
    is_column_level=True,
    template="""
SELECT CASE
    WHEN COUNT(*) = 0 THEN 0.0
    ELSE 100.0 * COUNT(CASE WHEN LENGTH({{ column_name }}::TEXT) < {{ min_length }} THEN 1 END) / COUNT(*)
END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"min_length": 1},
)

TEXT_LENGTH_ABOVE_MAX_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_LENGTH_ABOVE_MAX_PERCENT,
    description="Percentage of text values above maximum length",
    is_column_level=True,
    template="""
SELECT CASE
    WHEN COUNT(*) = 0 THEN 0.0
    ELSE 100.0 * COUNT(CASE WHEN LENGTH({{ column_name }}::TEXT) > {{ max_length }} THEN 1 END) / COUNT(*)
END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"max_length": 255},
)


# =============================================================================
# Phase 10: New DQOps Checks - Column-Level Custom SQL Sensors
# =============================================================================

SQL_CONDITION_FAILED_ON_COLUMN_COUNT_SENSOR = Sensor(
    name=SensorType.SQL_CONDITION_FAILED_ON_COLUMN_COUNT,
    description="Count of rows where column fails custom SQL condition",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL AND NOT ({{ sql_condition }})
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"sql_condition": "1=1"},
)

SQL_CONDITION_PASSED_ON_COLUMN_PERCENT_SENSOR = Sensor(
    name=SensorType.SQL_CONDITION_PASSED_ON_COLUMN_PERCENT,
    description="Percentage of rows where column passes custom SQL condition",
    is_column_level=True,
    template="""
SELECT CASE
    WHEN COUNT(CASE WHEN {{ column_name }} IS NOT NULL THEN 1 END) = 0 THEN 100.0
    ELSE 100.0 * COUNT(CASE WHEN {{ column_name }} IS NOT NULL AND ({{ sql_condition }}) THEN 1 END)
         / COUNT(CASE WHEN {{ column_name }} IS NOT NULL THEN 1 END)
END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
""",
    default_params={"sql_condition": "1=1"},
)

SQL_AGGREGATE_ON_COLUMN_VALUE_SENSOR = Sensor(
    name=SensorType.SQL_AGGREGATE_ON_COLUMN_VALUE,
    description="Result of a custom SQL aggregate expression on a column",
    is_column_level=True,
    template="""
SELECT ({{ sql_expression }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"sql_expression": "COUNT(*)"},
)

SQL_INVALID_VALUE_ON_COLUMN_COUNT_SENSOR = Sensor(
    name=SensorType.SQL_INVALID_VALUE_ON_COLUMN_COUNT,
    description="Count of column values matching invalid values from SQL subquery",
    is_column_level=True,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL AND {{ column_name }}::TEXT IN ({{ invalid_values }})
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"invalid_values": "''"},
)

IMPORT_CUSTOM_RESULT_ON_COLUMN_SENSOR = Sensor(
    name=SensorType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    description="Import external DQ result for a column (returns provided value)",
    is_column_level=True,
    template="""
SELECT {{ imported_value }}::FLOAT as sensor_value
""",
    default_params={"imported_value": 0.0},
)


# =============================================================================
# Phase 10: New DQOps Checks - Table-Level SQL Sensors
# =============================================================================

SQL_INVALID_RECORD_COUNT_SENSOR = Sensor(
    name=SensorType.SQL_INVALID_RECORD_COUNT,
    description="Count of records matching a custom SQL condition (invalid records)",
    is_column_level=False,
    template="""
SELECT COUNT(*) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ sql_condition }}
{% if partition_filter %}AND {{ partition_filter }}{% endif %}
""",
    default_params={"sql_condition": "1=0"},
)


# =============================================================================
# Phase 10: New DQOps Checks - Schema Detection Sensors
# =============================================================================

COLUMN_LIST_HASH_SENSOR = Sensor(
    name=SensorType.COLUMN_LIST_HASH,
    description="Hash of sorted column list (detects if columns added/removed)",
    is_column_level=False,
    template="""
WITH current_cols AS (
    SELECT MD5(STRING_AGG(column_name, ',' ORDER BY column_name)) as col_hash
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_name }}' AND table_name = '{{ table_name }}'
)
SELECT CASE
    WHEN '{{ expected_hash | default("") }}' = '' THEN 0
    WHEN c.col_hash = '{{ expected_hash }}' THEN 0
    ELSE 1
END as sensor_value
FROM current_cols c
""",
    default_params={"expected_hash": ""},
)

COLUMN_LIST_OR_ORDER_HASH_SENSOR = Sensor(
    name=SensorType.COLUMN_LIST_OR_ORDER_HASH,
    description="Hash of column list in ordinal order (detects columns or order changes)",
    is_column_level=False,
    template="""
WITH current_cols AS (
    SELECT MD5(STRING_AGG(column_name, ',' ORDER BY ordinal_position)) as col_hash
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_name }}' AND table_name = '{{ table_name }}'
)
SELECT CASE
    WHEN '{{ expected_hash | default("") }}' = '' THEN 0
    WHEN c.col_hash = '{{ expected_hash }}' THEN 0
    ELSE 1
END as sensor_value
FROM current_cols c
""",
    default_params={"expected_hash": ""},
)

COLUMN_TYPES_HASH_SENSOR = Sensor(
    name=SensorType.COLUMN_TYPES_HASH,
    description="Hash of column names and types (detects type changes)",
    is_column_level=False,
    template="""
WITH current_types AS (
    SELECT MD5(STRING_AGG(column_name || ':' || data_type, ',' ORDER BY column_name)) as type_hash
    FROM information_schema.columns
    WHERE table_schema = '{{ schema_name }}' AND table_name = '{{ table_name }}'
)
SELECT CASE
    WHEN '{{ expected_hash | default("") }}' = '' THEN 0
    WHEN c.type_hash = '{{ expected_hash }}' THEN 0
    ELSE 1
END as sensor_value
FROM current_types c
""",
    default_params={"expected_hash": ""},
)


# =============================================================================
# Phase 11: Import and Generic Change Detection Sensors
# =============================================================================

IMPORT_CUSTOM_RESULT_ON_TABLE_SENSOR = Sensor(
    name=SensorType.IMPORT_CUSTOM_RESULT_ON_TABLE,
    description="Import external DQ result for a table (returns provided value)",
    is_column_level=False,
    template="""
SELECT {{ imported_value }}::FLOAT as sensor_value
""",
    default_params={"imported_value": 0.0},
)

ROW_COUNT_CHANGE_SENSOR = Sensor(
    name=SensorType.ROW_COUNT_CHANGE,
    description="Percentage change in row count compared to baseline",
    is_column_level=False,
    template="""
WITH current_count AS (
    SELECT COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
)
SELECT CASE
    WHEN {{ baseline_count | default(0) }} = 0 THEN
        CASE WHEN c.cnt = 0 THEN 0.0 ELSE 100.0 END
    ELSE ABS(100.0 * (c.cnt - {{ baseline_count }}) / {{ baseline_count }})
END as sensor_value
FROM current_count c
""",
    default_params={"baseline_count": 0},
)

NULLS_PERCENT_CHANGE_SENSOR = Sensor(
    name=SensorType.NULLS_PERCENT_CHANGE,
    description="Absolute change in null percentage compared to baseline",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT CASE
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE 100.0 * COUNT(CASE WHEN {{ column_name }} IS NULL THEN 1 END) / COUNT(*)
    END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
)
SELECT ABS(c.pct - {{ baseline_percent | default(0) }}) as sensor_value
FROM current_pct c
""",
    default_params={"baseline_percent": 0.0},
)

DISTINCT_COUNT_CHANGE_SENSOR = Sensor(
    name=SensorType.DISTINCT_COUNT_CHANGE,
    description="Percentage change in distinct count compared to baseline",
    is_column_level=True,
    template="""
WITH current_count AS (
    SELECT COUNT(DISTINCT {{ column_name }}) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
)
SELECT CASE
    WHEN {{ baseline_count | default(0) }} = 0 THEN
        CASE WHEN c.cnt = 0 THEN 0.0 ELSE 100.0 END
    ELSE ABS(100.0 * (c.cnt - {{ baseline_count }}) / {{ baseline_count }})
END as sensor_value
FROM current_count c
""",
    default_params={"baseline_count": 0},
)

DISTINCT_PERCENT_CHANGE_SENSOR = Sensor(
    name=SensorType.DISTINCT_PERCENT_CHANGE,
    description="Absolute change in distinct percentage compared to baseline",
    is_column_level=True,
    template="""
WITH current_pct AS (
    SELECT CASE
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE 100.0 * COUNT(DISTINCT {{ column_name }}) / COUNT(*)
    END as pct
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
)
SELECT ABS(c.pct - {{ baseline_percent | default(0) }}) as sensor_value
FROM current_pct c
""",
    default_params={"baseline_percent": 0.0},
)

MEAN_CHANGE_SENSOR = Sensor(
    name=SensorType.MEAN_CHANGE,
    description="Percentage change in mean value compared to baseline",
    is_column_level=True,
    template="""
WITH current_mean AS (
    SELECT AVG({{ column_name }}::FLOAT) as val
    FROM {{ schema_name }}.{{ table_name }}
    WHERE {{ column_name }} IS NOT NULL
    {% if partition_filter %}AND {{ partition_filter }}{% endif %}
)
SELECT CASE
    WHEN {{ baseline_value | default(0) }} = 0 THEN
        CASE WHEN c.val IS NULL OR c.val = 0 THEN 0.0 ELSE 100.0 END
    WHEN c.val IS NULL THEN 100.0
    ELSE ABS(100.0 * (c.val - {{ baseline_value }}) / {{ baseline_value }})
END as sensor_value
FROM current_mean c
""",
    default_params={"baseline_value": 0.0},
)

MEDIAN_CHANGE_SENSOR = Sensor(
    name=SensorType.MEDIAN_CHANGE,
    description="Percentage change in median value compared to baseline",
    is_column_level=True,
    template="""
WITH current_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}::FLOAT) as val
    FROM {{ schema_name }}.{{ table_name }}
    WHERE {{ column_name }} IS NOT NULL
    {% if partition_filter %}AND {{ partition_filter }}{% endif %}
)
SELECT CASE
    WHEN {{ baseline_value | default(0) }} = 0 THEN
        CASE WHEN c.val IS NULL OR c.val = 0 THEN 0.0 ELSE 100.0 END
    WHEN c.val IS NULL THEN 100.0
    ELSE ABS(100.0 * (c.val - {{ baseline_value }}) / {{ baseline_value }})
END as sensor_value
FROM current_median c
""",
    default_params={"baseline_value": 0.0},
)

SUM_CHANGE_SENSOR = Sensor(
    name=SensorType.SUM_CHANGE,
    description="Percentage change in sum value compared to baseline",
    is_column_level=True,
    template="""
WITH current_sum AS (
    SELECT SUM({{ column_name }}::FLOAT) as val
    FROM {{ schema_name }}.{{ table_name }}
    WHERE {{ column_name }} IS NOT NULL
    {% if partition_filter %}AND {{ partition_filter }}{% endif %}
)
SELECT CASE
    WHEN {{ baseline_value | default(0) }} = 0 THEN
        CASE WHEN c.val IS NULL OR c.val = 0 THEN 0.0 ELSE 100.0 END
    WHEN c.val IS NULL THEN 100.0
    ELSE ABS(100.0 * (c.val - {{ baseline_value }}) / {{ baseline_value }})
END as sensor_value
FROM current_sum c
""",
    default_params={"baseline_value": 0.0},
)


# =============================================================================
# Sensor Registry
# =============================================================================

SENSOR_REGISTRY: dict[SensorType, Sensor] = {
    # Volume
    SensorType.ROW_COUNT: ROW_COUNT_SENSOR,
    SensorType.ROW_COUNT_CHANGE_1_DAY: ROW_COUNT_CHANGE_1_DAY_SENSOR,
    SensorType.ROW_COUNT_CHANGE_7_DAYS: ROW_COUNT_CHANGE_7_DAYS_SENSOR,
    SensorType.ROW_COUNT_CHANGE_30_DAYS: ROW_COUNT_CHANGE_30_DAYS_SENSOR,
    # Schema
    SensorType.COLUMN_COUNT: COLUMN_COUNT_SENSOR,
    SensorType.COLUMN_EXISTS: COLUMN_EXISTS_SENSOR,
    # Timeliness
    SensorType.DATA_FRESHNESS: DATA_FRESHNESS_SENSOR,
    SensorType.DATA_STALENESS: DATA_STALENESS_SENSOR,
    # Nulls/Completeness
    SensorType.NULLS_COUNT: NULLS_COUNT_SENSOR,
    SensorType.NULLS_PERCENT: NULLS_PERCENT_SENSOR,
    SensorType.NOT_NULLS_COUNT: NOT_NULLS_COUNT_SENSOR,
    SensorType.NOT_NULLS_PERCENT: NOT_NULLS_PERCENT_SENSOR,
    # Uniqueness
    SensorType.DISTINCT_COUNT: DISTINCT_COUNT_SENSOR,
    SensorType.DISTINCT_PERCENT: DISTINCT_PERCENT_SENSOR,
    SensorType.DUPLICATE_COUNT: DUPLICATE_COUNT_SENSOR,
    SensorType.DUPLICATE_PERCENT: DUPLICATE_PERCENT_SENSOR,
    # Numeric/Statistical
    SensorType.MIN_VALUE: MIN_VALUE_SENSOR,
    SensorType.MAX_VALUE: MAX_VALUE_SENSOR,
    SensorType.SUM_VALUE: SUM_VALUE_SENSOR,
    SensorType.MEAN_VALUE: MEAN_VALUE_SENSOR,
    SensorType.MEDIAN_VALUE: MEDIAN_VALUE_SENSOR,
    SensorType.STDDEV_SAMPLE: STDDEV_SAMPLE_SENSOR,
    SensorType.STDDEV_POPULATION: STDDEV_POPULATION_SENSOR,
    SensorType.VARIANCE_SAMPLE: VARIANCE_SAMPLE_SENSOR,
    SensorType.VARIANCE_POPULATION: VARIANCE_POPULATION_SENSOR,
    SensorType.PERCENTILE: PERCENTILE_SENSOR,
    # Text/Pattern
    SensorType.TEXT_MIN_LENGTH: TEXT_MIN_LENGTH_SENSOR,
    SensorType.TEXT_MAX_LENGTH: TEXT_MAX_LENGTH_SENSOR,
    SensorType.TEXT_MEAN_LENGTH: TEXT_MEAN_LENGTH_SENSOR,
    SensorType.TEXT_LENGTH_BELOW_MIN: TEXT_LENGTH_BELOW_MIN_SENSOR,
    SensorType.TEXT_LENGTH_ABOVE_MAX: TEXT_LENGTH_ABOVE_MAX_SENSOR,
    SensorType.TEXT_LENGTH_IN_RANGE_PERCENT: TEXT_LENGTH_IN_RANGE_PERCENT_SENSOR,
    SensorType.EMPTY_TEXT_COUNT: EMPTY_TEXT_COUNT_SENSOR,
    SensorType.WHITESPACE_TEXT_COUNT: WHITESPACE_TEXT_COUNT_SENSOR,
    SensorType.REGEX_MATCH_COUNT: REGEX_MATCH_COUNT_SENSOR,
    SensorType.REGEX_NOT_MATCH_COUNT: REGEX_NOT_MATCH_COUNT_SENSOR,
    # Geographic
    SensorType.INVALID_LATITUDE_COUNT: INVALID_LATITUDE_COUNT_SENSOR,
    SensorType.INVALID_LONGITUDE_COUNT: INVALID_LONGITUDE_COUNT_SENSOR,
    # Boolean
    SensorType.TRUE_COUNT: TRUE_COUNT_SENSOR,
    SensorType.TRUE_PERCENT: TRUE_PERCENT_SENSOR,
    SensorType.FALSE_COUNT: FALSE_COUNT_SENSOR,
    SensorType.FALSE_PERCENT: FALSE_PERCENT_SENSOR,
    # DateTime
    SensorType.FUTURE_DATE_COUNT: FUTURE_DATE_COUNT_SENSOR,
    SensorType.FUTURE_DATE_PERCENT: FUTURE_DATE_PERCENT_SENSOR,
    SensorType.DATE_IN_RANGE_COUNT: DATE_IN_RANGE_COUNT_SENSOR,
    SensorType.DATE_IN_RANGE_PERCENT: DATE_IN_RANGE_PERCENT_SENSOR,
    # Pattern/Format
    SensorType.INVALID_EMAIL_FORMAT_COUNT: INVALID_EMAIL_FORMAT_COUNT_SENSOR,
    SensorType.INVALID_UUID_FORMAT_COUNT: INVALID_UUID_FORMAT_COUNT_SENSOR,
    SensorType.INVALID_IP4_FORMAT_COUNT: INVALID_IP4_FORMAT_COUNT_SENSOR,
    SensorType.INVALID_IP6_FORMAT_COUNT: INVALID_IP6_FORMAT_COUNT_SENSOR,
    SensorType.INVALID_PHONE_FORMAT_COUNT: INVALID_PHONE_FORMAT_COUNT_SENSOR,
    SensorType.INVALID_ZIPCODE_FORMAT_COUNT: INVALID_ZIPCODE_FORMAT_COUNT_SENSOR,
    # Referential Integrity
    SensorType.FOREIGN_KEY_NOT_FOUND_COUNT: FOREIGN_KEY_NOT_FOUND_COUNT_SENSOR,
    SensorType.FOREIGN_KEY_FOUND_PERCENT: FOREIGN_KEY_FOUND_PERCENT_SENSOR,
    # Table-level Duplicates
    SensorType.DUPLICATE_RECORD_COUNT: DUPLICATE_RECORD_COUNT_SENSOR,
    SensorType.DUPLICATE_RECORD_PERCENT: DUPLICATE_RECORD_PERCENT_SENSOR,
    # Phase 1: Whitespace & Text
    SensorType.EMPTY_TEXT_PERCENT: EMPTY_TEXT_PERCENT_SENSOR,
    SensorType.WHITESPACE_TEXT_PERCENT: WHITESPACE_TEXT_PERCENT_SENSOR,
    SensorType.NULL_PLACEHOLDER_TEXT_COUNT: NULL_PLACEHOLDER_TEXT_COUNT_SENSOR,
    SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT: TEXT_SURROUNDED_WHITESPACE_COUNT_SENSOR,
    SensorType.REGEX_NOT_MATCH_PERCENT: REGEX_NOT_MATCH_PERCENT_SENSOR,
    SensorType.REGEX_MATCH_PERCENT: REGEX_MATCH_PERCENT_SENSOR,
    SensorType.TEXT_WORD_COUNT_MIN: TEXT_WORD_COUNT_MIN_SENSOR,
    SensorType.TEXT_WORD_COUNT_MAX: TEXT_WORD_COUNT_MAX_SENSOR,
    # Phase 2: Geographic
    SensorType.VALID_LATITUDE_PERCENT: VALID_LATITUDE_PERCENT_SENSOR,
    SensorType.VALID_LONGITUDE_PERCENT: VALID_LONGITUDE_PERCENT_SENSOR,
    # Phase 2: Numeric
    SensorType.NUMBER_BELOW_MIN_PERCENT: NUMBER_BELOW_MIN_PERCENT_SENSOR,
    SensorType.NUMBER_ABOVE_MAX_PERCENT: NUMBER_ABOVE_MAX_PERCENT_SENSOR,
    SensorType.NEGATIVE_VALUE_COUNT: NEGATIVE_VALUE_COUNT_SENSOR,
    SensorType.NEGATIVE_VALUE_PERCENT: NEGATIVE_VALUE_PERCENT_SENSOR,
    SensorType.NON_NEGATIVE_VALUE_COUNT: NON_NEGATIVE_VALUE_COUNT_SENSOR,
    SensorType.NON_NEGATIVE_VALUE_PERCENT: NON_NEGATIVE_VALUE_PERCENT_SENSOR,
    SensorType.INTEGER_IN_RANGE_PERCENT: INTEGER_IN_RANGE_PERCENT_SENSOR,
    SensorType.NUMBER_IN_RANGE_PERCENT: NUMBER_IN_RANGE_PERCENT_SENSOR,
    # Phase 3: Percentile
    SensorType.PERCENTILE_10: PERCENTILE_10_SENSOR,
    SensorType.PERCENTILE_25: PERCENTILE_25_SENSOR,
    SensorType.PERCENTILE_75: PERCENTILE_75_SENSOR,
    SensorType.PERCENTILE_90: PERCENTILE_90_SENSOR,
    # Phase 4: Accepted Values
    SensorType.TEXT_IN_SET_PERCENT: TEXT_IN_SET_PERCENT_SENSOR,
    SensorType.NUMBER_IN_SET_PERCENT: NUMBER_IN_SET_PERCENT_SENSOR,
    SensorType.EXPECTED_TEXT_IN_USE_COUNT: EXPECTED_TEXT_IN_USE_COUNT_SENSOR,
    SensorType.EXPECTED_NUMBER_IN_USE_COUNT: EXPECTED_NUMBER_IN_USE_COUNT_SENSOR,
    SensorType.EXPECTED_TEXTS_TOP_N_COUNT: EXPECTED_TEXTS_TOP_N_COUNT_SENSOR,
    SensorType.VALID_COUNTRY_CODE_PERCENT: VALID_COUNTRY_CODE_PERCENT_SENSOR,
    SensorType.VALID_CURRENCY_CODE_PERCENT: VALID_CURRENCY_CODE_PERCENT_SENSOR,
    # Phase 5: Date Pattern & Data Type
    SensorType.TEXT_NOT_MATCHING_DATE_COUNT: TEXT_NOT_MATCHING_DATE_COUNT_SENSOR,
    SensorType.TEXT_MATCH_DATE_PERCENT: TEXT_MATCH_DATE_PERCENT_SENSOR,
    SensorType.TEXT_NOT_MATCHING_NAME_PERCENT: TEXT_NOT_MATCHING_NAME_PERCENT_SENSOR,
    SensorType.TEXT_PARSABLE_BOOLEAN_PERCENT: TEXT_PARSABLE_BOOLEAN_PERCENT_SENSOR,
    SensorType.TEXT_PARSABLE_INTEGER_PERCENT: TEXT_PARSABLE_INTEGER_PERCENT_SENSOR,
    SensorType.TEXT_PARSABLE_FLOAT_PERCENT: TEXT_PARSABLE_FLOAT_PERCENT_SENSOR,
    SensorType.TEXT_PARSABLE_DATE_PERCENT: TEXT_PARSABLE_DATE_PERCENT_SENSOR,
    SensorType.DETECTED_DATATYPE: DETECTED_DATATYPE_SENSOR,
    SensorType.DETECTED_DATATYPE_CHANGED: DETECTED_DATATYPE_CHANGED_SENSOR,
    # Phase 6: PII Detection
    SensorType.CONTAINS_PHONE_PERCENT: CONTAINS_PHONE_PERCENT_SENSOR,
    SensorType.CONTAINS_EMAIL_PERCENT: CONTAINS_EMAIL_PERCENT_SENSOR,
    SensorType.CONTAINS_ZIPCODE_PERCENT: CONTAINS_ZIPCODE_PERCENT_SENSOR,
    SensorType.CONTAINS_IP4_PERCENT: CONTAINS_IP4_PERCENT_SENSOR,
    SensorType.CONTAINS_IP6_PERCENT: CONTAINS_IP6_PERCENT_SENSOR,
    # Phase 7: Change Detection
    SensorType.NULLS_PERCENT_CHANGE_1_DAY: NULLS_PERCENT_CHANGE_1_DAY_SENSOR,
    SensorType.NULLS_PERCENT_CHANGE_7_DAYS: NULLS_PERCENT_CHANGE_7_DAYS_SENSOR,
    SensorType.NULLS_PERCENT_CHANGE_30_DAYS: NULLS_PERCENT_CHANGE_30_DAYS_SENSOR,
    SensorType.DISTINCT_COUNT_CHANGE_1_DAY: DISTINCT_COUNT_CHANGE_1_DAY_SENSOR,
    SensorType.DISTINCT_COUNT_CHANGE_7_DAYS: DISTINCT_COUNT_CHANGE_7_DAYS_SENSOR,
    SensorType.DISTINCT_COUNT_CHANGE_30_DAYS: DISTINCT_COUNT_CHANGE_30_DAYS_SENSOR,
    SensorType.DISTINCT_PERCENT_CHANGE_1_DAY: DISTINCT_PERCENT_CHANGE_1_DAY_SENSOR,
    SensorType.DISTINCT_PERCENT_CHANGE_7_DAYS: DISTINCT_PERCENT_CHANGE_7_DAYS_SENSOR,
    SensorType.DISTINCT_PERCENT_CHANGE_30_DAYS: DISTINCT_PERCENT_CHANGE_30_DAYS_SENSOR,
    SensorType.MEAN_CHANGE_1_DAY: MEAN_CHANGE_1_DAY_SENSOR,
    SensorType.MEAN_CHANGE_7_DAYS: MEAN_CHANGE_7_DAYS_SENSOR,
    SensorType.MEAN_CHANGE_30_DAYS: MEAN_CHANGE_30_DAYS_SENSOR,
    SensorType.MEDIAN_CHANGE_1_DAY: MEDIAN_CHANGE_1_DAY_SENSOR,
    SensorType.MEDIAN_CHANGE_7_DAYS: MEDIAN_CHANGE_7_DAYS_SENSOR,
    SensorType.MEDIAN_CHANGE_30_DAYS: MEDIAN_CHANGE_30_DAYS_SENSOR,
    SensorType.SUM_CHANGE_1_DAY: SUM_CHANGE_1_DAY_SENSOR,
    SensorType.SUM_CHANGE_7_DAYS: SUM_CHANGE_7_DAYS_SENSOR,
    SensorType.SUM_CHANGE_30_DAYS: SUM_CHANGE_30_DAYS_SENSOR,
    # Phase 8: Cross-Table
    SensorType.ROW_COUNT_MATCH_PERCENT: ROW_COUNT_MATCH_PERCENT_SENSOR,
    SensorType.SUM_MATCH_PERCENT: SUM_MATCH_PERCENT_SENSOR,
    SensorType.MIN_MATCH_PERCENT: MIN_MATCH_PERCENT_SENSOR,
    SensorType.MAX_MATCH_PERCENT: MAX_MATCH_PERCENT_SENSOR,
    SensorType.AVERAGE_MATCH_PERCENT: AVERAGE_MATCH_PERCENT_SENSOR,
    SensorType.NOT_NULL_COUNT_MATCH_PERCENT: NOT_NULL_COUNT_MATCH_PERCENT_SENSOR,
    # Phase 9: Table-Level Misc
    SensorType.TABLE_AVAILABILITY: TABLE_AVAILABILITY_SENSOR,
    SensorType.DATA_INGESTION_DELAY: DATA_INGESTION_DELAY_SENSOR,
    SensorType.RELOAD_LAG: RELOAD_LAG_SENSOR,
    SensorType.SQL_CONDITION_PASSED_PERCENT: SQL_CONDITION_PASSED_PERCENT_SENSOR,
    SensorType.COLUMN_TYPE_CHANGED: COLUMN_TYPE_CHANGED_SENSOR,
    # Custom SQL
    SensorType.SQL_CONDITION_FAILED_COUNT: SQL_CONDITION_FAILED_COUNT_SENSOR,
    SensorType.SQL_AGGREGATE_VALUE: SQL_AGGREGATE_VALUE_SENSOR,
    # Phase 10: Text Length Percent
    SensorType.TEXT_LENGTH_BELOW_MIN_PERCENT: TEXT_LENGTH_BELOW_MIN_PERCENT_SENSOR,
    SensorType.TEXT_LENGTH_ABOVE_MAX_PERCENT: TEXT_LENGTH_ABOVE_MAX_PERCENT_SENSOR,
    # Phase 10: Column-Level Custom SQL
    SensorType.SQL_CONDITION_FAILED_ON_COLUMN_COUNT: SQL_CONDITION_FAILED_ON_COLUMN_COUNT_SENSOR,
    SensorType.SQL_CONDITION_PASSED_ON_COLUMN_PERCENT: SQL_CONDITION_PASSED_ON_COLUMN_PERCENT_SENSOR,
    SensorType.SQL_AGGREGATE_ON_COLUMN_VALUE: SQL_AGGREGATE_ON_COLUMN_VALUE_SENSOR,
    SensorType.SQL_INVALID_VALUE_ON_COLUMN_COUNT: SQL_INVALID_VALUE_ON_COLUMN_COUNT_SENSOR,
    SensorType.IMPORT_CUSTOM_RESULT_ON_COLUMN: IMPORT_CUSTOM_RESULT_ON_COLUMN_SENSOR,
    # Phase 10: Table-Level Custom SQL
    SensorType.SQL_INVALID_RECORD_COUNT: SQL_INVALID_RECORD_COUNT_SENSOR,
    # Phase 10: Schema Detection
    SensorType.COLUMN_LIST_HASH: COLUMN_LIST_HASH_SENSOR,
    SensorType.COLUMN_LIST_OR_ORDER_HASH: COLUMN_LIST_OR_ORDER_HASH_SENSOR,
    SensorType.COLUMN_TYPES_HASH: COLUMN_TYPES_HASH_SENSOR,
    # Phase 11: Import and Generic Change Detection
    SensorType.IMPORT_CUSTOM_RESULT_ON_TABLE: IMPORT_CUSTOM_RESULT_ON_TABLE_SENSOR,
    SensorType.ROW_COUNT_CHANGE: ROW_COUNT_CHANGE_SENSOR,
    SensorType.NULLS_PERCENT_CHANGE: NULLS_PERCENT_CHANGE_SENSOR,
    SensorType.DISTINCT_COUNT_CHANGE: DISTINCT_COUNT_CHANGE_SENSOR,
    SensorType.DISTINCT_PERCENT_CHANGE: DISTINCT_PERCENT_CHANGE_SENSOR,
    SensorType.MEAN_CHANGE: MEAN_CHANGE_SENSOR,
    SensorType.MEDIAN_CHANGE: MEDIAN_CHANGE_SENSOR,
    SensorType.SUM_CHANGE: SUM_CHANGE_SENSOR,
}


def get_sensor(sensor_type: SensorType) -> Sensor:
    """Get a sensor by type.

    Args:
        sensor_type: The type of sensor to retrieve.

    Returns:
        The sensor definition.

    Raises:
        ValueError: If sensor type is not registered.
    """
    if sensor_type not in SENSOR_REGISTRY:
        raise ValueError(f"Unknown sensor type: {sensor_type}")
    return SENSOR_REGISTRY[sensor_type]


def list_sensors() -> list[Sensor]:
    """List all registered sensors.

    Returns:
        List of all sensors.
    """
    return list(SENSOR_REGISTRY.values())


def get_column_level_sensors() -> list[Sensor]:
    """Get all column-level sensors.

    Returns:
        List of column-level sensors.
    """
    return [s for s in SENSOR_REGISTRY.values() if s.is_column_level]


def get_table_level_sensors() -> list[Sensor]:
    """Get all table-level sensors.

    Returns:
        List of table-level sensors.
    """
    return [s for s in SENSOR_REGISTRY.values() if not s.is_column_level]
