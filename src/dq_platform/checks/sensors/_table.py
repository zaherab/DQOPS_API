"""Table-level sensor definitions (is_column_level=False)."""

from dq_platform.checks.sensors._base import Sensor, SensorType

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
    required_params=["check_id"],
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
    required_params=["check_id"],
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
    required_params=["check_id"],
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
    required_params=["timestamp_column"],
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
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
# Phase 10: Table-Level SQL Sensors
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
    default_params={"sql_condition": "1=1"},
    required_params=["sql_condition"],
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
    required_params=["sql_expression"],
)

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
# Phase 10: Schema Detection Sensors (Table-level)
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
# Phase 11: Import External Results (Table-level)
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
