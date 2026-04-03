"""Advanced column-level sensor definitions."""

from dq_platform.checks.sensors._base import Sensor, SensorType

# =============================================================================
# Phase 7: Change Detection Sensors (Column-level) - All 18 sensors
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
    required_params=["reference_table"],
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
    required_params=["reference_table"],
)

# =============================================================================
# Phase 10: Column-Level Custom SQL Sensors
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
    required_params=["sql_condition"],
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

# =============================================================================
# Phase 11: Generic Change Detection Sensors (Column-level)
# =============================================================================

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
# Phase 11: Import Custom Result on Column
# =============================================================================

IMPORT_CUSTOM_RESULT_ON_COLUMN_SENSOR = Sensor(
    name=SensorType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    description="Import external DQ result for a column (returns provided value)",
    is_column_level=True,
    template="""
SELECT {{ imported_value }}::FLOAT as sensor_value
""",
    default_params={"imported_value": 0.0},
)
