"""Core column-level sensor definitions."""

from dq_platform.checks.sensors._base import Sensor, SensorType

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
    description="Percentage of distinct values in the column (among non-null rows)",
    is_column_level=True,
    template="""
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE (COUNT(DISTINCT {{ column_name }})::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

DUPLICATE_COUNT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_COUNT,
    description="Count of duplicate values in the column (excluding nulls)",
    is_column_level=True,
    template="""
SELECT COUNT(*) - COUNT(DISTINCT {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
{% endif %}
""",
)

DUPLICATE_PERCENT_SENSOR = Sensor(
    name=SensorType.DUPLICATE_PERCENT,
    description="Percentage of duplicate values in the column (among non-null rows)",
    is_column_level=True,
    template="""
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE ((COUNT(*) - COUNT(DISTINCT {{ column_name }}))::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {{ column_name }} IS NOT NULL
{% if partition_filter %}
  AND {{ partition_filter }}
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
    # SQL Server has no native regex in standard T-SQL (pre-2025).
    unsupported_dialects=frozenset({"sqlserver"}),
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
    # SQL Server has no native regex in standard T-SQL (pre-2025).
    unsupported_dialects=frozenset({"sqlserver"}),
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
    # SQL Server has no native regex in standard T-SQL (pre-2025).
    unsupported_dialects=frozenset({"sqlserver"}),
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
    # SQL Server has no native regex in standard T-SQL (pre-2025).
    unsupported_dialects=frozenset({"sqlserver"}),
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
# Phase 10: Text Length Percent Sensors (Column-level)
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
