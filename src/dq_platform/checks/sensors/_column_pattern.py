"""Pattern/detection column-level sensor definitions."""

import json
from pathlib import Path

from dq_platform.checks.sensors._base import Sensor, SensorType

# Bundled ISO codelists — reused by the country/currency code sensors so
# they validate against the *real* standard (membership), not just a
# `^[A-Z]{2}$` shape regex which would pass invalid codes like "ZZ".
_CODELIST_DIR = Path(__file__).resolve().parents[2] / "profilers" / "data"


def _load_codes(filename: str) -> list[str]:
    with (_CODELIST_DIR / filename).open() as f:
        codes: list[str] = json.load(f)["codes"]
    return codes


_ISO_3166 = _load_codes("iso3166_alpha2.json")
_ISO_4217 = _load_codes("iso4217.json")

# Reusable IN-membership CASE fragment — portable (no regex, no casts),
# runs on every dialect. `expected_values` is escaped + kept as a list by
# Sensor.render (see the expected_values special-case there).
_IN_SET_CASE = (
    "{% if expected_values %}UPPER({{ column_name }}) IN "
    "({% for v in expected_values %}'{{ v }}'"
    "{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}"
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
# Phase 4: Accepted Values & Domain Sensors (Column-level)
# =============================================================================

# In-set sensors use a plain `IN (...)` list, not PG `= ANY(ARRAY[...])`.
# `IN` is universal SQL — it transpiles cleanly to every dialect, whereas
# `ANY(ARRAY[...])` has no MySQL/SQL Server equivalent. The empty-set guard
# `1=0` keeps the SQL valid when no expected_values are configured.
TEXT_IN_SET_PERCENT_SENSOR = Sensor(
    name=SensorType.TEXT_IN_SET_PERCENT,
    description="Percentage of text values found in expected set",
    is_column_level=True,
    template="""
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE
            WHEN {% if expected_values %}{{ column_name }} IN ({% for v in expected_values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}
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
            WHEN {% if expected_values %}{{ column_name }} IN ({% for v in expected_values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}
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
SELECT COUNT(DISTINCT {{ column_name }}) as sensor_value
FROM {{ schema_name }}.{{ table_name }}
WHERE {% if expected_values %}{{ column_name }} IN ({% for v in expected_values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}
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
WHERE {% if expected_values %}{{ column_name }} IN ({% for v in expected_values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}
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
SELECT COUNT(DISTINCT v) as sensor_value
FROM (
    SELECT {{ column_name }} AS v, COUNT(*) as cnt
    FROM {{ schema_name }}.{{ table_name }}
    {% if partition_filter %}
    WHERE {{ partition_filter }}
    {% endif %}
    GROUP BY {{ column_name }}
    ORDER BY cnt DESC
    LIMIT {{ top_n }}
) t
WHERE {% if expected_values %}v IN ({% for val in expected_values %}'{{ val }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}1=0{% endif %}
""",
    default_params={"expected_values": [], "top_n": 10},
)

VALID_COUNTRY_CODE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_COUNTRY_CODE_PERCENT,
    description="Percentage of valid ISO 3166-1 alpha-2 country codes",
    is_column_level=True,
    template="""
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE
            WHEN """
    + _IN_SET_CASE
    + """
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": _ISO_3166},
)

VALID_CURRENCY_CODE_PERCENT_SENSOR = Sensor(
    name=SensorType.VALID_CURRENCY_CODE_PERCENT,
    description="Percentage of valid ISO 4217 currency codes",
    is_column_level=True,
    template="""
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 100.0
        ELSE (SUM(CASE
            WHEN """
    + _IN_SET_CASE
    + """
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100
    END as sensor_value
FROM {{ schema_name }}.{{ table_name }}
{% if partition_filter %}
WHERE {{ partition_filter }}
{% endif %}
""",
    default_params={"expected_values": _ISO_4217},
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
