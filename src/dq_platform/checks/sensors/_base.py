"""Core types and helpers for sensor definitions."""

import re
from dataclasses import dataclass, field
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


def _quote_identifier(identifier: str, quote_char: str = '"') -> str:
    """Quote a SQL identifier to prevent injection.

    Args:
        identifier: The identifier to quote.
        quote_char: Quote character ('"' for most DBs, '`' for MySQL/BigQuery,
                    or '[' for SQL Server bracket quoting).

    Returns:
        Safely quoted identifier.
    """
    if quote_char == "[":
        # SQL Server bracket quoting: escape ] as ]]
        return f"[{identifier.replace(']', ']]')}]"
    # Standard quoting: escape the quote char by doubling it
    return f"{quote_char}{identifier.replace(quote_char, quote_char * 2)}{quote_char}"


def _validate_partition_filter(partition_filter: str) -> str:
    """Validate a partition filter to prevent SQL injection.

    Args:
        partition_filter: The partition filter expression.

    Returns:
        The validated partition filter.

    Raises:
        ValueError: If the partition filter contains suspicious patterns.
    """
    dangerous_patterns = ["--", "/*", "*/", ";"]
    pf_lower = partition_filter.lower()
    for pattern in dangerous_patterns:
        if pattern in pf_lower:
            raise ValueError(f"Partition filter contains disallowed pattern: {pattern!r}")

    # Block SQL keywords that have no place in a WHERE partition clause.
    # Use word boundaries to avoid false positives (e.g. "execution_date").
    _dangerous_keywords = (
        "union",
        "into",
        "exec",
        "execute",
        "drop",
        "alter",
        "create",
        "insert",
        "update",
        "delete",
        "truncate",
    )
    for kw in _dangerous_keywords:
        if re.search(rf"\b{kw}\b", pf_lower):
            raise ValueError(f"Partition filter contains disallowed keyword: {kw!r}")

    # Check for unbalanced quotes
    if pf_lower.count("'") % 2 != 0:
        raise ValueError("Partition filter contains unbalanced single quotes")
    if pf_lower.count('"') % 2 != 0:
        raise ValueError("Partition filter contains unbalanced double quotes")
    return partition_filter


def _list_to_sql_array(values: list[Any]) -> str:
    """Convert a Python list to a SQL ARRAY literal string.

    Args:
        values: Python list of values.

    Returns:
        SQL ARRAY literal, e.g. ``['a','b']`` or ``[1,2]``.
        For empty lists returns ``[]::TEXT[]`` to avoid PostgreSQL
        "cannot determine type of empty array" errors.
    """
    if not values:
        return "[]::TEXT[]"
    # Quote string values, pass numbers through
    parts = []
    for v in values:
        if isinstance(v, str):
            escaped = v.replace("'", "''")
            parts.append(f"'{escaped}'")
        else:
            parts.append(str(v))
    return f"[{', '.join(parts)}]"


# Regex to match Python inline comments at the end of SQL lines.
# Strips linter-directive comments (noqa, type:, pylint:, pragma) that are
# never valid SQL. A bare "#" inside a regex string literal is left alone.
_PYTHON_COMMENT_RE = re.compile(r"\s+#\s+(noqa\b|type:\s|pylint:\s|pragma\b).*$", re.MULTILINE)


def _strip_python_comments(sql: str) -> str:
    """Remove Python linter comments that leaked into rendered SQL."""
    return _PYTHON_COMMENT_RE.sub("", sql)


# Mapping of connection types to their identifier quote characters
QUOTE_CHARS: dict[str, str] = {
    "mysql": "`",
    "bigquery": "`",
    "databricks": "`",
    "sqlserver": "[",
}


@dataclass
class Sensor:
    """A sensor definition with SQL template.

    Dialect portability — most sensors author one Postgres `template` and
    rely on sqlglot transpilation at execution time. Two escape hatches
    cover the cases transpilation can't:

    - `dialect_templates`: per-dialect template override, keyed by the
      connection type string ("oracle", "sqlserver", ...). Used when a
      construct has no portable form (e.g. "seconds since" arithmetic).
    - `unsupported_dialects`: engines where the sensor genuinely cannot
      run (e.g. regex on SQL Server, which has no native regex). The
      executor skips these cleanly — the check is recorded not_assessed,
      never retried.
    """

    name: str
    description: str
    is_column_level: bool
    template: str
    default_params: dict[str, Any] | None = None
    required_params: list[str] = field(default_factory=list)
    dialect_templates: dict[str, str] = field(default_factory=dict)
    unsupported_dialects: frozenset[str] = frozenset()

    def template_for(self, dialect: str | None) -> str:
        """Return the SQL template for a dialect — override or base."""
        if dialect and dialect in self.dialect_templates:
            return self.dialect_templates[dialect]
        return self.template

    def supports(self, dialect: str | None) -> bool:
        """Whether this sensor can run on the given dialect."""
        return not dialect or dialect not in self.unsupported_dialects

    def render(
        self,
        params: dict[str, Any],
        quote_char: str = '"',
        dialect: str | None = None,
    ) -> str:
        """Render the SQL template with parameters.

        Identifier parameters (schema_name, table_name, column_name) are
        automatically quoted to prevent SQL injection. The partition_filter
        parameter is validated for dangerous patterns. List values are
        converted to SQL ARRAY literals.

        Args:
            params: Template parameters.
            quote_char: Quote character for identifiers (default: '"').
            dialect: Connection type — selects a `dialect_templates`
                override when one exists, otherwise the base template.

        Returns:
            Rendered SQL string.

        Raises:
            ValueError: If a required parameter is missing or empty.
        """
        safe_params = dict(params)

        # Validate required parameters are present and non-empty
        for key in self.required_params:
            val = safe_params.get(key)
            if val is None or (isinstance(val, str) and not val.strip()):
                raise ValueError(f"Sensor '{self.name}' requires non-empty parameter '{key}'")

        # Convert Python lists to SQL ARRAY literals — EXCEPT
        # `expected_values`, which the in-set sensors iterate directly with
        # a Jinja {% for %} loop to build a portable `IN (...)` list.
        # (PG `ANY(ARRAY[...])` has no MySQL/SQL Server equivalent.)
        #
        # SECURITY: expected_values is producer-controlled. The IN-list
        # template renders each value as `'{{ v }}'` — Jinja does NOT
        # escape SQL quotes, so a value containing `'` would break out of
        # the string literal (SQL injection). Escape single quotes here by
        # doubling them before the template ever sees the value.
        for key, val in safe_params.items():
            if key == "expected_values" and isinstance(val, list):
                safe_params[key] = [str(v).replace("'", "''") for v in val]
            elif isinstance(val, list):
                safe_params[key] = _list_to_sql_array(val)

        # Expose un-quoted copies as raw_* — catalog sensors need the bare
        # name as a STRING LITERAL (e.g. WHERE table_name = 'orders'),
        # which the quoted-identifier form can't provide.
        for key in ("schema_name", "table_name", "column_name"):
            if key in safe_params and safe_params[key] is not None:
                # Escape single quotes — these land inside string literals.
                safe_params[f"raw_{key}"] = str(safe_params[key]).replace("'", "''")

        # Quote identifier parameters
        for key in ("schema_name", "table_name", "column_name"):
            if key in safe_params and safe_params[key] is not None:
                safe_params[key] = _quote_identifier(str(safe_params[key]), quote_char)

        # Quote reference identifier parameters
        for key in ("reference_schema", "reference_table", "reference_column"):
            if key in safe_params and safe_params[key] is not None:
                raw = str(safe_params[key])
                if raw and not raw.startswith(quote_char) and raw != "[":
                    safe_params[key] = _quote_identifier(raw, quote_char)

        # Validate partition_filter
        if "partition_filter" in safe_params and safe_params["partition_filter"]:
            _validate_partition_filter(str(safe_params["partition_filter"]))

        template = Template(self.template_for(dialect))
        sql = str(template.render(**safe_params))

        # Strip any Python comments that leaked into SQL
        sql = _strip_python_comments(sql)

        return sql
