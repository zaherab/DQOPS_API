"""Tests for Sensor.render() hardening: comment stripping, list conversion,
required param validation, and reference identifier quoting."""

import pytest

from dq_platform.checks.sensors import get_sensor
from dq_platform.checks.sensors._base import (
    Sensor,
    SensorType,
    _list_to_sql_array,
    _strip_python_comments,
)


# ---------------------------------------------------------------------------
# _list_to_sql_array
# ---------------------------------------------------------------------------
class TestListToSqlArray:
    def test_empty_list_returns_typed_cast(self) -> None:
        assert _list_to_sql_array([]) == "[]::TEXT[]"

    def test_string_values_are_quoted(self) -> None:
        result = _list_to_sql_array(["active", "inactive"])
        assert result == "['active', 'inactive']"

    def test_numeric_values_are_unquoted(self) -> None:
        result = _list_to_sql_array([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_mixed_types(self) -> None:
        result = _list_to_sql_array(["a", 1, "b"])
        assert result == "['a', 1, 'b']"

    def test_single_quotes_in_values_are_escaped(self) -> None:
        result = _list_to_sql_array(["it's", "that's"])
        assert result == "['it''s', 'that''s']"


# ---------------------------------------------------------------------------
# _strip_python_comments
# ---------------------------------------------------------------------------
class TestStripPythonComments:
    def test_strips_noqa_comment(self) -> None:
        sql = "WHERE col::TEXT !~ '^[A-Z]+$'  # noqa: E501\n  AND col IS NOT NULL"
        result = _strip_python_comments(sql)
        assert "# noqa" not in result
        assert "AND col IS NOT NULL" in result

    def test_strips_type_comment(self) -> None:
        sql = "SELECT 1  # type: ignore\nFROM t"
        result = _strip_python_comments(sql)
        assert "# type:" not in result
        assert "FROM t" in result

    def test_strips_pylint_comment(self) -> None:
        sql = "SELECT 1  # pylint: disable=foo\nFROM t"
        result = _strip_python_comments(sql)
        assert "# pylint:" not in result

    def test_strips_pragma_comment(self) -> None:
        sql = "SELECT 1  # pragma: no cover\nFROM t"
        result = _strip_python_comments(sql)
        assert "# pragma:" not in result

    def test_preserves_hash_in_regex(self) -> None:
        """A # inside a regex pattern is not a Python comment."""
        sql = "WHERE col ~ '^#[0-9]+$'"
        result = _strip_python_comments(sql)
        assert result == sql

    def test_no_comments_unchanged(self) -> None:
        sql = "SELECT COUNT(*) FROM t WHERE x > 1"
        assert _strip_python_comments(sql) == sql

    def test_multiple_noqa_lines(self) -> None:
        sql = "WHEN col ~ '^[0-9]+$' THEN 1  # noqa: E501\nWHEN col ~ '^[a-z]+$' THEN 2  # noqa: E501\nELSE 3"
        result = _strip_python_comments(sql)
        assert result.count("# noqa") == 0
        assert "THEN 1" in result
        assert "THEN 2" in result
        assert "ELSE 3" in result


# ---------------------------------------------------------------------------
# Sensor.required_params validation
# ---------------------------------------------------------------------------
class TestRequiredParams:
    def test_missing_required_param_raises(self) -> None:
        sensor = Sensor(
            name="test",
            description="test",
            is_column_level=False,
            template="SELECT 1 FROM {{ table_name }} WHERE {{ condition }}",
            required_params=["condition"],
        )
        with pytest.raises(ValueError, match="requires non-empty parameter 'condition'"):
            sensor.render({"schema_name": "public", "table_name": "t"})

    def test_empty_string_required_param_raises(self) -> None:
        sensor = Sensor(
            name="test",
            description="test",
            is_column_level=False,
            template="SELECT 1 FROM {{ table_name }} WHERE {{ condition }}",
            required_params=["condition"],
        )
        with pytest.raises(ValueError, match="requires non-empty parameter 'condition'"):
            sensor.render({"schema_name": "public", "table_name": "t", "condition": ""})

    def test_whitespace_only_required_param_raises(self) -> None:
        sensor = Sensor(
            name="test",
            description="test",
            is_column_level=False,
            template="SELECT 1",
            required_params=["condition"],
        )
        with pytest.raises(ValueError, match="condition"):
            sensor.render({"condition": "   "})

    def test_valid_required_param_passes(self) -> None:
        sensor = Sensor(
            name="test",
            description="test",
            is_column_level=False,
            template="SELECT 1 FROM {{ schema_name }}.{{ table_name }} WHERE {{ condition }}",
            required_params=["condition"],
        )
        sql = sensor.render({"schema_name": "public", "table_name": "t", "condition": "x > 1"})
        assert "WHERE x > 1" in sql

    def test_no_required_params_accepts_anything(self) -> None:
        sensor = Sensor(
            name="test",
            description="test",
            is_column_level=False,
            template="SELECT 1",
        )
        sql = sensor.render({})
        assert "SELECT 1" in sql


# ---------------------------------------------------------------------------
# Foreign key sensors require reference_table
# ---------------------------------------------------------------------------
class TestForeignKeySensors:
    def test_fk_not_found_missing_reference_table_raises(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_NOT_FOUND_COUNT)
        with pytest.raises(ValueError, match="reference_table"):
            sensor.render(
                {
                    "schema_name": "public",
                    "table_name": "orders",
                    "column_name": "customer_id",
                }
            )

    def test_fk_not_found_with_reference_table_succeeds(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_NOT_FOUND_COUNT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "customer_id",
                "reference_table": "customers",
                "reference_schema": "public",
                "reference_column": "id",
            }
        )
        assert '"customers"' in sql
        assert '"orders"' in sql
        assert "NOT EXISTS" in sql

    def test_fk_found_percent_missing_reference_table_raises(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_FOUND_PERCENT)
        with pytest.raises(ValueError, match="reference_table"):
            sensor.render(
                {
                    "schema_name": "public",
                    "table_name": "orders",
                    "column_name": "customer_id",
                }
            )

    def test_fk_found_percent_with_reference_table_succeeds(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_FOUND_PERCENT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "customer_id",
                "reference_table": "customers",
            }
        )
        assert '"customers"' in sql


# ---------------------------------------------------------------------------
# SQL condition sensors require sql_condition
# ---------------------------------------------------------------------------
class TestSqlConditionSensors:
    def test_failed_count_empty_condition_raises(self) -> None:
        sensor = get_sensor(SensorType.SQL_CONDITION_FAILED_COUNT)
        with pytest.raises(ValueError, match="sql_condition"):
            sensor.render(
                {
                    "schema_name": "public",
                    "table_name": "users",
                    "sql_condition": "",
                }
            )

    def test_failed_count_with_condition_succeeds(self) -> None:
        sensor = get_sensor(SensorType.SQL_CONDITION_FAILED_COUNT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "users",
                "sql_condition": "status IS NOT NULL",
            }
        )
        assert "NOT (status IS NOT NULL)" in sql

    def test_failed_count_default_condition_from_params(self) -> None:
        """When sql_condition comes from default_params, it should work."""
        sensor = get_sensor(SensorType.SQL_CONDITION_FAILED_COUNT)
        assert sensor.default_params is not None
        assert sensor.default_params["sql_condition"] == "1=1"

    def test_column_condition_empty_raises(self) -> None:
        sensor = get_sensor(SensorType.SQL_CONDITION_FAILED_ON_COLUMN_COUNT)
        with pytest.raises(ValueError, match="sql_condition"):
            sensor.render(
                {
                    "schema_name": "public",
                    "table_name": "users",
                    "column_name": "email",
                    "sql_condition": "",
                }
            )


# ---------------------------------------------------------------------------
# expected_values → portable IN-list in render()
#
# In-set sensors render `expected_values` as a plain `IN (...)` list rather
# than PG `= ANY(ARRAY[...])` — `IN` transpiles cleanly to every dialect.
# ---------------------------------------------------------------------------
class TestListConversionInRender:
    def test_empty_list_renders_false_guard(self) -> None:
        # No expected_values → `1=0` guard keeps the SQL valid (matches
        # nothing) instead of producing an empty `IN ()`.
        sensor = get_sensor(SensorType.TEXT_IN_SET_PERCENT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "users",
                "column_name": "status",
                "expected_values": [],
            }
        )
        assert "1=0" in sql
        assert "ARRAY" not in sql

    def test_string_list_rendered_as_in_clause(self) -> None:
        sensor = get_sensor(SensorType.TEXT_IN_SET_PERCENT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "users",
                "column_name": "status",
                "expected_values": ["active", "inactive"],
            }
        )
        assert "IN ('active', 'inactive')" in sql
        assert "ARRAY" not in sql

    def test_number_list_rendered_as_in_clause(self) -> None:
        sensor = get_sensor(SensorType.NUMBER_IN_SET_PERCENT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "users",
                "column_name": "tier",
                "expected_values": [1, 2, 3],
            }
        )
        assert "IN ('1', '2', '3')" in sql
        assert "ARRAY" not in sql

    def test_expected_values_escapes_sql_quotes(self) -> None:
        # SECURITY: expected_values is producer-controlled. A single quote
        # in a value must be doubled so it can't break out of the string
        # literal — otherwise it's a SQL injection vector.
        sensor = get_sensor(SensorType.TEXT_IN_SET_PERCENT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "users",
                "column_name": "name",
                "expected_values": ["O'Brien", "x') OR 1=1 --"],
            }
        )
        # Quote doubled — value stays inside the literal.
        assert "O''Brien" in sql
        # The injection payload's quote is doubled too, so `OR 1=1`
        # remains inert text inside the string, not executable SQL.
        assert "x'') OR 1=1 --" in sql


# ---------------------------------------------------------------------------
# Reference identifier quoting
# ---------------------------------------------------------------------------
class TestReferenceQuoting:
    def test_reference_identifiers_are_quoted(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_NOT_FOUND_COUNT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "customer_id",
                "reference_table": "customers",
                "reference_schema": "myschema",
                "reference_column": "id",
            }
        )
        assert '"myschema"."customers"' in sql
        assert '"id"' in sql

    def test_mysql_backtick_quoting(self) -> None:
        sensor = get_sensor(SensorType.FOREIGN_KEY_NOT_FOUND_COUNT)
        sql = sensor.render(
            {
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "customer_id",
                "reference_table": "customers",
                "reference_schema": "myschema",
                "reference_column": "id",
            },
            quote_char="`",
        )
        assert "`myschema`.`customers`" in sql


# ---------------------------------------------------------------------------
# Noqa comments stripped from real sensors
# ---------------------------------------------------------------------------
class TestRealSensorNoqaStripping:
    """Verify the actual sensors that previously had # noqa produce clean SQL."""

    @pytest.mark.parametrize(
        "sensor_type",
        [
            SensorType.INVALID_EMAIL_FORMAT_COUNT,
            SensorType.INVALID_UUID_FORMAT_COUNT,
            SensorType.INVALID_IP4_FORMAT_COUNT,
            SensorType.INVALID_IP6_FORMAT_COUNT,
            SensorType.INVALID_PHONE_FORMAT_COUNT,
            SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
            SensorType.TEXT_NOT_MATCHING_NAME_PERCENT,
            SensorType.DETECTED_DATATYPE,
            SensorType.DETECTED_DATATYPE_CHANGED,
        ],
    )
    def test_no_python_comments_in_rendered_sql(self, sensor_type: SensorType) -> None:
        sensor = get_sensor(sensor_type)
        params: dict = {
            "schema_name": "public",
            "table_name": "test_table",
            "column_name": "test_col",
        }
        if sensor.default_params:
            params.update(sensor.default_params)
        sql = sensor.render(params)
        assert "# noqa" not in sql, f"Sensor {sensor_type.value} still has # noqa in SQL"
        assert "# type:" not in sql
        assert "# pylint:" not in sql


# ---------------------------------------------------------------------------
# Partition filter keyword validation
# ---------------------------------------------------------------------------
class TestPartitionFilterKeywords:
    def test_union_blocked(self) -> None:
        from dq_platform.checks.sensors._base import _validate_partition_filter

        with pytest.raises(ValueError, match="union"):
            _validate_partition_filter("date_col = 1 UNION SELECT 1")

    def test_drop_blocked(self) -> None:
        from dq_platform.checks.sensors._base import _validate_partition_filter

        with pytest.raises(ValueError, match="drop"):
            _validate_partition_filter("x = 1 DROP TABLE users")

    def test_exec_blocked(self) -> None:
        from dq_platform.checks.sensors._base import _validate_partition_filter

        with pytest.raises(ValueError, match="exec"):
            _validate_partition_filter("EXEC xp_cmdshell")

    @pytest.mark.parametrize(
        "safe_filter",
        [
            "execution_date >= '2024-01-01'",
            "updated_at > '2024-01-01'",
            "created_at BETWEEN '2024-01-01' AND '2024-12-31'",
            "deleted = false",
        ],
    )
    def test_no_false_positives(self, safe_filter: str) -> None:
        from dq_platform.checks.sensors._base import _validate_partition_filter

        result = _validate_partition_filter(safe_filter)
        assert result == safe_filter


# ---------------------------------------------------------------------------
# Identifier validation (schema allows dots, table/column do not)
# ---------------------------------------------------------------------------
class TestIdentifierValidation:
    def test_target_table_rejects_dots(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        with pytest.raises(ValueError, match="target_table"):
            _validate_identifier("schema.table", "target_table")

    def test_target_column_rejects_dots(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        with pytest.raises(ValueError, match="target_column"):
            _validate_identifier("table.column", "target_column")

    def test_target_schema_allows_dots(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        result = _validate_identifier("catalog.schema", "target_schema", allow_dot=True)
        assert result == "catalog.schema"

    def test_normal_identifier_passes(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        assert _validate_identifier("my_table", "target_table") == "my_table"

    def test_none_passes(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        assert _validate_identifier(None, "target_table") is None

    def test_injection_attempt_rejected(self) -> None:
        from dq_platform.schemas.check import _validate_identifier

        with pytest.raises(ValueError):
            _validate_identifier("users; DROP TABLE--", "target_table")
