"""Cross-table sensor templates must declare `reference_table` as required.

Historical context: cross-table sensors (SUM_MATCH_PERCENT, ROW_COUNT_MATCH_PERCENT, …)
were declared without `required_params`. Rendering with an empty `reference_table`
produced invalid SQL like `FROM public.` and every execution of a check built from
these sensors failed in production. These tests lock in the contract so the class
of bug can't come back.
"""

from __future__ import annotations

import pytest

from dq_platform.checks.sensors import SensorType, get_sensor

_CROSS_TABLE_SENSORS = [
    SensorType.ROW_COUNT_MATCH_PERCENT,
    SensorType.SUM_MATCH_PERCENT,
    SensorType.MIN_MATCH_PERCENT,
    SensorType.MAX_MATCH_PERCENT,
    SensorType.AVERAGE_MATCH_PERCENT,
    SensorType.NOT_NULL_COUNT_MATCH_PERCENT,
]

# Every sensor whose Jinja template references a parameter that isn't an
# auto-supplied identifier MUST list that parameter in required_params, or
# rendering will silently emit invalid SQL. These are the non-cross-table
# members of that contract caught by the systematic audit.
_OTHER_PARAM_SENSITIVE_SENSORS: list[tuple[SensorType, str]] = [
    (SensorType.DATA_STALENESS, "timestamp_column"),
    (SensorType.SQL_AGGREGATE_VALUE, "sql_expression"),
    # Change-detection sensors — 21 variants all query check_results with
    # a check_id that's never injected. Declaring it required makes
    # create-time validation fail loudly instead of silently.
    (SensorType.NULLS_PERCENT_CHANGE_1_DAY, "check_id"),
    (SensorType.NULLS_PERCENT_CHANGE_7_DAYS, "check_id"),
    (SensorType.NULLS_PERCENT_CHANGE_30_DAYS, "check_id"),
    (SensorType.DISTINCT_COUNT_CHANGE_1_DAY, "check_id"),
    (SensorType.DISTINCT_COUNT_CHANGE_7_DAYS, "check_id"),
    (SensorType.DISTINCT_COUNT_CHANGE_30_DAYS, "check_id"),
    (SensorType.DISTINCT_PERCENT_CHANGE_1_DAY, "check_id"),
    (SensorType.DISTINCT_PERCENT_CHANGE_7_DAYS, "check_id"),
    (SensorType.DISTINCT_PERCENT_CHANGE_30_DAYS, "check_id"),
    (SensorType.MEAN_CHANGE_1_DAY, "check_id"),
    (SensorType.MEAN_CHANGE_7_DAYS, "check_id"),
    (SensorType.MEAN_CHANGE_30_DAYS, "check_id"),
    (SensorType.MEDIAN_CHANGE_1_DAY, "check_id"),
    (SensorType.MEDIAN_CHANGE_7_DAYS, "check_id"),
    (SensorType.MEDIAN_CHANGE_30_DAYS, "check_id"),
    (SensorType.SUM_CHANGE_1_DAY, "check_id"),
    (SensorType.SUM_CHANGE_7_DAYS, "check_id"),
    (SensorType.SUM_CHANGE_30_DAYS, "check_id"),
    (SensorType.ROW_COUNT_CHANGE_1_DAY, "check_id"),
    (SensorType.ROW_COUNT_CHANGE_7_DAYS, "check_id"),
    (SensorType.ROW_COUNT_CHANGE_30_DAYS, "check_id"),
]


@pytest.mark.parametrize("sensor_type,required", _OTHER_PARAM_SENSITIVE_SENSORS)
def test_param_sensitive_sensor_declares_required(sensor_type: SensorType, required: str) -> None:
    sensor = get_sensor(sensor_type)
    assert required in sensor.required_params, (
        f"{sensor_type.value}: {required!r} must be in required_params so "
        "create-time validation rejects bare checks that cannot render"
    )


@pytest.mark.parametrize("sensor_type", _CROSS_TABLE_SENSORS)
def test_cross_table_sensor_declares_reference_table_required(sensor_type: SensorType) -> None:
    sensor = get_sensor(sensor_type)
    assert "reference_table" in sensor.required_params, (
        f"{sensor_type.value}: reference_table must be required_params, or render() will silently emit broken SQL"
    )


@pytest.mark.parametrize("sensor_type", _CROSS_TABLE_SENSORS)
def test_cross_table_sensor_render_rejects_missing_reference_table(
    sensor_type: SensorType,
) -> None:
    sensor = get_sensor(sensor_type)
    with pytest.raises(ValueError, match="reference_table"):
        sensor.render(
            {
                "schema_name": "public",
                "table_name": "t",
                "column_name": "c",
                # reference_table intentionally omitted
            }
        )


@pytest.mark.parametrize("sensor_type", _CROSS_TABLE_SENSORS)
def test_cross_table_sensor_render_succeeds_with_reference_table(
    sensor_type: SensorType,
) -> None:
    sensor = get_sensor(sensor_type)
    sql = sensor.render(
        {
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "total",
            "reference_table": "orders_backup",
        }
    )
    assert "orders_backup" in sql
