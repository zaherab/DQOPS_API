"""Table-level DQOps check definitions (is_column_level=False)."""

from dq_platform.checks.dqops_checks._base import (
    DQOpsCheck,
    DQOpsCheckType,
    RuleType,
    SensorType,
)

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
# Availability Check
# =============================================================================

TABLE_AVAILABILITY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TABLE_AVAILABILITY,
    description="Check that table is accessible",
    category="availability",
    sensor_type=SensorType.TABLE_AVAILABILITY,
    rule_type=RuleType.IS_TRUE,
    is_column_level=False,
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
# Table-level Custom SQL Checks (Phase 10)
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

SQL_CONDITION_PASSED_PERCENT_ON_TABLE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_TABLE,
    description="Check that percentage of rows passing SQL condition meets minimum",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_PASSED_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 95.0, "sql_condition": "1=1"},
)

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
# Schema Detection Checks (Phase 10 - Table-level)
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
# Table-Level Misc Checks (Phase 9)
# =============================================================================

RELOAD_LAG_CHECK = DQOpsCheck(
    name=DQOpsCheckType.RELOAD_LAG,
    description="Check that table reload lag is within acceptable limit",
    category="timeliness",
    sensor_type=SensorType.RELOAD_LAG,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=False,
    default_params={"max_value": 86400},  # 24 hours
)

# =============================================================================
# Import External Results (Phase 11 - Table-level)
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
