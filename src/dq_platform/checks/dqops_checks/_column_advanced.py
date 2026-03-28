"""Advanced column-level DQOps check definitions.

Includes: Referential Integrity, Custom SQL (column-level), Phase 7 (Change Detection),
Phase 8 (Cross-Table Comparison), Phase 9 misc column checks, Phase 10 (Column-level Custom SQL),
Phase 11 (Generic Change Detection), Phase 12 (Anomaly Detection, Cross-Source Comparison).
"""

from dq_platform.checks.dqops_checks._base import (
    DQOpsCheck,
    DQOpsCheckType,
    RuleType,
    SensorType,
)

# =============================================================================
# Referential Integrity Checks (Column-level)
# =============================================================================

FOREIGN_KEY_NOT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FOREIGN_KEY_NOT_FOUND,
    description="Check that count of foreign keys not found in reference table is within limit",
    category="referential",
    sensor_type=SensorType.FOREIGN_KEY_NOT_FOUND_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "reference_schema": "public", "reference_column": "id"},
)

FOREIGN_KEY_FOUND_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FOREIGN_KEY_FOUND_PERCENT,
    description="Check that percentage of foreign keys found in reference table meets minimum",
    category="referential",
    sensor_type=SensorType.FOREIGN_KEY_FOUND_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Phase 7: Change Detection Checks (all 18)
# =============================================================================

NULLS_PERCENT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_1_DAY,
    description="Check that nulls percent change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

NULLS_PERCENT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_7_DAYS,
    description="Check that nulls percent change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

NULLS_PERCENT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE_30_DAYS,
    description="Check that nulls percent change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

DISTINCT_COUNT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_1_DAY,
    description="Check that distinct count change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

DISTINCT_COUNT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_7_DAYS,
    description="Check that distinct count change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

DISTINCT_COUNT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE_30_DAYS,
    description="Check that distinct count change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

DISTINCT_PERCENT_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_1_DAY,
    description="Check that distinct percent change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

DISTINCT_PERCENT_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_7_DAYS,
    description="Check that distinct percent change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

DISTINCT_PERCENT_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE_30_DAYS,
    description="Check that distinct percent change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

MEAN_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_1_DAY,
    description="Check that mean change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

MEAN_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_7_DAYS,
    description="Check that mean change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

MEAN_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE_30_DAYS,
    description="Check that mean change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEAN_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

MEDIAN_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_1_DAY,
    description="Check that median change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

MEDIAN_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_7_DAYS,
    description="Check that median change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

MEDIAN_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE_30_DAYS,
    description="Check that median change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.MEDIAN_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

SUM_CHANGE_1_DAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_1_DAY,
    description="Check that sum change from yesterday is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_1_DAY,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0},
)

SUM_CHANGE_7_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_7_DAYS,
    description="Check that sum change from 7 days ago is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_7_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 20.0},
)

SUM_CHANGE_30_DAYS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE_30_DAYS,
    description="Check that sum change from 30 days ago is within limit",
    category="change",
    sensor_type=SensorType.SUM_CHANGE_30_DAYS,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 50.0},
)

# =============================================================================
# Phase 8: Cross-Table Comparison Checks
# =============================================================================

TOTAL_ROW_COUNT_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_ROW_COUNT_MATCH_PERCENT,
    description="Check that row count matches reference table within tolerance",
    category="comparison",
    sensor_type=SensorType.ROW_COUNT_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 95.0, "reference_schema": "public"},
)

TOTAL_SUM_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_SUM_MATCH_PERCENT,
    description="Check that column sum matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.SUM_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_MIN_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_MIN_MATCH_PERCENT,
    description="Check that column minimum matches reference column",
    category="comparison",
    sensor_type=SensorType.MIN_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_MAX_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_MAX_MATCH_PERCENT,
    description="Check that column maximum matches reference column",
    category="comparison",
    sensor_type=SensorType.MAX_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0, "reference_schema": "public", "reference_column": "id"},
)

TOTAL_AVERAGE_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_AVERAGE_MATCH_PERCENT,
    description="Check that column average matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.AVERAGE_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={
        "min_percent": 95.0,
        "reference_schema": "public",
        "reference_column": "id",
        "tolerance_percent": 0.01,
    },
)

TOTAL_NOT_NULL_COUNT_MATCH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TOTAL_NOT_NULL_COUNT_MATCH_PERCENT,
    description="Check that not-null count matches reference column within tolerance",
    category="comparison",
    sensor_type=SensorType.NOT_NULL_COUNT_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "reference_schema": "public", "reference_column": "id"},
)

# =============================================================================
# Phase 9: Column-level Misc Checks
# =============================================================================

COLUMN_EXISTS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_EXISTS,
    description="Check that a column exists in the table",
    category="schema",
    sensor_type=SensorType.COLUMN_EXISTS,
    rule_type=RuleType.IS_TRUE,
    is_column_level=True,
)

DATA_FRESHNESS_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_FRESHNESS,
    description="Check that data is fresh (max age in seconds)",
    category="timeliness",
    sensor_type=SensorType.DATA_FRESHNESS,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 86400},  # 24 hours
)

DATA_INGESTION_DELAY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_INGESTION_DELAY,
    description="Check that data ingestion delay is within acceptable limit",
    category="timeliness",
    sensor_type=SensorType.DATA_INGESTION_DELAY,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 3600},  # 1 hour
)

COLUMN_TYPE_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_TYPE_CHANGED,
    description="Check that column type has not changed unexpectedly",
    category="schema",
    sensor_type=SensorType.COLUMN_TYPE_CHANGED,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=True,
    default_params={"forbidden_value": 1, "expected_type": "character varying"},
)

# =============================================================================
# Phase 10: Column-level Custom SQL Checks
# =============================================================================

SQL_CONDITION_FAILED_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_FAILED_ON_COLUMN,
    description="Check that count of column values failing SQL condition is within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_FAILED_ON_COLUMN_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "sql_condition": "1=1"},
)

SQL_CONDITION_PASSED_PERCENT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_CONDITION_PASSED_PERCENT_ON_COLUMN,
    description="Check that percentage of column values passing SQL condition meets minimum",
    category="custom_sql",
    sensor_type=SensorType.SQL_CONDITION_PASSED_ON_COLUMN_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "sql_condition": "1=1"},
)

SQL_AGGREGATE_EXPRESSION_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_AGGREGATE_EXPRESSION_ON_COLUMN,
    description="Check that SQL aggregate expression on column returns expected value",
    category="custom_sql",
    sensor_type=SensorType.SQL_AGGREGATE_ON_COLUMN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"sql_expression": "COUNT(*)"},
)

SQL_INVALID_VALUE_COUNT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SQL_INVALID_VALUE_COUNT_ON_COLUMN,
    description="Check that count of invalid column values is within limit",
    category="custom_sql",
    sensor_type=SensorType.SQL_INVALID_VALUE_ON_COLUMN_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "invalid_values": "''"},
)

IMPORT_CUSTOM_RESULT_ON_COLUMN_CHECK = DQOpsCheck(
    name=DQOpsCheckType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    description="Import and validate external data quality result for column",
    category="custom_sql",
    sensor_type=SensorType.IMPORT_CUSTOM_RESULT_ON_COLUMN,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"imported_value": 0.0},
)

# =============================================================================
# Phase 11: Generic Change Detection Checks
# =============================================================================

ROW_COUNT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_CHANGE,
    description="Check that row count change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.ROW_COUNT_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=False,
    default_params={"max_change_percent": 10.0, "baseline_count": 0},
)

NULLS_PERCENT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_CHANGE,
    description="Check that null percentage change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.NULLS_PERCENT_CHANGE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 5.0, "baseline_percent": 0.0},
)

DISTINCT_COUNT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_CHANGE,
    description="Check that distinct count change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.DISTINCT_COUNT_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_count": 0},
)

DISTINCT_PERCENT_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_CHANGE,
    description="Check that distinct percent change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.DISTINCT_PERCENT_CHANGE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
    default_params={"max_value": 5.0, "baseline_percent": 0.0},
)

MEAN_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_CHANGE,
    description="Check that mean value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.MEAN_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)

MEDIAN_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_CHANGE,
    description="Check that median value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.MEDIAN_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)

SUM_CHANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_CHANGE,
    description="Check that sum value change from baseline is within limit",
    category="change_detection",
    sensor_type=SensorType.SUM_CHANGE,
    rule_type=RuleType.MAX_CHANGE_PERCENT,
    is_column_level=True,
    default_params={"max_change_percent": 10.0, "baseline_value": 0.0},
)

# =============================================================================
# Anomaly Detection Checks (Phase 12)
# =============================================================================

ROW_COUNT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_ANOMALY,
    description="Detect anomalous row count using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.ROW_COUNT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=False,
    default_params={"anomaly_percent": 0.05},
)

DATA_FRESHNESS_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATA_FRESHNESS_ANOMALY,
    description="Detect anomalous data freshness using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DATA_FRESHNESS,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

NULLS_PERCENT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT_ANOMALY,
    description="Detect anomalous null percentage using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

DISTINCT_COUNT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_ANOMALY,
    description="Detect anomalous distinct count using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

DISTINCT_PERCENT_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT_ANOMALY,
    description="Detect anomalous distinct percentage using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.DISTINCT_PERCENT,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

SUM_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_ANOMALY,
    description="Detect anomalous sum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MEAN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_ANOMALY,
    description="Detect anomalous mean value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MEDIAN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_ANOMALY,
    description="Detect anomalous median value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MEDIAN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MIN_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_ANOMALY,
    description="Detect anomalous minimum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

MAX_ANOMALY_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_ANOMALY,
    description="Detect anomalous maximum value using IQR-based statistical analysis",
    category="anomaly",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.ANOMALY_PERCENTILE,
    is_column_level=True,
    default_params={"anomaly_percent": 0.05},
)

# =============================================================================
# Cross-Source Comparison Checks (Phase 12)
# =============================================================================

ROW_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.ROW_COUNT_MATCH,
    description="Compare row count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.ROW_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 100.0},
)

COLUMN_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.COLUMN_COUNT_MATCH,
    description="Compare column count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.COLUMN_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=False,
    default_params={"min_percent": 100.0},
)

SUM_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_MATCH,
    description="Compare sum of values between source and reference connection",
    category="comparison",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MIN_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_MATCH,
    description="Compare minimum value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MAX_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_MATCH,
    description="Compare maximum value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

MEAN_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_MATCH,
    description="Compare mean value between source and reference connection",
    category="comparison",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

NOT_NULL_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULL_COUNT_MATCH,
    description="Compare not-null count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.NOT_NULLS_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

NULL_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_COUNT_MATCH,
    description="Compare null count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.NULLS_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)

DISTINCT_COUNT_MATCH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT_MATCH,
    description="Compare distinct count between source and reference connection",
    category="comparison",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 100.0},
)
