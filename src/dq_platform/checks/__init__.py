"""Data quality checks module."""

from dq_platform.checks.dqops_checks import (
    CHECK_REGISTRY,
    DQOpsCheck,
    DQOpsCheckType,
    get_check,
    get_checks_by_category,
    get_column_level_checks,
    get_table_level_checks,
    list_checks,
)
from dq_platform.checks.dqops_executor import (
    DQOpsExecutor,
    DQOpsLocalExecutor,
    CheckExecutionResult,
    run_dqops_check,
)
from dq_platform.checks.gx_executor import GreatExpectationsExecutor, run_gx_check
from dq_platform.checks.gx_registry import (
    GX_EXPECTATION_MAP,
    build_expectation,
    get_check_description,
    is_column_level_check,
)
from dq_platform.checks.rules import (
    RULE_REGISTRY,
    RuleResult,
    RuleType,
    Severity,
    evaluate_rule,
    get_rule,
    list_rules,
)
from dq_platform.checks.sensors import (
    SENSOR_REGISTRY,
    Sensor,
    SensorType,
    get_column_level_sensors,
    get_sensor,
    get_table_level_sensors,
    list_sensors,
)

__all__ = [
    # DQOps checks
    "DQOpsCheck",
    "DQOpsCheckType",
    "get_check",
    "list_checks",
    "get_column_level_checks",
    "get_table_level_checks",
    "get_checks_by_category",
    "CHECK_REGISTRY",
    # DQOps executor
    "DQOpsExecutor",
    "DQOpsLocalExecutor",
    "CheckExecutionResult",
    "run_dqops_check",
    # Sensors
    "Sensor",
    "SensorType",
    "get_sensor",
    "list_sensors",
    "get_column_level_sensors",
    "get_table_level_sensors",
    "SENSOR_REGISTRY",
    # Rules
    "RuleResult",
    "RuleType",
    "Severity",
    "get_rule",
    "evaluate_rule",
    "list_rules",
    "RULE_REGISTRY",
    # Great Expectations (legacy)
    "GreatExpectationsExecutor",
    "run_gx_check",
    "build_expectation",
    "get_check_description",
    "is_column_level_check",
    "GX_EXPECTATION_MAP",
]
