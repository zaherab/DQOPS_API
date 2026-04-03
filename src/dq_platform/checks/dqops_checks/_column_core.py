"""Core column-level DQOps check definitions.

Includes: Null/Completeness, Uniqueness, Numeric/Statistical, Text/Pattern,
Phase 1 (Whitespace & Text), Phase 2 (Geographic & Numeric Percent),
Phase 3 (Statistical & Percentile), Phase 10 (Text Length Percent).
"""

from dq_platform.checks.dqops_checks._base import (
    DQOpsCheck,
    DQOpsCheckType,
    RuleType,
    SensorType,
)

# =============================================================================
# Null/Completeness Checks (Column-level)
# =============================================================================

NULLS_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_COUNT,
    description="Check that null count is within limit",
    category="nulls",
    sensor_type=SensorType.NULLS_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NULLS_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULLS_PERCENT,
    description="Check that null percentage is within limit",
    category="nulls",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

NOT_NULLS_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULLS_COUNT,
    description="Check that non-null count meets minimum",
    category="nulls",
    sensor_type=SensorType.NOT_NULLS_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1},
)

NOT_NULLS_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NOT_NULLS_PERCENT,
    description="Check that non-null percentage meets minimum",
    category="nulls",
    sensor_type=SensorType.NOT_NULLS_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

EMPTY_COLUMN_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_COLUMN_FOUND,
    description="Check that column is not completely empty",
    category="nulls",
    sensor_type=SensorType.NULLS_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 99.99},  # Allow at most 99.99% nulls
)

# =============================================================================
# Uniqueness Checks (Column-level)
# =============================================================================

DISTINCT_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_COUNT,
    description="Check that distinct count is within range",
    category="uniqueness",
    sensor_type=SensorType.DISTINCT_COUNT,
    rule_type=RuleType.MIN_MAX_COUNT,
    is_column_level=True,
)

DISTINCT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DISTINCT_PERCENT,
    description="Check that distinct percentage is within range",
    category="uniqueness",
    sensor_type=SensorType.DISTINCT_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

DUPLICATE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_COUNT,
    description="Check that duplicate count is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

DUPLICATE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DUPLICATE_PERCENT,
    description="Check that duplicate percentage is within limit",
    category="uniqueness",
    sensor_type=SensorType.DUPLICATE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

# =============================================================================
# Numeric/Statistical Checks (Column-level)
# =============================================================================

MIN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_IN_RANGE,
    description="Check that column minimum is within range",
    category="numeric",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MAX_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_IN_RANGE,
    description="Check that column maximum is within range",
    category="numeric",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

SUM_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SUM_IN_RANGE,
    description="Check that column sum is within range",
    category="numeric",
    sensor_type=SensorType.SUM_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MEAN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEAN_IN_RANGE,
    description="Check that column mean is within range",
    category="numeric",
    sensor_type=SensorType.MEAN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MEDIAN_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MEDIAN_IN_RANGE,
    description="Check that column median is within range",
    category="numeric",
    sensor_type=SensorType.MEDIAN_VALUE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

NUMBER_BELOW_MIN_VALUE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_BELOW_MIN_VALUE,
    description="Check that count of values below min is within limit",
    category="numeric",
    sensor_type=SensorType.MIN_VALUE,
    rule_type=RuleType.MIN_VALUE,
    is_column_level=True,
)

NUMBER_ABOVE_MAX_VALUE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE,
    description="Check that count of values above max is within limit",
    category="numeric",
    sensor_type=SensorType.MAX_VALUE,
    rule_type=RuleType.MAX_VALUE,
    is_column_level=True,
)

NUMBER_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_IN_RANGE_PERCENT,
    description="Check that percentage of numbers in range meets minimum",
    category="numeric",
    sensor_type=SensorType.NUMBER_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_value": 0, "max_value": 100},
)

# =============================================================================
# Text/Pattern Checks (Column-level)
# =============================================================================

TEXT_MIN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MIN_LENGTH,
    description="Check that minimum text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MIN_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_MAX_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MAX_LENGTH,
    description="Check that maximum text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MAX_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_MEAN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MEAN_LENGTH,
    description="Check that average text length is within range",
    category="text",
    sensor_type=SensorType.TEXT_MEAN_LENGTH,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

TEXT_LENGTH_BELOW_MIN_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH,
    description="Check that count of texts below min length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_BELOW_MIN,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_LENGTH_ABOVE_MAX_LENGTH_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH,
    description="Check that count of texts above max length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_ABOVE_MAX,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_LENGTH_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_IN_RANGE_PERCENT,
    description="Check that percentage of texts with length in range meets minimum",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_length": 1, "max_length": 255},
)

EMPTY_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_TEXT_FOUND,
    description="Check that empty string count is within limit",
    category="text",
    sensor_type=SensorType.EMPTY_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

WHITESPACE_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.WHITESPACE_TEXT_FOUND,
    description="Check that whitespace-only text count is within limit",
    category="text",
    sensor_type=SensorType.WHITESPACE_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_NOT_MATCHING_REGEX_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_REGEX_FOUND,
    description="Check that count of values not matching regex is within limit",
    category="text",
    sensor_type=SensorType.REGEX_NOT_MATCH_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "regex_pattern": ".*"},
)

# =============================================================================
# Phase 1: Whitespace & Text Checks
# =============================================================================

EMPTY_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EMPTY_TEXT_PERCENT,
    description="Check that empty text percentage is within limit",
    category="text",
    sensor_type=SensorType.EMPTY_TEXT_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

WHITESPACE_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.WHITESPACE_TEXT_PERCENT,
    description="Check that whitespace-only text percentage is within limit",
    category="text",
    sensor_type=SensorType.WHITESPACE_TEXT_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

NULL_PLACEHOLDER_TEXT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_PLACEHOLDER_TEXT_FOUND,
    description="Check that null placeholder text count is within limit",
    category="text",
    sensor_type=SensorType.NULL_PLACEHOLDER_TEXT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NULL_PLACEHOLDER_TEXT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NULL_PLACEHOLDER_TEXT_PERCENT,
    description="Check that null placeholder text percentage is within limit",
    category="text",
    sensor_type=SensorType.NULL_PLACEHOLDER_TEXT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

TEXT_SURROUNDED_BY_WHITESPACE_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_FOUND,
    description="Check that text surrounded by whitespace count is within limit",
    category="text",
    sensor_type=SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

TEXT_SURROUNDED_BY_WHITESPACE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_SURROUNDED_BY_WHITESPACE_PERCENT,
    description="Check that text surrounded by whitespace percentage is within limit",
    category="text",
    sensor_type=SensorType.TEXT_SURROUNDED_WHITESPACE_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

TEXTS_NOT_MATCHING_REGEX_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXTS_NOT_MATCHING_REGEX_PERCENT,
    description="Check that percentage of texts not matching regex is within limit",
    category="text",
    sensor_type=SensorType.REGEX_NOT_MATCH_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "regex_pattern": ".*"},
)

TEXT_MATCHING_REGEX_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MATCHING_REGEX_PERCENT,
    description="Check that percentage of texts matching regex meets minimum",
    category="text",
    sensor_type=SensorType.REGEX_MATCH_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "regex_pattern": ".*"},
)

MIN_WORD_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MIN_WORD_COUNT,
    description="Check that minimum word count meets expected value",
    category="text",
    sensor_type=SensorType.TEXT_WORD_COUNT_MIN,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

MAX_WORD_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.MAX_WORD_COUNT,
    description="Check that maximum word count is within expected range",
    category="text",
    sensor_type=SensorType.TEXT_WORD_COUNT_MAX,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

# =============================================================================
# Phase 2: Geographic & Numeric Percent Checks
# =============================================================================

VALID_LATITUDE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.VALID_LATITUDE_PERCENT,
    description="Check that valid latitude percentage meets minimum",
    category="geographic",
    sensor_type=SensorType.VALID_LATITUDE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

VALID_LONGITUDE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.VALID_LONGITUDE_PERCENT,
    description="Check that valid longitude percentage meets minimum",
    category="geographic",
    sensor_type=SensorType.VALID_LONGITUDE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

NUMBER_BELOW_MIN_VALUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_BELOW_MIN_VALUE_PERCENT,
    description="Check that percentage of values below min is within limit",
    category="numeric",
    sensor_type=SensorType.NUMBER_BELOW_MIN_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "min_value": 0},
)

NUMBER_ABOVE_MAX_VALUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_ABOVE_MAX_VALUE_PERCENT,
    description="Check that percentage of values above max is within limit",
    category="numeric",
    sensor_type=SensorType.NUMBER_ABOVE_MAX_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "max_value": 100},
)

NEGATIVE_VALUES_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NEGATIVE_VALUES,
    description="Check that negative value count is within limit",
    category="numeric",
    sensor_type=SensorType.NEGATIVE_VALUE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

NEGATIVE_VALUES_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NEGATIVE_VALUES_PERCENT,
    description="Check that negative value percentage is within limit",
    category="numeric",
    sensor_type=SensorType.NEGATIVE_VALUE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

NON_NEGATIVE_VALUES_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NON_NEGATIVE_VALUES,
    description="Check that non-negative value count meets minimum",
    category="numeric",
    sensor_type=SensorType.NON_NEGATIVE_VALUE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1},
)

NON_NEGATIVE_VALUES_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NON_NEGATIVE_VALUES_PERCENT,
    description="Check that non-negative value percentage meets minimum",
    category="numeric",
    sensor_type=SensorType.NON_NEGATIVE_VALUE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

INTEGER_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INTEGER_IN_RANGE_PERCENT,
    description="Check that percentage of integers in range meets minimum",
    category="numeric",
    sensor_type=SensorType.INTEGER_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_value": 0, "max_value": 100},
)

# =============================================================================
# Phase 3: Statistical & Percentile Checks
# =============================================================================

SAMPLE_STDDEV_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SAMPLE_STDDEV_IN_RANGE,
    description="Check that sample standard deviation is within expected range",
    category="statistical",
    sensor_type=SensorType.STDDEV_SAMPLE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

POPULATION_STDDEV_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.POPULATION_STDDEV_IN_RANGE,
    description="Check that population standard deviation is within expected range",
    category="statistical",
    sensor_type=SensorType.STDDEV_POPULATION,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

SAMPLE_VARIANCE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.SAMPLE_VARIANCE_IN_RANGE,
    description="Check that sample variance is within expected range",
    category="statistical",
    sensor_type=SensorType.VARIANCE_SAMPLE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

POPULATION_VARIANCE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.POPULATION_VARIANCE_IN_RANGE,
    description="Check that population variance is within expected range",
    category="statistical",
    sensor_type=SensorType.VARIANCE_POPULATION,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_IN_RANGE,
    description="Check that percentile value is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
    default_params={"percentile": 0.5},
)

PERCENTILE_10_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_10_IN_RANGE,
    description="Check that 10th percentile is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_10,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_25_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_25_IN_RANGE,
    description="Check that 25th percentile (Q1) is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_25,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_75_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_75_IN_RANGE,
    description="Check that 75th percentile (Q3) is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_75,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

PERCENTILE_90_IN_RANGE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.PERCENTILE_90_IN_RANGE,
    description="Check that 90th percentile is within expected range",
    category="statistical",
    sensor_type=SensorType.PERCENTILE_90,
    rule_type=RuleType.MIN_MAX_VALUE,
    is_column_level=True,
)

# =============================================================================
# Phase 10: Text Length Percent Checks (Column-level)
# =============================================================================

TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_BELOW_MIN_LENGTH_PERCENT,
    description="Check that percentage of text values below minimum length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_BELOW_MIN_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "min_length": 1},
)

TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_LENGTH_ABOVE_MAX_LENGTH_PERCENT,
    description="Check that percentage of text values above maximum length is within limit",
    category="text",
    sensor_type=SensorType.TEXT_LENGTH_ABOVE_MAX_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0, "max_length": 255},
)
