"""Pattern/detection column-level DQOps check definitions.

Includes: Pattern/Format, PII Detection (Phase 6), Geographic, Boolean,
DateTime, Phase 4 (Accepted Values & Domain), Phase 5 (Date Pattern & Data Type Detection).
"""

from dq_platform.checks.dqops_checks._base import (
    DQOpsCheck,
    DQOpsCheckType,
    RuleType,
    SensorType,
)

# =============================================================================
# Pattern/Format Checks (Column-level)
# =============================================================================

INVALID_EMAIL_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_EMAIL_FORMAT_FOUND,
    description="Check that count of invalid email formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_EMAIL_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_EMAIL_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_EMAIL_FORMAT_PERCENT,
    description="Check that percentage of invalid email formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_EMAIL_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_UUID_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_UUID_FORMAT_FOUND,
    description="Check that count of invalid UUID formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_UUID_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_UUID_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_UUID_FORMAT_PERCENT,
    description="Check that percentage of invalid UUID formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_UUID_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_IP4_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP4_FORMAT_FOUND,
    description="Check that count of invalid IPv4 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP4_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_IP4_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP4_FORMAT_PERCENT,
    description="Check that percentage of invalid IPv4 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP4_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_IP6_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP6_FORMAT_FOUND,
    description="Check that count of invalid IPv6 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP6_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_IP6_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_IP6_FORMAT_PERCENT,
    description="Check that percentage of invalid IPv6 formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_IP6_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_USA_PHONE_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_PHONE_FORMAT_FOUND,
    description="Check that count of invalid USA phone formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_PHONE_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_USA_PHONE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_PHONE_FORMAT_PERCENT,
    description="Check that percentage of invalid USA phone formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_PHONE_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

INVALID_USA_ZIPCODE_FORMAT_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_FOUND,
    description="Check that count of invalid USA zipcode formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_USA_ZIPCODE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_USA_ZIPCODE_FORMAT_PERCENT,
    description="Check that percentage of invalid USA zipcode formats is within limit",
    category="patterns",
    sensor_type=SensorType.INVALID_ZIPCODE_FORMAT_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

# =============================================================================
# PII Detection Checks (Phase 6)
# =============================================================================

CONTAINS_USA_PHONE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_USA_PHONE_PERCENT,
    description="Check that percentage of values containing phone numbers is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_PHONE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_EMAIL_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_EMAIL_PERCENT,
    description="Check that percentage of values containing emails is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_EMAIL_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_USA_ZIPCODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_USA_ZIPCODE_PERCENT,
    description="Check that percentage of values containing zipcodes is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_ZIPCODE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_IP4_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_IP4_PERCENT,
    description="Check that percentage of values containing IPv4 is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_IP4_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

CONTAINS_IP6_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.CONTAINS_IP6_PERCENT,
    description="Check that percentage of values containing IPv6 is within limit",
    category="pii",
    sensor_type=SensorType.CONTAINS_IP6_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

# =============================================================================
# Geographic Checks (Column-level)
# =============================================================================

INVALID_LATITUDE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_LATITUDE,
    description="Check that invalid latitude count is within limit",
    category="geographic",
    sensor_type=SensorType.INVALID_LATITUDE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

INVALID_LONGITUDE_CHECK = DQOpsCheck(
    name=DQOpsCheckType.INVALID_LONGITUDE,
    description="Check that invalid longitude count is within limit",
    category="geographic",
    sensor_type=SensorType.INVALID_LONGITUDE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0},
)

# =============================================================================
# Boolean Checks (Column-level)
# =============================================================================

TRUE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TRUE_PERCENT,
    description="Check that true percentage is within range",
    category="boolean",
    sensor_type=SensorType.TRUE_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

FALSE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.FALSE_PERCENT,
    description="Check that false percentage is within range",
    category="boolean",
    sensor_type=SensorType.FALSE_PERCENT,
    rule_type=RuleType.MIN_MAX_PERCENT,
    is_column_level=True,
)

# =============================================================================
# DateTime Checks (Column-level)
# =============================================================================

DATE_VALUES_IN_FUTURE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATE_VALUES_IN_FUTURE_PERCENT,
    description="Check that percentage of future dates is within limit",
    category="datetime",
    sensor_type=SensorType.FUTURE_DATE_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0},
)

DATE_IN_RANGE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DATE_IN_RANGE_PERCENT,
    description="Check that percentage of dates is within valid range",
    category="datetime",
    sensor_type=SensorType.DATE_IN_RANGE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "min_date": "1900-01-01", "max_date": "2099-12-31"},
)

# =============================================================================
# Phase 4: Accepted Values & Domain Checks
# =============================================================================

TEXT_FOUND_IN_SET_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_FOUND_IN_SET_PERCENT,
    description="Check that percentage of texts in expected set meets minimum",
    category="accepted_values",
    sensor_type=SensorType.TEXT_IN_SET_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "expected_values": []},
)

NUMBER_FOUND_IN_SET_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.NUMBER_FOUND_IN_SET_PERCENT,
    description="Check that percentage of numbers in expected set meets minimum",
    category="accepted_values",
    sensor_type=SensorType.NUMBER_IN_SET_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "expected_values": []},
)

EXPECTED_TEXT_VALUES_IN_USE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_TEXT_VALUES_IN_USE_COUNT,
    description="Check that expected text values are actually used",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_TEXT_IN_USE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": []},
)

EXPECTED_NUMBERS_IN_USE_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_NUMBERS_IN_USE_COUNT,
    description="Check that expected numbers are actually used",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_NUMBER_IN_USE_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": []},
)

EXPECTED_TEXTS_IN_TOP_VALUES_COUNT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.EXPECTED_TEXTS_IN_TOP_VALUES_COUNT,
    description="Check that expected texts appear in top N values",
    category="accepted_values",
    sensor_type=SensorType.EXPECTED_TEXTS_TOP_N_COUNT,
    rule_type=RuleType.MIN_COUNT,
    is_column_level=True,
    default_params={"min_count": 1, "expected_values": [], "top_n": 10},
)

TEXT_VALID_COUNTRY_CODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_VALID_COUNTRY_CODE_PERCENT,
    description="Check that valid country code percentage meets minimum",
    category="accepted_values",
    sensor_type=SensorType.VALID_COUNTRY_CODE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_VALID_CURRENCY_CODE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_VALID_CURRENCY_CODE_PERCENT,
    description="Check that valid currency code percentage meets minimum",
    category="accepted_values",
    sensor_type=SensorType.VALID_CURRENCY_CODE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

# =============================================================================
# Phase 5: Date Pattern & Data Type Detection Checks
# =============================================================================

TEXT_NOT_MATCHING_DATE_PATTERN_FOUND_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_FOUND,
    description="Check that count of texts not matching date pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_DATE_COUNT,
    rule_type=RuleType.MAX_COUNT,
    is_column_level=True,
    default_params={"max_count": 0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_DATE_PATTERN_PERCENT,
    description="Check that percentage of texts not matching date pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_DATE_COUNT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 0.0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_MATCH_DATE_FORMAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_MATCH_DATE_FORMAT_PERCENT,
    description="Check that percentage of texts matching date format meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_MATCH_DATE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0, "date_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
)

TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_NOT_MATCHING_NAME_PATTERN_PERCENT,
    description="Check that percentage of texts not matching name pattern is within limit",
    category="datatype",
    sensor_type=SensorType.TEXT_NOT_MATCHING_NAME_PERCENT,
    rule_type=RuleType.MAX_PERCENT,
    is_column_level=True,
    default_params={"max_percent": 5.0},
)

TEXT_PARSABLE_TO_BOOLEAN_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_BOOLEAN_PERCENT,
    description="Check that percentage of texts parsable to boolean meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_BOOLEAN_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_INTEGER_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_INTEGER_PERCENT,
    description="Check that percentage of texts parsable to integer meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_INTEGER_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_FLOAT_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_FLOAT_PERCENT,
    description="Check that percentage of texts parsable to float meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_FLOAT_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

TEXT_PARSABLE_TO_DATE_PERCENT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.TEXT_PARSABLE_TO_DATE_PERCENT,
    description="Check that percentage of texts parsable to date meets minimum",
    category="datatype",
    sensor_type=SensorType.TEXT_PARSABLE_DATE_PERCENT,
    rule_type=RuleType.MIN_PERCENT,
    is_column_level=True,
    default_params={"min_percent": 95.0},
)

DETECTED_DATATYPE_IN_TEXT_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT,
    description="Check that detected datatype matches expected",
    category="datatype",
    sensor_type=SensorType.DETECTED_DATATYPE,
    rule_type=RuleType.EQUAL_TO,
    is_column_level=True,
    default_params={"expected_value": 5},
)

DETECTED_DATATYPE_IN_TEXT_CHANGED_CHECK = DQOpsCheck(
    name=DQOpsCheckType.DETECTED_DATATYPE_IN_TEXT_CHANGED,
    description="Check that detected datatype has not changed",
    category="datatype",
    sensor_type=SensorType.DETECTED_DATATYPE_CHANGED,
    rule_type=RuleType.NOT_EQUAL_TO,
    is_column_level=True,
    default_params={"forbidden_value": 1},
)
