"""Static mapping between DQ Platform check categories and ODPS 4.1 quality dimensions.

ODPS 4.1 defines 8 standardized data quality dimensions. This module maps
each of the DQ Platform's 22 check categories to the appropriate ODPS dimension.
"""

from enum import Enum

from dq_platform.checks.dqops_checks import CHECK_REGISTRY, DQOpsCheckType


class ODPSDimension(str, Enum):
    """ODPS 4.1 standardized data quality dimensions."""

    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CONFORMITY = "conformity"
    CONSISTENCY = "consistency"
    COVERAGE = "coverage"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


# Maps each DQ Platform check category to an ODPS dimension.
# custom_sql is excluded — those checks require explicit user assignment.
CATEGORY_TO_DIMENSION: dict[str, ODPSDimension] = {
    # completeness — data population checks
    "nulls": ODPSDimension.COMPLETENESS,
    # coverage — expected data presence
    "volume": ODPSDimension.COVERAGE,
    "availability": ODPSDimension.COVERAGE,
    # uniqueness — duplicate detection
    "uniqueness": ODPSDimension.UNIQUENESS,
    # accuracy — value correctness
    "numeric": ODPSDimension.ACCURACY,
    "statistical": ODPSDimension.ACCURACY,
    "anomaly": ODPSDimension.ACCURACY,
    # conformity — format and standard compliance
    "text": ODPSDimension.CONFORMITY,
    "patterns": ODPSDimension.CONFORMITY,
    "accepted_values": ODPSDimension.CONFORMITY,
    "pii": ODPSDimension.CONFORMITY,
    # timeliness — freshness and temporal monitoring
    "timeliness": ODPSDimension.TIMELINESS,
    "change": ODPSDimension.TIMELINESS,
    "change_detection": ODPSDimension.TIMELINESS,
    # consistency — structural and referential integrity
    "schema": ODPSDimension.CONSISTENCY,
    "referential": ODPSDimension.CONSISTENCY,
    "comparison": ODPSDimension.CONSISTENCY,
    # validity — real-world representation accuracy
    "boolean": ODPSDimension.VALIDITY,
    "datetime": ODPSDimension.VALIDITY,
    "geographic": ODPSDimension.VALIDITY,
    "datatype": ODPSDimension.VALIDITY,
}

# Fallback mapping for CheckType values not registered in DQOpsCheckType/CHECK_REGISTRY.
# These are simpler check types that don't have full sensor+rule definitions but still
# need to contribute to dimension scoring.
FALLBACK_CATEGORY_MAP: dict[str, str] = {
    # Volume/Coverage
    "row_count_min": "volume",
    "row_count_max": "volume",
    "row_count_exact": "volume",
    "row_count_compare": "comparison",
    # Schema/Consistency
    "schema_column_count": "schema",
    "schema_column_exists": "schema",
    "schema_column_list": "schema",
    "schema_column_order": "schema",
    # Completeness/Nulls
    "null_count": "nulls",
    "null_percent": "nulls",
    "not_null": "nulls",
    "completeness_percent": "nulls",
    # Uniqueness
    "unique": "uniqueness",
    "uniqueness_percent": "uniqueness",
    "distinct_values_in_set": "accepted_values",
    "composite_key_unique": "uniqueness",
    "multicolumn_unique": "uniqueness",
    "most_common_value": "uniqueness",
    # Numeric/Accuracy
    "value_range": "numeric",
    "column_min": "numeric",
    "column_max": "numeric",
    "column_mean": "numeric",
    "column_median": "numeric",
    "column_sum": "numeric",
    "column_quantile": "numeric",
    "column_stddev": "statistical",
    # Text/Conformity
    "text_length_range": "text",
    "text_length_exact": "text",
    "like_pattern": "patterns",
    # Patterns/Conformity
    "regex_pattern": "patterns",
    "regex_not_match": "patterns",
    # Accepted values/Conformity
    "allowed_values": "accepted_values",
    "forbidden_values": "accepted_values",
    # Datatype/Validity
    "column_type": "datatype",
    "date_parseable": "datatype",
    "json_parseable": "datatype",
    "datetime_format": "datetime",
    # Comparison/Consistency
    "column_pair_equal": "comparison",
    "column_pair_comparison": "comparison",
    # Ordering/Timeliness
    "values_increasing": "change_detection",
    "values_decreasing": "change_detection",
}

# Severity weights for scoring. Higher weight = greater penalty.
SEVERITY_WEIGHTS: dict[str, float] = {
    "passed": 0.0,
    "warning": 1.0,
    "error": 2.5,
    "fatal": 5.0,
}

ALL_DIMENSIONS: list[ODPSDimension] = list(ODPSDimension)


def get_dimension_for_category(category: str) -> ODPSDimension | None:
    """Get the ODPS dimension for a check category.

    Returns None for unmapped categories (e.g. custom_sql).
    """
    return CATEGORY_TO_DIMENSION.get(category)


def get_dimension_for_check_type(check_type: str) -> ODPSDimension | None:
    """Get the ODPS dimension for a specific check type.

    Looks up the check in CHECK_REGISTRY to find its category,
    then maps that category to an ODPS dimension.
    """
    try:
        ct = DQOpsCheckType(check_type)
        check_def = CHECK_REGISTRY.get(ct)
        if check_def:
            return CATEGORY_TO_DIMENSION.get(check_def.category)
    except ValueError:
        pass
    # Fallback for CheckType values not in DQOpsCheckType
    category = FALLBACK_CATEGORY_MAP.get(check_type)
    return CATEGORY_TO_DIMENSION.get(category) if category else None


def get_all_check_types_for_dimension(
    dimension: ODPSDimension,
) -> list[DQOpsCheckType]:
    """Get all check types that map to a given ODPS dimension.

    Returns a list of DQOpsCheckType enum values.
    """
    # Find categories for this dimension
    categories = {cat for cat, dim in CATEGORY_TO_DIMENSION.items() if dim == dimension}

    # Find all check types in those categories
    return [ct for ct, check_def in CHECK_REGISTRY.items() if check_def.category in categories]
