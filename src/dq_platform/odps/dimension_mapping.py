"""Static mapping between DQ Platform check categories and ODPS 4.1 quality dimensions.

ODPS 4.1 defines 8 standardized data quality dimensions. This module maps
each of the DQ Platform's 22 check categories to the appropriate ODPS dimension.

Mapping is layered for correctness:

1. CHECK_TYPE_OVERRIDE — per-check-type mapping, takes precedence when a
   category alone is ambiguous (e.g. `accepted_values` splits into accuracy
   for ISO codelists vs validity for business enums; `comparison` splits
   into accuracy for cross-source matches vs consistency for intra-source).
2. CATEGORY_TO_DIMENSION — default mapping when no override applies.
3. FALLBACK_CATEGORY_MAP — legacy alias resolution for checks not in
   DQOpsCheckType enum.
4. ANOMALY_EXCLUDED — check types that fire as advisory monitors but do
   NOT contribute to dim score aggregation (anomaly/drift detection is
   stability monitoring, not promise verification).

The category defaults were re-aligned to ODPS verbatim definitions:
  - numeric/statistical → conformity (range = "format/range standards", ODPS def)
  - referential       → accuracy (foreign-key = veracity vs source-of-truth)
  - datatype/datetime → conformity (type/format match)
  - accepted_values   → validity by default; ISO codelists override to accuracy
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
#
# This is the *default* per-category fallback. CHECK_TYPE_OVERRIDE below
# takes precedence when a specific check_type maps differently.
CATEGORY_TO_DIMENSION: dict[str, ODPSDimension] = {
    # completeness — populated, not null (ODPS def)
    "nulls": ODPSDimension.COMPLETENESS,
    # coverage — all expected records present (ODPS def)
    "volume": ODPSDimension.COVERAGE,
    "availability": ODPSDimension.COVERAGE,
    # uniqueness — no duplicates (ODPS def)
    "uniqueness": ODPSDimension.UNIQUENESS,
    # conformity — format / syntax / type / range standards (ODPS def)
    # numeric & statistical bound checks ARE range conformity, not accuracy:
    # accuracy = veracity vs authoritative source, which requires referential
    # comparison (handled per-check via CHECK_TYPE_OVERRIDE).
    "numeric": ODPSDimension.CONFORMITY,
    "statistical": ODPSDimension.CONFORMITY,
    "text": ODPSDimension.CONFORMITY,
    "patterns": ODPSDimension.CONFORMITY,
    "pii": ODPSDimension.CONFORMITY,
    "datatype": ODPSDimension.CONFORMITY,
    "datetime": ODPSDimension.CONFORMITY,
    # validity — real-world representation (ODPS def)
    # acceptedValues for business enums = validity; ISO codelists split
    # to accuracy via CHECK_TYPE_OVERRIDE.
    "accepted_values": ODPSDimension.VALIDITY,
    "boolean": ODPSDimension.VALIDITY,
    "geographic": ODPSDimension.VALIDITY,
    # timeliness — current / on-time (ODPS def)
    "timeliness": ODPSDimension.TIMELINESS,
    "change": ODPSDimension.TIMELINESS,
    "change_detection": ODPSDimension.TIMELINESS,
    # consistency — retain across stores (ODPS def)
    # schema drift = consistency; static schema shape (column_count etc.)
    # = conformity, handled via CHECK_TYPE_OVERRIDE.
    "schema": ODPSDimension.CONSISTENCY,
    "comparison": ODPSDimension.CONSISTENCY,
    # accuracy — veracity vs authoritative source (ODPS def)
    # foreign-key matches against a source-of-truth table.
    "referential": ODPSDimension.ACCURACY,
    # anomaly — drift/stability monitoring, NOT a promise dimension.
    # Intentionally omitted: get_dimension_for_category returns None for
    # unmapped categories, so anomaly checks never aggregate into a dim
    # score. See ANOMALY_EXCLUDED for the per-check-type enforcement.
}


# Per-check-type overrides — take precedence over CATEGORY_TO_DIMENSION.
#
# Use when a single category contains checks that must score against
# different dims. Example: `accepted_values` contains both ISO codelist
# validators (= accuracy) and free-form enum membership (= validity).
CHECK_TYPE_OVERRIDE: dict[str, ODPSDimension] = {
    # accepted_values split: ISO standards = accuracy, business enums = validity
    "text_valid_country_code_percent": ODPSDimension.ACCURACY,  # ISO 3166
    "text_valid_currency_code_percent": ODPSDimension.ACCURACY,  # ISO 4217
    # Referential integrity (foreign key) — accuracy by ODPS def
    "foreign_key_not_found": ODPSDimension.ACCURACY,
    "foreign_key_found_percent": ODPSDimension.ACCURACY,
    # Cross-source comparison — accuracy (veracity vs other source)
    "total_row_count_match_percent": ODPSDimension.ACCURACY,
    "total_sum_match_percent": ODPSDimension.ACCURACY,
    "total_min_match_percent": ODPSDimension.ACCURACY,
    "total_max_match_percent": ODPSDimension.ACCURACY,
    "total_average_match_percent": ODPSDimension.ACCURACY,
    "total_not_null_count_match_percent": ODPSDimension.ACCURACY,
    "row_count_match": ODPSDimension.ACCURACY,
    "column_count_match": ODPSDimension.ACCURACY,
    "sum_match": ODPSDimension.ACCURACY,
    "min_match": ODPSDimension.ACCURACY,
    "max_match": ODPSDimension.ACCURACY,
    "mean_match": ODPSDimension.ACCURACY,
    "not_null_count_match": ODPSDimension.ACCURACY,
    "null_count_match": ODPSDimension.ACCURACY,
    "distinct_count_match": ODPSDimension.ACCURACY,
    # Schema shape vs drift — shape is conformity, drift is consistency.
    # Default category mapping puts everything under consistency; override
    # the static-shape checks back to conformity.
    "column_count": ODPSDimension.CONFORMITY,
    "column_exists": ODPSDimension.CONFORMITY,
}


# Anomaly / drift check types — fire as advisory monitors but do NOT
# contribute to ODPS dim scoring. Stability ≠ promise verification.
ANOMALY_EXCLUDED: frozenset[str] = frozenset(
    {
        "row_count_anomaly",
        "data_freshness_anomaly",
        "nulls_percent_anomaly",
        "distinct_count_anomaly",
        "distinct_percent_anomaly",
        "sum_anomaly",
        "mean_anomaly",
        "median_anomaly",
        "min_anomaly",
        "max_anomaly",
    }
)

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

    Returns None for unmapped categories (e.g. custom_sql, anomaly).
    """
    return CATEGORY_TO_DIMENSION.get(category)


def get_dimension_for_check_type(check_type: str) -> ODPSDimension | None:
    """Get the ODPS dimension for a specific check type.

    Resolution order:
      1. ANOMALY_EXCLUDED → return None (advisory, never scored)
      2. CHECK_TYPE_OVERRIDE → take override verbatim
      3. CHECK_REGISTRY → resolve category, look up CATEGORY_TO_DIMENSION
      4. FALLBACK_CATEGORY_MAP → for legacy aliases not in DQOpsCheckType
    """
    if check_type in ANOMALY_EXCLUDED:
        return None
    if check_type in CHECK_TYPE_OVERRIDE:
        return CHECK_TYPE_OVERRIDE[check_type]
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


def is_anomaly_check(check_type: str) -> bool:
    """Whether a check type is advisory-only (excluded from dim scoring)."""
    return check_type in ANOMALY_EXCLUDED


def get_all_check_types_for_dimension(
    dimension: ODPSDimension,
) -> list[DQOpsCheckType]:
    """Get all check types that map to a given ODPS dimension.

    Honors CHECK_TYPE_OVERRIDE and ANOMALY_EXCLUDED in addition to the
    category default mapping.
    """
    out: list[DQOpsCheckType] = []
    for ct, check_def in CHECK_REGISTRY.items():
        if ct.value in ANOMALY_EXCLUDED:
            continue
        resolved = CHECK_TYPE_OVERRIDE.get(ct.value) or CATEGORY_TO_DIMENSION.get(check_def.category)
        if resolved == dimension:
            out.append(ct)
    return out
