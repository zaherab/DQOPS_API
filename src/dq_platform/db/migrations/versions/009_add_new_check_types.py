"""Add new DQOps check types for phases 1-9.

Revision ID: 009_add_new_check_types
Revises: 008_add_result_severity_column
Create Date: 2026-02-04

Adds 82 new check types:
- Phase 1: Whitespace & Text checks (10)
- Phase 2: Geographic & Numeric percent variants (8)
- Phase 3: Statistical & Percentile checks (10)
- Phase 4: Accepted Values & Domain checks (7)
- Phase 5: Date Pattern & Data Type detection (10)
- Phase 6: PII Detection checks (5)
- Phase 7: Change Detection checks (21)
- Phase 8: Cross-Table Comparison (6)
- Phase 9: Table-Level Misc (5)

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "009_add_new_check_types"
down_revision: str | None = "008_add_result_severity_column"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# All 82 new check types to add
NEW_CHECK_TYPES = [
    # Phase 1: Whitespace & Text checks
    "empty_text_percent",
    "whitespace_text_percent",
    "null_placeholder_text_found",
    "null_placeholder_text_percent",
    "text_surrounded_by_whitespace_found",
    "text_surrounded_by_whitespace_percent",
    "texts_not_matching_regex_percent",
    "text_matching_regex_percent",
    "min_word_count",
    "max_word_count",
    # Phase 2: Geographic & Numeric percent variants
    "valid_latitude_percent",
    "valid_longitude_percent",
    "number_below_min_value_percent",
    "number_above_max_value_percent",
    "negative_values",
    "negative_values_percent",
    "non_negative_values",
    "non_negative_values_percent",
    # Phase 3: Statistical & Percentile checks
    "integer_in_range_percent",
    "sample_stddev_in_range",
    "population_stddev_in_range",
    "sample_variance_in_range",
    "population_variance_in_range",
    "percentile_in_range",
    "percentile_10_in_range",
    "percentile_25_in_range",
    "percentile_75_in_range",
    "percentile_90_in_range",
    # Phase 4: Accepted Values & Domain checks
    "text_found_in_set_percent",
    "number_found_in_set_percent",
    "expected_text_values_in_use_count",
    "expected_numbers_in_use_count",
    "expected_texts_in_top_values_count",
    "text_valid_country_code_percent",
    "text_valid_currency_code_percent",
    # Phase 5: Date Pattern & Data Type detection
    "text_not_matching_date_pattern_found",
    "text_not_matching_date_pattern_percent",
    "text_match_date_format_percent",
    "text_not_matching_name_pattern_percent",
    "text_parsable_to_boolean_percent",
    "text_parsable_to_integer_percent",
    "text_parsable_to_float_percent",
    "text_parsable_to_date_percent",
    "detected_datatype_in_text",
    "detected_datatype_in_text_changed",
    # Phase 6: PII Detection checks
    "contains_usa_phone_percent",
    "contains_email_percent",
    "contains_usa_zipcode_percent",
    "contains_ip4_percent",
    "contains_ip6_percent",
    # Phase 7: Change Detection checks - Nulls
    "nulls_percent_change_1_day",
    "nulls_percent_change_7_days",
    "nulls_percent_change_30_days",
    # Phase 7: Change Detection checks - Uniqueness
    "distinct_count_change_1_day",
    "distinct_count_change_7_days",
    "distinct_count_change_30_days",
    "distinct_percent_change_1_day",
    "distinct_percent_change_7_days",
    "distinct_percent_change_30_days",
    # Phase 7: Change Detection checks - Statistics
    "mean_change_1_day",
    "mean_change_7_days",
    "mean_change_30_days",
    "median_change_1_day",
    "median_change_7_days",
    "median_change_30_days",
    "sum_change_1_day",
    "sum_change_7_days",
    "sum_change_30_days",
    # Phase 8: Cross-Table Comparison
    "total_row_count_match_percent",
    "total_sum_match_percent",
    "total_min_match_percent",
    "total_max_match_percent",
    "total_average_match_percent",
    "total_not_null_count_match_percent",
    # Phase 9: Table-Level Misc
    "data_ingestion_delay",
    "reload_lag",
    "sql_condition_passed_percent_on_table",
    "column_type_changed",
    # Additional missing types
    "nulls_count",
    "nulls_percent",
    "column_count",
    "column_exists",
]


def upgrade() -> None:
    """Add new check types to the enum."""
    for check_type in NEW_CHECK_TYPES:
        op.execute(f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
