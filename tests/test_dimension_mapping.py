"""Tests for ODPS dimension mapping — overrides, exclusions, category defaults.

Asserts the re-aligned mapping where:
- numeric/statistical bound checks → conformity (range standard), not accuracy
- referential checks → accuracy (veracity vs source-of-truth)
- ISO codelist checks → accuracy
- cross-source match checks → accuracy
- column_count / column_exists (schema shape) → conformity
- schema_*_changed (drift) → consistency
- anomaly checks → None (advisory, not scored)
"""

from __future__ import annotations

import pytest

from dq_platform.odps.dimension_mapping import (
    ANOMALY_EXCLUDED,
    CATEGORY_TO_DIMENSION,
    CHECK_TYPE_OVERRIDE,
    ODPSDimension,
    get_dimension_for_check_type,
    is_anomaly_check,
)

# ─── ODPS-aligned category defaults ──────────────────────────────────────────


class TestCategoryDefaults:
    def test_nulls_maps_to_completeness(self) -> None:
        assert CATEGORY_TO_DIMENSION["nulls"] == ODPSDimension.COMPLETENESS

    def test_uniqueness_maps_to_uniqueness(self) -> None:
        assert CATEGORY_TO_DIMENSION["uniqueness"] == ODPSDimension.UNIQUENESS

    def test_volume_maps_to_coverage(self) -> None:
        assert CATEGORY_TO_DIMENSION["volume"] == ODPSDimension.COVERAGE

    def test_numeric_maps_to_conformity_not_accuracy(self) -> None:
        # ODPS def: accuracy = veracity to source. Numeric range is conformity.
        assert CATEGORY_TO_DIMENSION["numeric"] == ODPSDimension.CONFORMITY

    def test_statistical_maps_to_conformity_not_accuracy(self) -> None:
        assert CATEGORY_TO_DIMENSION["statistical"] == ODPSDimension.CONFORMITY

    def test_referential_maps_to_accuracy_not_consistency(self) -> None:
        # FK match = veracity vs authoritative source = accuracy.
        assert CATEGORY_TO_DIMENSION["referential"] == ODPSDimension.ACCURACY

    def test_datatype_maps_to_conformity_not_validity(self) -> None:
        assert CATEGORY_TO_DIMENSION["datatype"] == ODPSDimension.CONFORMITY

    def test_datetime_maps_to_conformity_not_validity(self) -> None:
        assert CATEGORY_TO_DIMENSION["datetime"] == ODPSDimension.CONFORMITY

    def test_accepted_values_default_is_validity(self) -> None:
        # Default for business enums; ISO standards split via override.
        assert CATEGORY_TO_DIMENSION["accepted_values"] == ODPSDimension.VALIDITY

    def test_anomaly_category_absent_from_default_map(self) -> None:
        # Anomaly intentionally unmapped; resolution returns None.
        assert "anomaly" not in CATEGORY_TO_DIMENSION


# ─── Per-check overrides ─────────────────────────────────────────────────────


class TestCheckTypeOverride:
    @pytest.mark.parametrize(
        "check_type",
        [
            "text_valid_country_code_percent",  # ISO 3166
            "text_valid_currency_code_percent",  # ISO 4217
            "foreign_key_not_found",
            "foreign_key_found_percent",
            "total_row_count_match_percent",
            "total_sum_match_percent",
            "row_count_match",
            "sum_match",
            "mean_match",
            "distinct_count_match",
        ],
    )
    def test_check_resolves_to_accuracy(self, check_type: str) -> None:
        assert get_dimension_for_check_type(check_type) == ODPSDimension.ACCURACY

    @pytest.mark.parametrize(
        "check_type",
        ["column_count", "column_exists"],
    )
    def test_schema_shape_resolves_to_conformity(self, check_type: str) -> None:
        # Shape checks belong to conformity; drift checks belong to consistency.
        assert get_dimension_for_check_type(check_type) == ODPSDimension.CONFORMITY

    def test_override_beats_category(self) -> None:
        # text_valid_country_code_percent is in `accepted_values` category
        # (would default to VALIDITY) but the override pins it to ACCURACY.
        assert get_dimension_for_check_type("text_valid_country_code_percent") == ODPSDimension.ACCURACY


# ─── Anomaly exclusion ───────────────────────────────────────────────────────


class TestAnomalyExclusion:
    @pytest.mark.parametrize(
        "check_type",
        sorted(ANOMALY_EXCLUDED),
    )
    def test_anomaly_check_returns_none(self, check_type: str) -> None:
        assert get_dimension_for_check_type(check_type) is None

    @pytest.mark.parametrize(
        "check_type",
        sorted(ANOMALY_EXCLUDED),
    )
    def test_is_anomaly_check_true(self, check_type: str) -> None:
        assert is_anomaly_check(check_type) is True

    def test_non_anomaly_check_is_not_anomaly(self) -> None:
        assert is_anomaly_check("nulls_percent") is False


# ─── Unknown check types ─────────────────────────────────────────────────────


class TestUnknownCheckTypes:
    def test_unknown_check_returns_none(self) -> None:
        assert get_dimension_for_check_type("totally_made_up_check") is None

    def test_empty_string_returns_none(self) -> None:
        assert get_dimension_for_check_type("") is None


# ─── Registry sanity ─────────────────────────────────────────────────────────


class TestRegistrySanity:
    def test_all_overrides_have_valid_dim(self) -> None:
        for check_type, dim in CHECK_TYPE_OVERRIDE.items():
            assert isinstance(dim, ODPSDimension), f"{check_type} maps to non-ODPSDimension {dim}"

    def test_no_override_collides_with_anomaly_exclusion(self) -> None:
        # A check type can't be both excluded AND remapped — exclusion wins
        # but the dict overlap would be ambiguous intent.
        overlap = set(CHECK_TYPE_OVERRIDE) & ANOMALY_EXCLUDED
        assert not overlap, f"check types in both override and excluded: {overlap}"

    def test_all_categories_map_to_valid_dim(self) -> None:
        for cat, dim in CATEGORY_TO_DIMENSION.items():
            assert isinstance(dim, ODPSDimension), f"category {cat} maps to non-ODPSDimension {dim}"
