"""Tests for check_emitter — deterministic check selection.

Anchor test reproduces the worked example from
Deterministic_DQ_Whitepaper.pdf (customer_transactions table).
"""

from __future__ import annotations

import pytest

from dq_platform.profilers.check_emitter import (
    REASON_NO_AUTHORITATIVE_SOURCE,
    REASON_NO_VALIDITY_DECL,
    EmittedCheck,
    FieldDeclaration,
    FieldProfileAggregates,
    TableDeclaration,
    emit,
)
from dq_platform.profilers.inference_engine import (
    CodelistRef,
    InferenceResult,
    RegexCandidate,
)

# ─── Determinism ─────────────────────────────────────────────────────────────


class TestDeterminism:
    def test_same_inputs_same_output(self) -> None:
        fields = [
            FieldDeclaration(
                name="transaction_id",
                logical_type="string",
                primary_key=True,
                accepted_length=(9, 9),
            ),
            FieldDeclaration(
                name="total_amount",
                logical_type="number",
                accepted_range=(0.0, 100000.0),
            ),
        ]
        table_decl = TableDeclaration()
        profile = {"completeness": "99", "uniqueness": "100", "validity": "95"}
        f_profiles = {
            "transaction_id": FieldProfileAggregates(count=100, distinct=100),
            "total_amount": FieldProfileAggregates(count=100),
        }
        f_inferences: dict[str, InferenceResult] = {}

        a = emit(fields, table_decl, profile, f_profiles, f_inferences)
        b = emit(fields, table_decl, profile, f_profiles, f_inferences)
        assert _check_tuples(a.checks) == _check_tuples(b.checks)


# ─── PK + uniqueness ─────────────────────────────────────────────────────────


class TestPrimaryKey:
    def test_pk_emits_distinct_percent_and_nulls_count(self) -> None:
        fields = [FieldDeclaration(name="id", logical_type="string", primary_key=True)]
        profile = {"uniqueness": "100", "completeness": "99"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"id": FieldProfileAggregates()},
            {},
        )
        types = {c.check_type for c in out.checks}
        assert "distinct_percent" in types
        assert "nulls_count" in types


# ─── acceptedValues → text_found_in_set_percent ──────────────────────────────


class TestAcceptedValues:
    def test_emits_text_found_in_set(self) -> None:
        fields = [
            FieldDeclaration(
                name="status",
                logical_type="string",
                accepted_values=["Shipped", "Pending", "Cancelled"],
            )
        ]
        profile = {"validity": "95"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"status": FieldProfileAggregates()},
            {},
        )
        check = _find(out.checks, "text_found_in_set_percent", "status")
        assert check is not None
        assert check.dimension == "validity"
        assert check.source == "declaration"
        # expected_values is a SENSOR param — lives in `parameters`,
        # not in the severity-keyed `rule_parameters`.
        assert check.parameters["expected_values"] == [
            "Shipped",
            "Pending",
            "Cancelled",
        ]


# ─── acceptedRange → number_in_range_percent ─────────────────────────────────


class TestAcceptedRange:
    def test_emits_number_in_range(self) -> None:
        # number_in_range_percent is a conformity check (range standards),
        # so the profile must promise conformity — not validity.
        fields = [
            FieldDeclaration(
                name="amount",
                logical_type="number",
                accepted_range=(0.0, 100000.0),
            )
        ]
        profile = {"conformity": "95"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"amount": FieldProfileAggregates()},
            {},
        )
        check = _find(out.checks, "number_in_range_percent", "amount")
        assert check is not None
        assert check.dimension == "conformity"
        # min_value / max_value are sensor params.
        assert check.parameters["min_value"] == 0.0
        assert check.parameters["max_value"] == 100000.0

    def test_number_in_range_not_emitted_when_only_validity_promised(self) -> None:
        # Guards the conformity routing: acceptedRange must NOT emit under a
        # validity-only promise, or the caller churns it as an orphan.
        fields = [FieldDeclaration(name="amount", logical_type="number", accepted_range=(0.0, 100.0))]
        out = emit(fields, TableDeclaration(), {"validity": "95"}, {"amount": FieldProfileAggregates()}, {})
        assert _find(out.checks, "number_in_range_percent", "amount") is None


# ─── format=country_code → text_valid_country_code_percent ───────────────────


class TestCountryCode:
    def test_declaration_emits_iso_check(self) -> None:
        fields = [FieldDeclaration(name="country", logical_type="string", format="country_code")]
        profile = {"accuracy": "95"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"country": FieldProfileAggregates()},
            {},
        )
        check = _find(out.checks, "text_valid_country_code_percent", "country")
        assert check is not None
        assert check.dimension == "accuracy"

    def test_inferred_codelist_emits_iso_check(self) -> None:
        fields = [FieldDeclaration(name="country", logical_type="string")]
        profile = {"accuracy": "95"}
        inferences = {
            "country": InferenceResult(codelist=CodelistRef(standard="ISO_3166_alpha2", version="2024", coverage=1.0))
        }
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"country": FieldProfileAggregates()},
            inferences,
        )
        check = _find(out.checks, "text_valid_country_code_percent", "country")
        assert check is not None
        assert check.source == "inference"


# ─── Completeness fallback ───────────────────────────────────────────────────


class TestCompletenessFallback:
    def test_emits_nulls_percent_per_col_when_promised(self) -> None:
        fields = [
            FieldDeclaration(name="a", logical_type="string"),
            FieldDeclaration(name="b", logical_type="number"),
        ]
        profile = {"completeness": "99"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"a": FieldProfileAggregates(), "b": FieldProfileAggregates()},
            {},
        )
        nulls_targets = {c.target_column for c in out.checks if c.check_type == "nulls_percent"}
        assert nulls_targets == {"a", "b"}

    def test_no_completeness_when_not_promised(self) -> None:
        fields = [FieldDeclaration(name="a", logical_type="string")]
        out = emit(
            fields,
            TableDeclaration(),
            {"uniqueness": "100"},
            {"a": FieldProfileAggregates()},
            {},
        )
        assert not any(c.check_type == "nulls_percent" for c in out.checks)


# ─── Table-level: freshness, row_count, consistentWith ───────────────────────


class TestTableLevel:
    def test_data_freshness_targets_the_freshness_column(self) -> None:
        # data_freshness is a column-level DQOps sensor — the freshness
        # column is the check's target_column, not a sensor parameter.
        td = TableDeclaration(freshness_column="updated_at")
        profile = {"timeliness": "24h"}
        out = emit([], td, profile, {}, {})
        check = _find(out.checks, "data_freshness", "updated_at")
        assert check is not None
        assert check.dimension == "timeliness"

    def test_row_count_emitted_with_bounds(self) -> None:
        td = TableDeclaration(expected_row_count=(1000, 1_000_000))
        profile = {"coverage": "99"}
        out = emit([], td, profile, {}, {})
        check = _find(out.checks, "row_count", None)
        assert check is not None

    def test_consistent_with_emitted_for_accuracy(self) -> None:
        td = TableDeclaration(consistent_with=[{"product_id": "p2", "keys": ["id"]}])
        profile = {"accuracy": "95"}
        out = emit([], td, profile, {}, {})
        check = _find(out.checks, "total_row_count_match_percent", None)
        assert check is not None


# ─── Structural defaults gated by promised dims ───────────────────────────────


class TestStructuralDefaults:
    def test_structural_checks_emitted_when_dims_promised(self) -> None:
        # column_count → conformity, column_list_changed → consistency.
        # Each emits only when its dimension is promised — emitting on an
        # un-promised dim makes the caller treat it as an orphan and churn it.
        both = emit([], TableDeclaration(), {"conformity": "95", "consistency": "100"}, {}, {})
        types = {c.check_type for c in both.checks}
        assert "column_count" in types
        assert "column_list_changed" in types

    def test_no_structural_checks_when_dims_not_promised(self) -> None:
        out = emit([], TableDeclaration(), {"completeness": "99"}, {}, {})
        types = {c.check_type for c in out.checks}
        assert "column_count" not in types
        assert "column_list_changed" not in types

    def test_column_list_changed_only_when_consistency_promised(self) -> None:
        out = emit([], TableDeclaration(), {"consistency": "100"}, {}, {})
        types = {c.check_type for c in out.checks}
        assert "column_list_changed" in types
        assert "column_count" not in types  # conformity not promised


# ─── not_assessed_reasons ────────────────────────────────────────────────────


class TestNotAssessedReasons:
    def test_accuracy_promised_no_decl_yields_reason(self) -> None:
        profile = {"accuracy": "95"}
        out = emit([], TableDeclaration(), profile, {}, {})
        assert out.not_assessed_reasons.get("accuracy") == REASON_NO_AUTHORITATIVE_SOURCE

    def test_validity_promised_no_decl_yields_reason(self) -> None:
        profile = {"validity": "95"}
        out = emit(
            [FieldDeclaration(name="x", logical_type="string")],
            TableDeclaration(),
            profile,
            {"x": FieldProfileAggregates()},
            {},
        )
        assert out.not_assessed_reasons.get("validity") == REASON_NO_VALIDITY_DECL


# ─── Worked example from whitepaper ──────────────────────────────────────────


class TestWorkedExample:
    """Reproduce the customer_transactions example from the whitepaper.

    Promised dims: completeness 99, uniqueness 100, conformity 95,
                   validity 95, timeliness 95, consistency 100.
    Fields: 8 cols with 3 validity declarations + PK + freshness col.

    The declaration-driven checks below are asserted individually; the
    conformity promise also fans out the default per-column guards
    (dirty-text, column_exists, future-date), so the total count is well
    above the declared-only set. This test pins the declared checks and the
    promised-dim invariant, not an exact total.
    """

    def test_full_emit(self) -> None:
        fields = [
            FieldDeclaration(
                name="transaction_id",
                logical_type="string",
                primary_key=True,
                accepted_length=(9, 9),
                examples=["TXN001234", "TXN789012"],
            ),
            FieldDeclaration(name="customer_id", logical_type="string"),
            FieldDeclaration(name="country_code", logical_type="string"),
            FieldDeclaration(
                name="order_status",
                logical_type="string",
                accepted_values=["Shipped", "Pending", "Cancelled", "Returned"],
            ),
            FieldDeclaration(
                name="total_amount",
                logical_type="number",
                accepted_range=(0.0, 100000.0),
            ),
            FieldDeclaration(
                name="quantity",
                logical_type="number",
                accepted_range=(1.0, 1000.0),
            ),
            FieldDeclaration(name="transaction_date", logical_type="date"),
            FieldDeclaration(name="updated_at", logical_type="date"),
        ]
        table_decl = TableDeclaration(freshness_column="updated_at")
        profile = {
            "completeness": "99",
            "uniqueness": "100",
            "conformity": "95",
            "validity": "95",
            "timeliness": "95",
            "consistency": "100",
        }

        # Inferences supplied for cols the worked example expects to fire on:
        # country_code → ISO 3166 codelist (accuracy is NOT promised here per
        # whitepaper page 2, so this won't emit). transaction_id regex.
        inferences = {
            "transaction_id": InferenceResult(regex=RegexCandidate(pattern=r"^TXN\d{6}$", coverage=1.0)),
            "country_code": InferenceResult(
                codelist=CodelistRef(standard="ISO_3166_alpha2", version="2024", coverage=1.0)
            ),
        }

        f_profiles = {f.name: FieldProfileAggregates(count=100, distinct=100) for f in fields}

        out = emit(fields, table_decl, profile, f_profiles, inferences)
        types = [(c.check_type, c.target_column) for c in out.checks]

        # Completeness: nulls_percent per col + nulls_count on PK
        nulls_percent_targets = {c for t, c in types if t == "nulls_percent"}
        assert nulls_percent_targets >= {f.name for f in fields if f.name != "transaction_id"} or (
            len(nulls_percent_targets) >= 7
        ), f"completeness fallback should cover most fields, got {nulls_percent_targets}"

        # PK uniqueness
        assert ("distinct_percent", "transaction_id") in types
        assert ("nulls_count", "transaction_id") in types

        # Validity declaration — acceptedValues enum
        assert ("text_found_in_set_percent", "order_status") in types

        # Conformity declarations — range + length standards, plus inferred regex
        assert ("number_in_range_percent", "total_amount") in types
        assert ("number_in_range_percent", "quantity") in types
        assert ("text_length_in_range_percent", "transaction_id") in types
        assert ("text_matching_regex_percent", "transaction_id") in types
        for ct, col in (
            ("number_in_range_percent", "total_amount"),
            ("text_length_in_range_percent", "transaction_id"),
        ):
            c = _find(out.checks, ct, col)
            assert c is not None and c.dimension == "conformity"

        # Timeliness from freshness column — targets the column itself.
        assert ("data_freshness", "updated_at") in types

        # Structural checks — conformity + consistency both promised here
        assert ("column_count", None) in types
        assert ("column_list_changed", None) in types

        # accuracy NOT promised in this profile → no accuracy checks
        assert all(c.dimension != "accuracy" for c in out.checks)

        # Core invariant: nothing emitted outside the promised dim set.
        assert {c.dimension for c in out.checks} <= set(profile)


# ─── Emitter invariant ───────────────────────────────────────────────────────


class TestPromisedDimInvariant:
    """The emitter must never produce a check whose dimension is not promised.

    The caller (dq-auto-run) deletes any check on an un-promised dimension as
    an orphan, so an emit that violates this invariant causes an infinite
    create/delete churn across runs. This test exercises every emit path
    (field decls, inference, table decls, structural defaults) under each
    single-dimension profile and asserts the subset relation holds.
    """

    @pytest.mark.parametrize(
        "dim",
        ["completeness", "uniqueness", "conformity", "validity", "timeliness", "consistency", "coverage", "accuracy"],
    )
    def test_no_check_outside_promised_set(self, dim: str) -> None:
        fields = [
            FieldDeclaration(name="id", logical_type="number", primary_key=True),
            FieldDeclaration(name="status", logical_type="string", accepted_values=["A", "B"]),
            FieldDeclaration(name="amount", logical_type="number", accepted_range=(0.0, 100.0)),
            FieldDeclaration(name="country", logical_type="string"),
        ]
        f_profiles = {f.name: FieldProfileAggregates(count=100, distinct=100) for f in fields}
        inferences = {
            "id": InferenceResult(regex=RegexCandidate(pattern=r"^\d+$", coverage=1.0)),
            "country": InferenceResult(codelist=CodelistRef(standard="ISO_3166_alpha2", version="2024", coverage=1.0)),
        }
        table_decl = TableDeclaration(
            freshness_column="updated_at",
            expected_row_count=(10, 1000),
            consistent_with=[{"product_id": "x", "keys": ["id"]}],
        )
        out = emit(fields, table_decl, {dim: "95"}, f_profiles, inferences)
        emitted_dims = {c.dimension for c in out.checks}
        assert emitted_dims <= {dim}, f"emit({dim}) leaked dims: {emitted_dims - {dim}}"


# ─── Dirty-text detectors ────────────────────────────────────────────────────


class TestDirtyText:
    _DIRTY = {
        "empty_text_percent",
        "whitespace_text_percent",
        "null_placeholder_text_percent",
        "text_surrounded_by_whitespace_percent",
    }

    def test_emits_all_four_on_text_column_under_conformity(self) -> None:
        fields = [FieldDeclaration(name="note", logical_type="string")]
        out = emit(fields, TableDeclaration(), {"conformity": "95"}, {"note": FieldProfileAggregates()}, {})
        emitted = {c.check_type for c in out.checks if c.target_column == "note"}
        assert self._DIRTY <= emitted
        assert all(c.dimension == "conformity" for c in out.checks if c.check_type in self._DIRTY)

    def test_not_emitted_on_numeric_column(self) -> None:
        fields = [FieldDeclaration(name="amount", logical_type="number")]
        out = emit(fields, TableDeclaration(), {"conformity": "95"}, {"amount": FieldProfileAggregates()}, {})
        assert not (self._DIRTY & {c.check_type for c in out.checks})

    def test_not_emitted_when_conformity_unpromised(self) -> None:
        fields = [FieldDeclaration(name="note", logical_type="string")]
        out = emit(fields, TableDeclaration(), {"completeness": "99"}, {"note": FieldProfileAggregates()}, {})
        assert not (self._DIRTY & {c.check_type for c in out.checks})

    def test_null_placeholder_carries_word_list(self) -> None:
        fields = [FieldDeclaration(name="note", logical_type="string")]
        out = emit(fields, TableDeclaration(), {"conformity": "95"}, {"note": FieldProfileAggregates()}, {})
        check = _find(out.checks, "null_placeholder_text_percent", "note")
        assert check is not None
        assert check.parameters["placeholders"] == ["NULL", "N/A", "NA", "NONE"]


# ─── Date columns: range + future-date ───────────────────────────────────────


class TestDateChecks:
    def test_accepted_range_on_date_emits_date_in_range_not_number(self) -> None:
        fields = [
            FieldDeclaration(
                name="event_date",
                logical_type="date",
                accepted_range=("2020-01-01", "2025-12-31"),  # type: ignore[arg-type]
            )
        ]
        out = emit(fields, TableDeclaration(), {"conformity": "95"}, {"event_date": FieldProfileAggregates()}, {})
        check = _find(out.checks, "date_in_range_percent", "event_date")
        assert check is not None
        assert check.parameters == {"min_date": "2020-01-01", "max_date": "2025-12-31"}
        # A date column must never be routed to the numeric range check.
        assert _find(out.checks, "number_in_range_percent", "event_date") is None

    def test_inferred_date_range_emits_with_inference_source(self) -> None:
        from dq_platform.profilers.inference_engine import DateRange

        fields = [FieldDeclaration(name="event_date", logical_type="timestamp")]
        inferences = {"event_date": InferenceResult(date_range=DateRange(min="2021-03-01", max="2024-09-09"))}
        out = emit(
            fields, TableDeclaration(), {"conformity": "95"}, {"event_date": FieldProfileAggregates()}, inferences
        )
        check = _find(out.checks, "date_in_range_percent", "event_date")
        assert check is not None
        assert check.source == "inference"

    def test_future_date_guard_emitted_for_date_column(self) -> None:
        fields = [FieldDeclaration(name="event_date", logical_type="date")]
        out = emit(fields, TableDeclaration(), {"conformity": "95"}, {"event_date": FieldProfileAggregates()}, {})
        assert _find(out.checks, "date_values_in_future_percent", "event_date") is not None


# ─── New table-level + per-column structural checks ──────────────────────────


class TestExpandedStructural:
    def test_table_availability_emitted_under_coverage(self) -> None:
        out = emit([], TableDeclaration(), {"coverage": "99"}, {}, {})
        check = _find(out.checks, "table_availability", None)
        assert check is not None
        assert check.dimension == "coverage"

    def test_column_types_changed_companion_to_column_list_changed(self) -> None:
        out = emit([], TableDeclaration(), {"consistency": "100"}, {}, {})
        types = {c.check_type for c in out.checks}
        assert {"column_list_changed", "column_types_changed"} <= types

    def test_duplicate_record_percent_when_no_primary_key(self) -> None:
        out = emit(
            [FieldDeclaration(name="a", logical_type="string")],
            TableDeclaration(),
            {"uniqueness": "100"},
            {"a": FieldProfileAggregates()},
            {},
        )
        assert _find(out.checks, "duplicate_record_percent", None) is not None

    def test_duplicate_record_percent_suppressed_when_primary_key_declared(self) -> None:
        out = emit(
            [FieldDeclaration(name="id", logical_type="number", primary_key=True)],
            TableDeclaration(),
            {"uniqueness": "100"},
            {"id": FieldProfileAggregates()},
            {},
        )
        assert _find(out.checks, "duplicate_record_percent", None) is None

    def test_column_exists_emitted_per_field_under_conformity(self) -> None:
        fields = [
            FieldDeclaration(name="a", logical_type="string"),
            FieldDeclaration(name="b", logical_type="number"),
        ]
        out = emit(
            fields,
            TableDeclaration(),
            {"conformity": "95"},
            {"a": FieldProfileAggregates(), "b": FieldProfileAggregates()},
            {},
        )
        targets = {c.target_column for c in out.checks if c.check_type == "column_exists"}
        assert targets == {"a", "b"}


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _check_tuples(checks: list[EmittedCheck]) -> set[tuple]:
    return {(c.check_type, c.target_column, c.dimension, c.source) for c in checks}


def _find(checks: list[EmittedCheck], check_type: str, target_column: str | None) -> EmittedCheck | None:
    for c in checks:
        if c.check_type == check_type and c.target_column == target_column:
            return c
    return None
