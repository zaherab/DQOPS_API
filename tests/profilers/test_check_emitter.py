"""Tests for check_emitter — deterministic check selection.

Anchor test reproduces the worked example from
Deterministic_DQ_Whitepaper.pdf (customer_transactions table).
"""

from __future__ import annotations

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
        fields = [
            FieldDeclaration(
                name="amount",
                logical_type="number",
                accepted_range=(0.0, 100000.0),
            )
        ]
        profile = {"validity": "95"}
        out = emit(
            fields,
            TableDeclaration(),
            profile,
            {"amount": FieldProfileAggregates()},
            {},
        )
        check = _find(out.checks, "number_in_range_percent", "amount")
        assert check is not None
        # min_value / max_value are sensor params.
        assert check.parameters["min_value"] == 0.0
        assert check.parameters["max_value"] == 100000.0


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


# ─── Structural defaults always emitted ──────────────────────────────────────


class TestStructuralDefaults:
    def test_column_count_and_drift_always_emitted(self) -> None:
        out = emit([], TableDeclaration(), {"consistency": "100"}, {}, {})
        types = {c.check_type for c in out.checks}
        assert "column_count" in types
        assert "column_list_changed" in types


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
    Expected: ~19 emitted checks deterministically.
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

        # Validity declarations
        assert ("text_found_in_set_percent", "order_status") in types
        assert ("number_in_range_percent", "total_amount") in types
        assert ("number_in_range_percent", "quantity") in types
        assert ("text_length_in_range_percent", "transaction_id") in types

        # Conformity from inferred regex
        assert ("text_matching_regex_percent", "transaction_id") in types

        # Timeliness from freshness column — targets the column itself.
        assert ("data_freshness", "updated_at") in types

        # Structural always-on
        assert ("column_count", None) in types
        assert ("column_list_changed", None) in types

        # accuracy NOT promised in this profile → no accuracy checks
        assert all(c.dimension != "accuracy" for c in out.checks)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _check_tuples(checks: list[EmittedCheck]) -> set[tuple]:
    return {(c.check_type, c.target_column, c.dimension, c.source) for c in checks}


def _find(checks: list[EmittedCheck], check_type: str, target_column: str | None) -> EmittedCheck | None:
    for c in checks:
        if c.check_type == check_type and c.target_column == target_column:
            return c
    return None
