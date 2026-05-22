"""Deterministic check emitter.

Given:
  - contentSchema declarations (logicalType, examples, primaryKey, unique,
    format, pattern, acceptedValues, acceptedRange, acceptedLength,
    classification, freshnessColumn, expectedRowCount, consistentWith)
  - DQ profile (per-dim promise levels)
  - Table profile aggregates (count, nulls, distinct, min/max, len bounds)
  - Inference results (regex/codelist/enum/format/range candidates)

Emit a fully-calibrated set of DQOps check specs deterministically. Same
inputs → identical output across runs. No LLM. No network.

Rules table (priority order, evaluated per field):
  1. Declaration in contentSchema beats inference.
  2. primaryKey=True   → distinct_percent (target 100) + nulls_count=0
  3. unique=True       → distinct_percent (target from DQ profile)
  4. acceptedValues OR inferred enum → text_found_in_set_percent
  5. acceptedRange OR inferred numeric range → number_in_range_percent OR
                                                date_in_range_percent
  6. acceptedLength    → text_length_in_range_percent
  7. format/pattern OR inferred regex → text_matching_regex_percent
  8. format=country_code           → text_valid_country_code_percent
  9. format=currency_code          → text_valid_currency_code_percent
 10. completeness promised on any  → nulls_percent on every col
 11. classification=PII             → skip sample-dependent checks
 12. Table-level:
       freshnessColumn → data_freshness
       expectedRowCount → row_count (min_max_count)
       consistentWith → total_row_count_match_percent (cross-source)
       always: column_count + column_list_changed

After per-field/table emit, for each PROMISED dim with zero applicable
checks, record a not_assessed_reason code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dq_platform.odps.dimension_mapping import (
    get_dimension_for_check_type,
)
from dq_platform.profilers.inference_engine import InferenceResult
from dq_platform.profilers.threshold_engine import (
    promise_for_dimension,
    promised_dimensions_from_profile,
    thresholds_from_promise,
)

# ─── Inputs ──────────────────────────────────────────────────────────────────


@dataclass
class FieldDeclaration:
    """One field as declared in contentSchema."""

    name: str
    logical_type: str | None = None
    examples: list[Any] = field(default_factory=list)
    primary_key: bool = False
    unique: bool = False
    format: str | None = None
    pattern: str | None = None
    accepted_values: list[Any] | None = None
    accepted_range: tuple[float, float] | None = None
    accepted_length: tuple[int, int] | None = None
    classification: str | None = None  # "PII" / "PCI" / "PHI" / None


@dataclass
class TableDeclaration:
    """Table-level contentSchema declarations."""

    freshness_column: str | None = None
    expected_row_count: tuple[int, int] | None = None
    consistent_with: list[dict[str, Any]] | None = None  # [{"product_id":..,"keys":[..]}]


@dataclass
class FieldProfileAggregates:
    """Per-field aggregates returned by profile_runner."""

    count: int = 0
    nulls: int = 0
    distinct: int = 0
    min: Any = None
    max: Any = None
    min_len: int | None = None
    max_len: int | None = None


# ─── Outputs ─────────────────────────────────────────────────────────────────


@dataclass
class EmittedCheck:
    """One concrete DQOps check spec ready to register with DQOps.

    Two distinct param payloads — DQOps keeps them separate:
      parameters       sensor config (expected_values, regex_pattern,
                       min_value, max_value, min_length, max_length, ...).
      rule_parameters  severity thresholds, keyed warning/error/fatal.

    `dimension` is NOT passed by callers — it is derived from `check_type`
    in __post_init__ via get_dimension_for_check_type(), the single
    authoritative check_type → dimension map. Letting each emit site pass a
    literal allowed it to drift from that map; the caller (dq-auto-run)
    resolves a check's dimension the same canonical way for orphan detection,
    so any disagreement made it delete-and-recreate the check every run.
    """

    check_type: str
    target_column: str | None
    rule_parameters: dict[str, dict[str, Any]]
    source: str  # "declaration" | "inference" | "default"
    parameters: dict[str, Any] = field(default_factory=dict)
    dimension: str = field(init=False)  # derived from check_type

    def __post_init__(self) -> None:
        dim = get_dimension_for_check_type(self.check_type)
        if dim is None:
            raise ValueError(
                f"check type {self.check_type!r} has no ODPS dimension — it cannot be emitted as a scored check"
            )
        self.dimension = dim.value


@dataclass
class EmittedCheckSet:
    """Emit output for a single product."""

    checks: list[EmittedCheck] = field(default_factory=list)
    not_assessed_reasons: dict[str, str] = field(default_factory=dict)


# ─── Reason codes ────────────────────────────────────────────────────────────


REASON_NO_VALIDITY_DECL = "no_validity_decl"
REASON_NO_AUTHORITATIVE_SOURCE = "no_authoritative_source"
REASON_CROSS_FIELD_RULE_REQUIRED = "cross_field_rule_required"
REASON_NO_FRESHNESS_COLUMN = "no_freshness_column"
REASON_NO_EXPECTED_ROW_COUNT = "no_expected_row_count"
REASON_NO_CONSISTENT_WITH = "no_consistent_with"
REASON_PII_AGGREGATE_ONLY = "pii_aggregate_only"


# ─── Public API ──────────────────────────────────────────────────────────────


def emit(
    fields: list[FieldDeclaration],
    table_decl: TableDeclaration,
    dq_profile: dict[str, Any],
    field_profiles: dict[str, FieldProfileAggregates],
    field_inferences: dict[str, InferenceResult],
) -> EmittedCheckSet:
    """Emit the deterministic check set for one (product, table) pair.

    Args:
        fields: per-field contentSchema declarations.
        table_decl: table-level contentSchema declarations.
        dq_profile: DQ profile dict, e.g. {"completeness": "99", ...}.
        field_profiles: per-field aggregate stats from profile_runner.
        field_inferences: per-field InferenceResult from inference_engine.

    Returns:
        EmittedCheckSet containing checks + reason codes for any promised
        dim that ended up uncovered.
    """
    promised = set(promised_dimensions_from_profile(dq_profile))
    checks: list[EmittedCheck] = []
    covered: set[str] = set()
    pii_fields: set[str] = set()

    # ─ Per-field emission ─
    for field_decl in fields:
        if field_decl.classification == "PII":
            pii_fields.add(field_decl.name)

        f_profile = field_profiles.get(field_decl.name, FieldProfileAggregates())
        f_inference = field_inferences.get(field_decl.name, InferenceResult())

        field_checks = _emit_field_checks(field_decl, f_profile, f_inference, dq_profile, promised)
        for c in field_checks:
            checks.append(c)
            covered.add(c.dimension)

    # ─ Completeness: nulls_percent on every non-PK col when promised ─
    # PK fields already get nulls_count (zero-tolerance); non-PK cols get a
    # rate-based nulls_percent calibrated to the completeness promise.
    nulls_dim = _dim_for("nulls_percent")
    if nulls_dim in promised:
        completeness_promise = promise_for_dimension(dq_profile, nulls_dim)
        params = thresholds_from_promise("max_percent", _invert_completeness(completeness_promise))
        if params:
            for field_decl in fields:
                if field_decl.primary_key:
                    continue  # PK already covered by nulls_count
                checks.append(
                    EmittedCheck(
                        check_type="nulls_percent",
                        target_column=field_decl.name,
                        rule_parameters=params,
                        source="default",
                    )
                )
            covered.add(nulls_dim)

    # ─ Table-level emissions ─
    table_checks = _emit_table_checks(table_decl, dq_profile, promised)
    for c in table_checks:
        checks.append(c)
        covered.add(c.dimension)

    # Structural checks — emitted only when their dimension is promised.
    # column_count → conformity per override; column_list_changed → consistency.
    # Emitting them unconditionally would fight the caller's promised-dims-only
    # reconciliation: any check on an un-promised dim is treated as an orphan
    # and deleted, then re-emitted next run — an infinite create/delete churn.
    structural_dim = _dim_for("column_count")
    if structural_dim in promised:
        checks.append(
            EmittedCheck(
                check_type="column_count",
                target_column=None,
                rule_parameters={},  # sensor-baseline drift threshold
                source="default",
            )
        )
        covered.add(structural_dim)
    drift_dim = _dim_for("column_list_changed")
    if drift_dim in promised:
        checks.append(
            EmittedCheck(
                check_type="column_list_changed",
                target_column=None,
                rule_parameters={},
                source="default",
            )
        )
        covered.add(drift_dim)

    # ─ Compute not_assessed_reasons ─
    reasons: dict[str, str] = {}
    for dim in promised - covered:
        reasons[dim] = _default_reason_for_dim(dim, table_decl, pii_fields)

    return EmittedCheckSet(checks=checks, not_assessed_reasons=reasons)


# ─── Per-field rules ─────────────────────────────────────────────────────────

_TEXT_TYPES = {"string", "text", "varchar", "char", "str"}
_NUMERIC_TYPES = {"number", "integer", "float", "int", "numeric", "decimal", "double"}


def _is_text_type(logical_type: str | None) -> bool:
    """Whether a logical type is text-shaped (length / regex / enum apply).

    Defaults to True for unknown types so a missing logicalType doesn't
    silently drop text checks — the DB-side check still validates safely.
    """
    if logical_type is None:
        return True
    return logical_type.lower() in _TEXT_TYPES


def _is_numeric_type(logical_type: str | None) -> bool:
    if logical_type is None:
        return False
    return logical_type.lower() in _NUMERIC_TYPES


def _dim_for(check_type: str) -> str:
    """The canonical ODPS dimension for a check type.

    Single source of truth for every emit decision: the promised-gate, the
    threshold-promise lookup, and EmittedCheck.dimension all derive from this
    one value. A branch that gates on a different dimension than the check's
    canonical one makes the caller orphan-delete and re-emit it every run.
    """
    dim = get_dimension_for_check_type(check_type)
    if dim is None:
        raise ValueError(f"check type {check_type!r} has no ODPS dimension")
    return dim.value


def _emit_field_checks(
    decl: FieldDeclaration,
    profile: FieldProfileAggregates,
    inference: InferenceResult,
    dq_profile: dict[str, Any],
    promised: set[str],
) -> list[EmittedCheck]:
    out: list[EmittedCheck] = []
    is_pii = decl.classification == "PII"

    # Every rule below resolves its dimension from _dim_for(check_type) — the
    # canonical map — and uses that one value for the promised-gate, the
    # threshold-promise lookup, and (implicitly, via __post_init__) the
    # emitted check's dimension. No rule hardcodes a dimension string.

    # 1. Primary key → uniqueness + completeness (zero-tolerance).
    #    PK does NOT short-circuit other rules — a PK field can still carry
    #    acceptedLength / acceptedRange / pattern declarations that emit
    #    their own checks. PK only adds the zero-tolerance uniqueness +
    #    nulls-count pair on top.
    pk_dim = _dim_for("distinct_percent")
    if decl.primary_key and pk_dim in promised:
        params = thresholds_from_promise("min_max_percent", "100")
        if params:
            out.append(
                EmittedCheck(
                    check_type="distinct_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    source="declaration",
                )
            )
        if _dim_for("nulls_count") in promised:
            params = thresholds_from_promise("max_count", "99")
            if params:
                out.append(
                    EmittedCheck(
                        check_type="nulls_count",
                        target_column=decl.name,
                        rule_parameters=params,
                        source="declaration",
                    )
                )

    # 2. Unique flag (non-PK) → uniqueness
    if decl.unique and not decl.primary_key and pk_dim in promised:
        promise = promise_for_dimension(dq_profile, pk_dim)
        params = thresholds_from_promise("min_max_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="distinct_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    source="declaration",
                )
            )

    # 3. acceptedValues OR inferred enum → in-set check.
    #    Numeric columns → number_found_in_set_percent.
    #    Text columns    → text_found_in_set_percent.
    #    Inferred enum only fires on text columns — on a numeric column a
    #    small distinct count is a range, not an enum (handled by rule 4).
    is_numeric = _is_numeric_type(decl.logical_type)
    is_text_col = _is_text_type(decl.logical_type)
    in_set_type = "number_found_in_set_percent" if is_numeric else "text_found_in_set_percent"
    in_set_dim = _dim_for(in_set_type)
    values: list[Any] | None = decl.accepted_values
    source = "declaration" if values else None
    # Inferred enum only on text columns. Date/number columns with a
    # small distinct count are ranges, not enums.
    if values is None and inference.enum and in_set_dim in promised and not is_pii and is_text_col:
        values = list(inference.enum.values)
        source = "inference"
    if values is not None and in_set_dim in promised:
        promise = promise_for_dimension(dq_profile, in_set_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type=in_set_type,
                    target_column=decl.name,
                    rule_parameters=params,
                    parameters={"expected_values": [str(v) for v in values]},
                    source=source or "declaration",
                )
            )

    # 4. acceptedRange OR inferred numeric range → number_in_range_percent.
    #    Numeric range = a conformity concern (range standards), per ODPS.
    nr_dim = _dim_for("number_in_range_percent")
    nrange = decl.accepted_range
    source = "declaration" if nrange else None
    if nrange is None and inference.numeric_range and nr_dim in promised:
        nrange = (inference.numeric_range.min, inference.numeric_range.max)
        source = "inference"
    if nrange is not None and nr_dim in promised:
        promise = promise_for_dimension(dq_profile, nr_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="number_in_range_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    parameters={"min_value": nrange[0], "max_value": nrange[1]},
                    source=source or "declaration",
                )
            )

    # 5. acceptedLength → text_length_in_range_percent.
    #    Length checks are text-only. A numeric column's "length" is just
    #    the VARCHAR-cast digit count — meaningless as a quality signal.
    tl_dim = _dim_for("text_length_in_range_percent")
    is_text = _is_text_type(decl.logical_type)
    lrange = decl.accepted_length if is_text else None
    source = "declaration" if lrange else None
    if lrange is None and is_text and inference.length_range and tl_dim in promised and not is_pii:
        lrange = (inference.length_range.min, inference.length_range.max)
        source = "inference"
    if lrange is not None and tl_dim in promised:
        promise = promise_for_dimension(dq_profile, tl_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="text_length_in_range_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    parameters={"min_length": lrange[0], "max_length": lrange[1]},
                    source=source or "declaration",
                )
            )

    # 6. format / pattern → format-specific or regex
    cc_dim = _dim_for("text_valid_country_code_percent")
    cur_dim = _dim_for("text_valid_currency_code_percent")
    if decl.format == "country_code" and cc_dim in promised:
        promise = promise_for_dimension(dq_profile, cc_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="text_valid_country_code_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    source="declaration",
                )
            )
    elif decl.format == "currency_code" and cur_dim in promised:
        promise = promise_for_dimension(dq_profile, cur_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="text_valid_currency_code_percent",
                    target_column=decl.name,
                    rule_parameters=params,
                    source="declaration",
                )
            )
    else:
        # Inferred ISO codelist → accuracy via codelist check
        if inference.codelist is not None and not is_pii:
            check_type = {
                "ISO_3166_alpha2": "text_valid_country_code_percent",
                "ISO_4217": "text_valid_currency_code_percent",
            }.get(inference.codelist.standard)
            if check_type and _dim_for(check_type) in promised:
                cl_dim = _dim_for(check_type)
                promise = promise_for_dimension(dq_profile, cl_dim)
                params = thresholds_from_promise("min_percent", promise)
                if params:
                    out.append(
                        EmittedCheck(
                            check_type=check_type,
                            target_column=decl.name,
                            rule_parameters=params,
                            source="inference",
                        )
                    )

        # Regex / pattern → conformity.
        # Inferred regex is text-only — a synthesized pattern over a
        # date/number column's string cast is noise. Explicit pattern/
        # format declarations are honored regardless (producer intent).
        regex_dim = _dim_for("text_matching_regex_percent")
        pattern: str | None = decl.pattern
        source = "declaration" if pattern else None
        if pattern is None and decl.format:
            pattern = _format_to_regex(decl.format)
            source = "declaration"
        if pattern is None and is_text and inference.regex and regex_dim in promised and not is_pii:
            pattern = inference.regex.pattern
            source = "inference"
        if pattern is not None and regex_dim in promised:
            promise = promise_for_dimension(dq_profile, regex_dim)
            params = thresholds_from_promise("min_percent", promise)
            if params:
                out.append(
                    EmittedCheck(
                        check_type="text_matching_regex_percent",
                        target_column=decl.name,
                        rule_parameters=params,
                        parameters={"regex_pattern": pattern},
                        source=source or "declaration",
                    )
                )

    return out


# ─── Table-level rules ───────────────────────────────────────────────────────


def _emit_table_checks(
    table_decl: TableDeclaration,
    dq_profile: dict[str, Any],
    promised: set[str],
) -> list[EmittedCheck]:
    out: list[EmittedCheck] = []

    # data_freshness — timeliness.
    # The DQOps data_freshness sensor is column-level: it reads
    # MAX({{ column_name }}). The freshness column is the check's
    # target_column, not a sensor parameter.
    fresh_dim = _dim_for("data_freshness")
    if fresh_dim in promised and table_decl.freshness_column:
        promise = promise_for_dimension(dq_profile, fresh_dim)
        params = thresholds_from_promise("max_value", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="data_freshness",
                    target_column=table_decl.freshness_column,
                    rule_parameters=params,
                    source="declaration",
                )
            )

    # row_count — coverage
    if _dim_for("row_count") in promised and table_decl.expected_row_count:
        lo, hi = table_decl.expected_row_count
        # row_count is min_max_count rule type. thresholds_from_promise
        # returns None for it (no clean promise→count mapping); we provide
        # the absolute bounds directly from the declaration.
        out.append(
            EmittedCheck(
                check_type="row_count",
                target_column=None,
                rule_parameters={
                    "warning": {"min_count": lo, "max_count": hi},
                    "error": {"min_count": max(0, int(lo * 0.9)), "max_count": int(hi * 1.1)},
                },
                source="declaration",
            )
        )

    # consistentWith — cross-source accuracy
    match_dim = _dim_for("total_row_count_match_percent")
    if match_dim in promised and table_decl.consistent_with:
        promise = promise_for_dimension(dq_profile, match_dim)
        params = thresholds_from_promise("min_percent", promise)
        if params:
            out.append(
                EmittedCheck(
                    check_type="total_row_count_match_percent",
                    target_column=None,
                    rule_parameters=params,
                    source="declaration",
                )
            )

    return out


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _covered_by_dim(checks: list[EmittedCheck], dim: str) -> bool:
    return any(c.dimension == dim for c in checks)


def _invert_completeness(promise: str | None) -> str | None:
    """nulls_percent uses max_percent shape, but the dim is completeness which
    is measured as min_percent. Invert: 99% completeness → 1% nulls max.
    """
    from dq_platform.profilers.threshold_engine import parse_promise_percent

    pct = parse_promise_percent(promise)
    if pct is None:
        return None
    return str(max(0.0, 100.0 - pct))


_FORMAT_TO_REGEX: dict[str, str] = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "iso_date": r"^\d{4}-\d{2}-\d{2}$",
    "iso_datetime": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
    "e164": r"^\+[1-9]\d{6,14}$",
    "url": r"^https?://[A-Za-z0-9.-]+",
}


def _format_to_regex(fmt: str) -> str | None:
    return _FORMAT_TO_REGEX.get(fmt)


def _default_reason_for_dim(
    dim: str,
    table_decl: TableDeclaration,
    pii_fields: set[str],
) -> str:
    if dim == "validity":
        return REASON_NO_VALIDITY_DECL
    if dim == "accuracy":
        return REASON_NO_AUTHORITATIVE_SOURCE
    if dim == "timeliness":
        if not table_decl.freshness_column:
            return REASON_NO_FRESHNESS_COLUMN
    if dim == "coverage":
        if not table_decl.expected_row_count:
            return REASON_NO_EXPECTED_ROW_COUNT
    if dim == "consistency":
        if not table_decl.consistent_with:
            return REASON_NO_CONSISTENT_WITH
    return REASON_NO_VALIDITY_DECL  # generic fallback
