"""Deterministic check engine endpoints.

POST /v1/profile/run             Profile a (connection, schema, table).
POST /v1/checks/emit-deterministic  Emit a calibrated check spec set.

Both endpoints are stateless w.r.t. the engine's own DB — they pull
aggregates/samples from the customer DB, run pure-Python inference + emit,
and return the result. The MLG-Spec-Reader caller decides whether to
register the emitted checks with DQOps.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from dq_platform.api.deps import ConnectionServiceDep
from dq_platform.connectors.factory import get_connector
from dq_platform.core.encryption import decrypt_config
from dq_platform.core.security import verify_api_key
from dq_platform.profilers.check_emitter import (
    FieldDeclaration,
    FieldProfileAggregates,
    TableDeclaration,
    emit,
)
from dq_platform.profilers.inference_engine import (
    ColumnProfileLite,
    InferenceResult,
    infer_all,
)
from dq_platform.profilers.profile_runner import (
    FieldProfileSpec,
    run_profile,
)
from dq_platform.profilers.sample_cache import CacheKey, get_sample_cache
from dq_platform.profilers.sample_fetcher import SampleSpec, fetch_sample
from dq_platform.services.profile_audit import audit

router = APIRouter(dependencies=[Depends(verify_api_key)])


# ─── Request / response models ────────────────────────────────────────────


class FieldSpecBody(BaseModel):
    name: str
    logical_type: str | None = None
    classification: str | None = None  # "PII" / "PCI" / "PHI" / "public"
    primary_key: bool = False
    sample_needed: bool = True


class ProfileRequest(BaseModel):
    org_id: str
    connection_id: UUID
    # `schema_name` on the model; accepts `schema` from JSON for API ergonomics.
    schema_name: str = Field(..., alias="schema")
    table: str
    pk: str | None = None  # column used for hash-mod sampling
    fields: list[FieldSpecBody]
    sample_size: int = 1_000
    seed: int = 1

    model_config = {"populate_by_name": True}


class ColumnProfileResp(BaseModel):
    count: int
    nulls: int
    distinct: int
    min: Any | None = None
    max: Any | None = None
    min_len: int | None = None
    max_len: int | None = None


class InferenceResp(BaseModel):
    format: str | None = None
    codelist_standard: str | None = None
    enum_values: list[Any] | None = None
    regex_pattern: str | None = None
    length_min: int | None = None
    length_max: int | None = None
    numeric_min: float | None = None
    numeric_max: float | None = None
    date_min: str | None = None
    date_max: str | None = None


class ProfileResponse(BaseModel):
    row_count: int
    columns: dict[str, ColumnProfileResp]
    inferences: dict[str, InferenceResp]
    schema_fingerprint: str | None
    cached_sample: bool
    # Declared fields absent from the physical table (schema drift). Profiled
    # fields skip these; the caller should drop them before emitting checks so
    # it never registers a check against a non-existent column.
    missing_columns: list[str] = []


class FieldDeclarationBody(BaseModel):
    name: str
    logical_type: str | None = None
    examples: list[Any] = Field(default_factory=list)
    primary_key: bool = False
    unique: bool = False
    format: str | None = None
    pattern: str | None = None
    accepted_values: list[Any] | None = None
    accepted_range: tuple[float, float] | None = None
    accepted_length: tuple[int, int] | None = None
    classification: str | None = None


class TableDeclarationBody(BaseModel):
    freshness_column: str | None = None
    expected_row_count: tuple[int, int] | None = None
    consistent_with: list[dict[str, Any]] | None = None


class EmitRequest(BaseModel):
    fields: list[FieldDeclarationBody]
    table: TableDeclarationBody = Field(default_factory=TableDeclarationBody)
    dq_profile: dict[str, Any]
    table_profile: ProfileResponse


class EmittedCheckBody(BaseModel):
    check_type: str
    target_column: str | None
    rule_parameters: dict[str, Any]
    parameters: dict[str, Any]
    dimension: str
    source: str


class EmitResponse(BaseModel):
    checks: list[EmittedCheckBody]
    not_assessed_reasons: dict[str, str]


# ─── Routes ───────────────────────────────────────────────────────────────


@router.post("/profile/run", response_model=ProfileResponse)
async def run_profile_endpoint(
    body: ProfileRequest,
    conn_service: ConnectionServiceDep,
) -> ProfileResponse:
    """Run the aggregate profile + (cached) sample inference for one table."""
    # Resolve customer connection. ConnectionService.get raises HTTPException(404)
    # internally when the row is missing, so a missing connection bubbles up.
    conn_record = await conn_service.get(body.connection_id)
    config = decrypt_config(conn_record.config_encrypted)
    connector = get_connector(conn_record.connection_type, config)
    await connector.connect_async()
    try:
        return await _profile_and_infer(connector, body)
    finally:
        await connector.disconnect_async()


async def _profile_and_infer(connector: Any, body: ProfileRequest) -> ProfileResponse:
    # Resolve the live column set first. A declared field that no longer
    # exists in the physical table (schema drift — column renamed/dropped)
    # must not blow up the whole profile + sample run. Profile the columns
    # that exist, report the rest as missing.
    fields = body.fields
    missing_columns: list[str] = []
    # A declared PK wins. When the caller declares none — e.g. an ODPS-only
    # contentSchema with no DQ extensions — fall back to the table's real
    # primary key so sampling (and therefore format/codelist inference) can
    # still run. Derived from get_columns() below, which we already call, so
    # this adds no extra round-trip.
    resolved_pk = body.pk
    try:
        # Exact-name membership. The declared name is reused verbatim as a
        # quoted identifier in the profile/sample SQL, so a case-mismatched
        # name is unusable anyway — treating it as missing (drop it) is the
        # safe outcome, vs. keeping it and failing the whole query.
        cols = connector.get_columns(body.schema_name, body.table)
        live_cols = {c.name for c in cols}
        fields = [f for f in body.fields if f.name in live_cols]
        missing_columns = [f.name for f in body.fields if f.name not in live_cols]
        if not resolved_pk:
            resolved_pk = next((c.name for c in cols if getattr(c, "is_primary_key", False)), None)
    except Exception:
        # Schema introspection unavailable (permission/network). Fall back to
        # profiling every declared field — no regression vs prior behaviour.
        fields = body.fields

    # Profile (aggregate round-trip).
    field_specs = [
        FieldProfileSpec(
            name=f.name,
            logical_type=f.logical_type,
            classification=f.classification,
        )
        for f in fields
    ]
    profile_sql = connector.build_profile_sql(
        body.schema_name,
        body.table,
        [
            {
                "name": f.name,
                "logical_type": f.logical_type,
                "classification": f.classification,
            }
            for f in fields
        ],
    )
    async with audit(
        kind="profile",
        schema=body.schema_name,
        table=body.table,
        sql=profile_sql,
        org_id=body.org_id,
        connection_id=str(body.connection_id),
    ) as rec:
        profile = await run_profile(connector, body.schema_name, body.table, field_specs)
        rec.set_result(list(profile.columns.values()))

    # Sample fetch (cached). Only when a PK is available (declared or
    # introspected) and at least one field is text-shaped — otherwise
    # inference has no work to do.
    cached = False
    rows: list[dict[str, Any]] = []
    if resolved_pk and any(f.sample_needed and f.classification != "PII" for f in fields):
        cache = get_sample_cache()
        cache_key = CacheKey(
            org_id=body.org_id,
            connection_id=str(body.connection_id),
            schema=body.schema_name,
            table=body.table,
            schema_fingerprint=profile.schema_fingerprint or "no-fp",
            seed=body.seed,
            n=body.sample_size,
        )
        rows = cache.get(cache_key) or []
        if rows:
            cached = True
        else:
            pii_cols = {f.name for f in fields if f.classification == "PII"}
            sample_cols = [f.name for f in fields if f.name not in pii_cols]
            spec = SampleSpec(columns=sample_cols, pk=resolved_pk, n=body.sample_size, seed=body.seed)
            sample_sql = connector.build_sample_sql(
                body.schema_name,
                body.table,
                sample_cols,
                resolved_pk,
                body.sample_size,
                body.seed,
            )
            async with audit(
                kind="sample",
                schema=body.schema_name,
                table=body.table,
                sql=sample_sql,
                org_id=body.org_id,
                connection_id=str(body.connection_id),
            ) as rec:
                rows = await fetch_sample(connector, body.schema_name, body.table, spec, pii_cols)
                rec.set_result(rows)
            if rows:
                cache.put(cache_key, rows)

    # Inference per column.
    inferences: dict[str, InferenceResp] = {}
    for f in fields:
        if f.classification == "PII":
            inferences[f.name] = InferenceResp()
            continue
        col_values = [r.get(f.name) for r in rows if f.name in r]
        col_profile = profile.columns.get(f.name)
        lite = ColumnProfileLite(
            count=col_profile.count if col_profile else 0,
            nulls=col_profile.nulls if col_profile else 0,
            distinct=col_profile.distinct if col_profile else 0,
            min=col_profile.min if col_profile else None,
            max=col_profile.max if col_profile else None,
            min_len=col_profile.min_len if col_profile else None,
            max_len=col_profile.max_len if col_profile else None,
        )
        result = infer_all(col_values, lite, f.logical_type)
        inferences[f.name] = InferenceResp(
            format=result.format.format if result.format else None,
            codelist_standard=result.codelist.standard if result.codelist else None,
            enum_values=list(result.enum.values) if result.enum else None,
            regex_pattern=result.regex.pattern if result.regex else None,
            length_min=result.length_range.min if result.length_range else None,
            length_max=result.length_range.max if result.length_range else None,
            numeric_min=result.numeric_range.min if result.numeric_range else None,
            numeric_max=result.numeric_range.max if result.numeric_range else None,
            date_min=result.date_range.min if result.date_range else None,
            date_max=result.date_range.max if result.date_range else None,
        )

    return ProfileResponse(
        row_count=profile.row_count,
        columns={
            name: ColumnProfileResp(
                count=cp.count,
                nulls=cp.nulls,
                distinct=cp.distinct,
                min=cp.min,
                max=cp.max,
                min_len=cp.min_len,
                max_len=cp.max_len,
            )
            for name, cp in profile.columns.items()
        },
        inferences=inferences,
        schema_fingerprint=profile.schema_fingerprint,
        cached_sample=cached,
        missing_columns=missing_columns,
    )


@router.post("/checks/emit-deterministic", response_model=EmitResponse)
async def emit_deterministic(body: EmitRequest) -> EmitResponse:
    """Emit the deterministic check set for one (table, profile, dq_profile).

    Pure compute — no DB access. Caller passes the profile from
    /v1/profile/run (or builds one out-of-band).
    """
    field_decls = [
        FieldDeclaration(
            name=f.name,
            logical_type=f.logical_type,
            examples=f.examples,
            primary_key=f.primary_key,
            unique=f.unique,
            format=f.format,
            pattern=f.pattern,
            accepted_values=f.accepted_values,
            accepted_range=f.accepted_range,
            accepted_length=f.accepted_length,
            classification=f.classification,
        )
        for f in body.fields
    ]
    table_decl = TableDeclaration(
        freshness_column=body.table.freshness_column,
        expected_row_count=body.table.expected_row_count,
        consistent_with=body.table.consistent_with,
    )

    field_profiles: dict[str, FieldProfileAggregates] = {}
    for name, cp in body.table_profile.columns.items():
        field_profiles[name] = FieldProfileAggregates(
            count=cp.count,
            nulls=cp.nulls,
            distinct=cp.distinct,
            min=cp.min,
            max=cp.max,
            min_len=cp.min_len,
            max_len=cp.max_len,
        )

    # Rehydrate InferenceResult per column from the response shape.
    from dq_platform.profilers.inference_engine import (
        CodelistRef,
        DateRange,
        EnumCandidate,
        FormatRef,
        LengthRange,
        NumericRange,
        RegexCandidate,
    )

    field_inferences: dict[str, InferenceResult] = {}
    for name, inf in body.table_profile.inferences.items():
        field_inferences[name] = InferenceResult(
            format=FormatRef(format=inf.format, coverage=1.0) if inf.format else None,
            codelist=(
                CodelistRef(standard=inf.codelist_standard, version="", coverage=1.0) if inf.codelist_standard else None
            ),
            enum=(EnumCandidate(values=tuple(inf.enum_values), coverage=1.0) if inf.enum_values else None),
            regex=(RegexCandidate(pattern=inf.regex_pattern, coverage=1.0) if inf.regex_pattern else None),
            length_range=(
                LengthRange(min=inf.length_min, max=inf.length_max)
                if inf.length_min is not None and inf.length_max is not None
                else None
            ),
            numeric_range=(
                NumericRange(min=inf.numeric_min, max=inf.numeric_max)
                if inf.numeric_min is not None and inf.numeric_max is not None
                else None
            ),
            date_range=(
                DateRange(min=inf.date_min, max=inf.date_max)
                if inf.date_min is not None and inf.date_max is not None
                else None
            ),
        )

    out = emit(field_decls, table_decl, body.dq_profile, field_profiles, field_inferences)
    return EmitResponse(
        checks=[
            EmittedCheckBody(
                check_type=c.check_type,
                target_column=c.target_column,
                rule_parameters=c.rule_parameters,
                parameters=c.parameters,
                dimension=c.dimension,
                source=c.source,
            )
            for c in out.checks
        ],
        not_assessed_reasons=out.not_assessed_reasons,
    )
