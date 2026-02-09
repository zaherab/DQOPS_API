"""Check API endpoints."""

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.deps import get_db
from dq_platform.api.errors import NotFoundError
from dq_platform.checks import (
    get_checks_by_category,
    get_column_level_checks,
    get_table_level_checks,
)
from dq_platform.checks import (
    list_checks as list_dqops_checks,
)
from dq_platform.checks.gx_registry import (
    get_check_description,
    is_column_level_check,
)
from dq_platform.models.check import CheckMode, CheckTimeScale, CheckType
from dq_platform.schemas.check import (
    BatchRunRequest,
    CheckCreate,
    CheckExecutionDetail,
    CheckPreviewRequest,
    CheckResponse,
    CheckTypeInfo,
    CheckUpdate,
    RunCheckRequest,
)
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.services.check_service import CheckService
from dq_platform.services.execution_service import ExecutionService

router = APIRouter()


@router.post("", response_model=CheckResponse, status_code=status.HTTP_201_CREATED)
async def create_check(
    data: CheckCreate,
    db: AsyncSession = Depends(get_db),
) -> CheckResponse:
    """Create a new data quality check."""
    service = CheckService(db)
    check = await service.create_check(
        name=data.name,
        description=data.description,
        connection_id=data.connection_id,
        check_type=data.check_type,
        target_schema=data.target_schema,
        target_table=data.target_table,
        target_column=data.target_column,
        parameters=data.parameters,
        metadata=data.metadata,
        check_mode=data.check_mode,
        time_scale=data.time_scale,
        partition_by_column=data.partition_by_column,
        rule_parameters=data.rule_parameters.model_dump() if data.rule_parameters else None,
    )
    return CheckResponse.model_validate(check)


@router.get("", response_model=PaginatedResponse[CheckResponse])
async def list_checks(
    connection_id: UUID | None = None,
    check_type: CheckType | None = None,
    check_mode: CheckMode | None = None,
    target_table: str | None = None,
    is_active: bool | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[CheckResponse]:
    """List data quality checks with optional filtering."""
    service = CheckService(db)
    checks, total = await service.list_checks(
        connection_id=connection_id,
        check_type=check_type,
        check_mode=check_mode,
        target_table=target_table,
        is_active=is_active,
        offset=offset,
        limit=limit,
    )
    return PaginatedResponse(
        items=[CheckResponse.model_validate(c) for c in checks],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/types", response_model=list[CheckTypeInfo])
async def list_check_types(
    category: str | None = None,
    column_level_only: bool = False,
    table_level_only: bool = False,
) -> list[CheckTypeInfo]:
    """List available check types.

    Args:
        category: Filter by category (volume, nulls, uniqueness, etc.)
        column_level_only: Only return column-level checks
        table_level_only: Only return table-level checks
    """
    # Get DQOps-style checks
    if category:
        dqops_checks = get_checks_by_category(category)
    elif column_level_only:
        dqops_checks = get_column_level_checks()
    elif table_level_only:
        dqops_checks = get_table_level_checks()
    else:
        dqops_checks = list_dqops_checks()

    # Convert to CheckTypeInfo
    check_types = []

    # Add DQOps checks
    for check in dqops_checks:
        check_types.append(
            CheckTypeInfo(
                type=check.name,
                description=check.description,
                is_column_level=check.is_column_level,
                category=check.category,
            )
        )

    # Add legacy Great Expectations checks not yet in DQOps format
    existing_types = {c.name for c in dqops_checks}
    for check_type in CheckType:
        if check_type.value not in existing_types:
            try:
                is_col_level = is_column_level_check(check_type)
                description = get_check_description(check_type)
                check_types.append(
                    CheckTypeInfo(
                        type=check_type.value,
                        description=description,
                        is_column_level=is_col_level,
                        category=None,
                    )
                )
            except ValueError:
                # Skip if not in GX registry
                pass

    return check_types


@router.get("/categories", response_model=list[str])
async def list_check_categories() -> list[str]:
    """List available check categories."""
    categories = set()
    for check in list_dqops_checks():
        categories.add(check.category)
    return sorted(list(categories))


@router.get("/modes", response_model=list[str])
async def list_check_modes() -> list[str]:
    """List available check modes (profiling, monitoring, partitioned)."""
    return [m.value for m in CheckMode]


@router.get("/time-scales", response_model=list[str])
async def list_time_scales() -> list[str]:
    """List available time scales (daily, monthly)."""
    return [ts.value for ts in CheckTimeScale]


@router.post("/batch/run", response_model=list[dict[str, Any]])
async def batch_run_checks(
    request: BatchRunRequest,
    db: AsyncSession = Depends(get_db),
) -> list[dict[str, Any]]:
    """Run multiple checks asynchronously.

    Returns job IDs for each check execution.
    """
    execution_service = ExecutionService(db)
    jobs: list[dict[str, Any]] = []

    for check_id in request.check_ids:
        try:
            job = await execution_service.create_job(
                check_id=check_id,
                triggered_by=request.triggered_by,
            )
            task_id = await execution_service.submit_job(job.id)
            jobs.append(
                {
                    "check_id": str(check_id),
                    "job_id": str(job.id),
                    "task_id": task_id,
                    "status": "started",
                }
            )
        except NotFoundError:
            jobs.append(
                {
                    "check_id": str(check_id),
                    "job_id": None,
                    "task_id": None,
                    "status": "error",
                    "message": "Check not found",
                }
            )

    return jobs


@router.post("/validate/preview", response_model=CheckExecutionDetail)
async def validate_check_preview(
    request: CheckPreviewRequest,
    db: AsyncSession = Depends(get_db),
) -> CheckExecutionDetail:
    """Preview a check configuration without saving it.

    This allows testing check parameters before creating the check.
    """
    service = CheckService(db)
    result = await service.preview_check_config(
        connection_id=request.connection_id,
        check_type=request.check_type,
        target_schema=request.target_schema,
        target_table=request.target_table,
        target_column=request.target_column,
        parameters=request.parameters,
        rule_parameters=request.rule_parameters.model_dump() if request.rule_parameters else None,
    )
    return CheckExecutionDetail(
        check_id=UUID("00000000-0000-0000-0000-000000000000"),  # Dummy ID for preview
        check_type=result.check_type,
        check_name=result.check_name,
        severity=result.severity,
        passed=result.passed,
        sensor_value=result.sensor_value,
        expected=result.expected,
        actual=result.actual,
        message=result.message,
        executed_sql=result.executed_sql,
        executed_at=result.executed_at,
    )


# Dynamic routes with {check_id} must come after static routes
@router.get("/{check_id}", response_model=CheckResponse)
async def get_check(
    check_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> CheckResponse:
    """Get a check by ID."""
    service = CheckService(db)
    check = await service.get_check(check_id)
    if not check:
        raise NotFoundError("Check", str(check_id))
    return CheckResponse.model_validate(check)


@router.patch("/{check_id}", response_model=CheckResponse)
async def update_check(
    check_id: UUID,
    data: CheckUpdate,
    db: AsyncSession = Depends(get_db),
) -> CheckResponse:
    """Update a check."""
    service = CheckService(db)

    rule_params = None
    if data.rule_parameters:
        rule_params = data.rule_parameters.model_dump()

    check = await service.update_check(
        check_id=check_id,
        name=data.name,
        description=data.description,
        target_schema=data.target_schema,
        target_table=data.target_table,
        target_column=data.target_column,
        parameters=data.parameters,
        metadata=data.metadata,
        is_active=data.is_active,
        partition_by_column=data.partition_by_column,
        rule_parameters=rule_params,
    )
    if not check:
        raise NotFoundError("Check", str(check_id))
    return CheckResponse.model_validate(check)


@router.delete("/{check_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_check(
    check_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> None:
    """Delete a check (soft delete)."""
    service = CheckService(db)
    success = await service.delete_check(check_id)
    if not success:
        raise NotFoundError("Check", str(check_id))


@router.post("/{check_id}/run", response_model=dict[str, Any])
async def run_check(
    check_id: UUID,
    request: RunCheckRequest | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Run a check asynchronously.

    Returns a job ID that can be used to track execution.
    """
    request = request or RunCheckRequest()
    execution_service = ExecutionService(db)
    job = await execution_service.create_job(
        check_id=check_id,
        triggered_by=request.triggered_by,
    )
    task_id = await execution_service.submit_job(job.id)

    return {
        "job_id": str(job.id),
        "task_id": task_id,
        "status": job.status.value,
        "message": "Check execution started",
    }


@router.post("/{check_id}/preview", response_model=CheckExecutionDetail)
async def preview_check(
    check_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> CheckExecutionDetail:
    """Preview a check execution (dry run) without saving results.

    This runs the check synchronously and returns the result immediately.
    """
    service = CheckService(db)
    result = await service.preview_check(check_id)
    return CheckExecutionDetail(
        check_id=check_id,
        check_type=result.check_type,
        check_name=result.check_name,
        severity=result.severity,
        passed=result.passed,
        sensor_value=result.sensor_value,
        expected=result.expected,
        actual=result.actual,
        message=result.message,
        executed_sql=result.executed_sql,
        executed_at=result.executed_at,
    )
