"""Results endpoints."""

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Query

from dq_platform.api.deps import APIKey, ResultServiceDep
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.schemas.result import ResultResponse, ResultSummary

router = APIRouter()


@router.get("", response_model=PaginatedResponse[ResultResponse])
async def query_results(
    service: ResultServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    check_id: UUID | None = None,
    connection_id: UUID | None = None,
    passed: bool | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
) -> PaginatedResponse[ResultResponse]:
    """Query check results with filters."""
    results, total = await service.query(
        offset=offset,
        limit=limit,
        check_id=check_id,
        connection_id=connection_id,
        passed=passed,
        from_date=from_date,
        to_date=to_date,
    )
    return PaginatedResponse(
        items=[ResultResponse.model_validate(r) for r in results],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/summary", response_model=ResultSummary)
async def get_results_summary(
    service: ResultServiceDep,
    api_key: APIKey,
    check_id: UUID | None = None,
    connection_id: UUID | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
) -> ResultSummary:
    """Get summary statistics for check results."""
    summary = await service.get_summary(
        check_id=check_id,
        connection_id=connection_id,
        from_date=from_date,
        to_date=to_date,
    )
    return ResultSummary(**summary)
