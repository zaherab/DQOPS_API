"""ODPS dimension scoring endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query

from dq_platform.api.deps import DimensionServiceDep
from dq_platform.odps.dimension_mapping import CATEGORY_TO_DIMENSION
from dq_platform.schemas.dimension import (
    DimensionCheckDetail,
    DimensionMappingEntry,
    DimensionScoreResponse,
    DimensionTrendResponse,
)

router = APIRouter()


@router.get("/scores", response_model=DimensionScoreResponse)
async def get_dimension_scores(
    service: DimensionServiceDep,
    connection_id: UUID | None = None,
    check_ids: list[UUID] | None = Query(None),
    from_date: str | None = None,
    to_date: str | None = None,
) -> DimensionScoreResponse:
    """Get scores for all 8 ODPS quality dimensions."""
    from datetime import datetime

    fd = datetime.fromisoformat(from_date) if from_date else None
    td = datetime.fromisoformat(to_date) if to_date else None

    return await service.get_dimension_scores(
        connection_id=connection_id,
        check_ids=check_ids,
        from_date=fd,
        to_date=td,
    )


@router.get("/mapping", response_model=list[DimensionMappingEntry])
async def get_dimension_mapping() -> list[DimensionMappingEntry]:
    """Return the static category-to-dimension mapping."""
    return [
        DimensionMappingEntry(category=cat, dimension=dim.value) for cat, dim in sorted(CATEGORY_TO_DIMENSION.items())
    ]


@router.get("/{dimension}/trend", response_model=DimensionTrendResponse)
async def get_dimension_trend(
    dimension: str,
    service: DimensionServiceDep,
    connection_id: UUID | None = None,
    days: int = Query(30, ge=1, le=365),
) -> DimensionTrendResponse:
    """Get daily score trend for a single ODPS dimension."""
    return await service.get_dimension_trend(
        dimension=dimension,
        connection_id=connection_id,
        days=days,
    )


@router.get("/{dimension}/checks", response_model=list[DimensionCheckDetail])
async def get_dimension_checks(
    dimension: str,
    service: DimensionServiceDep,
    connection_id: UUID | None = None,
) -> list[DimensionCheckDetail]:
    """Get all checks that contribute to a single ODPS dimension."""
    return await service.get_dimension_checks(
        dimension=dimension,
        connection_id=connection_id,
    )
