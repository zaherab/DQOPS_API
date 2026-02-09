"""Schedule endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query, status

from dq_platform.api.deps import APIKey, ScheduleServiceDep
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.schemas.schedule import ScheduleCreate, ScheduleResponse, ScheduleUpdate

router = APIRouter()


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    data: ScheduleCreate,
    service: ScheduleServiceDep,
    api_key: APIKey,
) -> ScheduleResponse:
    """Create a new schedule."""
    schedule = await service.create(
        name=data.name,
        check_id=data.check_id,
        cron_expression=data.cron_expression,
        timezone_str=data.timezone,
        description=data.description,
    )
    return ScheduleResponse.model_validate(schedule)


@router.get("", response_model=PaginatedResponse[ScheduleResponse])
async def list_schedules(
    service: ScheduleServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    check_id: UUID | None = None,
    is_active: bool | None = None,
) -> PaginatedResponse[ScheduleResponse]:
    """List all schedules with pagination and filters."""
    schedules, total = await service.list_schedules(
        offset=offset,
        limit=limit,
        check_id=check_id,
        is_active=is_active,
    )
    return PaginatedResponse(
        items=[ScheduleResponse.model_validate(s) for s in schedules],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: UUID,
    service: ScheduleServiceDep,
    api_key: APIKey,
) -> ScheduleResponse:
    """Get a schedule by ID."""
    schedule = await service.get(schedule_id)
    return ScheduleResponse.model_validate(schedule)


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: UUID,
    data: ScheduleUpdate,
    service: ScheduleServiceDep,
    api_key: APIKey,
) -> ScheduleResponse:
    """Update a schedule."""
    schedule = await service.update(
        schedule_id=schedule_id,
        name=data.name,
        description=data.description,
        cron_expression=data.cron_expression,
        timezone_str=data.timezone,
        is_active=data.is_active,
    )
    return ScheduleResponse.model_validate(schedule)


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(
    schedule_id: UUID,
    service: ScheduleServiceDep,
    api_key: APIKey,
) -> None:
    """Delete a schedule."""
    await service.delete(schedule_id)
