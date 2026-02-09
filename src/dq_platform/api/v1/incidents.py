"""Incident endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query

from dq_platform.api.deps import APIKey, IncidentServiceDep
from dq_platform.models.incident import IncidentSeverity, IncidentStatus
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.schemas.incident import IncidentResponse, IncidentStatusUpdate

router = APIRouter()


@router.get("", response_model=PaginatedResponse[IncidentResponse])
async def list_incidents(
    service: IncidentServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    check_id: UUID | None = None,
    status: IncidentStatus | None = None,
    severity: IncidentSeverity | None = None,
) -> PaginatedResponse[IncidentResponse]:
    """List all incidents with pagination and filters."""
    incidents, total = await service.list_incidents(
        offset=offset,
        limit=limit,
        check_id=check_id,
        status=status,
        severity=severity,
    )
    return PaginatedResponse(
        items=[IncidentResponse.model_validate(i) for i in incidents],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{incident_id}", response_model=IncidentResponse)
async def get_incident(
    incident_id: UUID,
    service: IncidentServiceDep,
    api_key: APIKey,
) -> IncidentResponse:
    """Get an incident by ID."""
    incident = await service.get(incident_id)
    return IncidentResponse.model_validate(incident)


@router.patch("/{incident_id}", response_model=IncidentResponse)
async def update_incident_status(
    incident_id: UUID,
    data: IncidentStatusUpdate,
    service: IncidentServiceDep,
    api_key: APIKey,
) -> IncidentResponse:
    """Update incident status (acknowledge/resolve)."""
    incident = await service.update_status(
        incident_id=incident_id,
        status=data.status,
        by=data.by,
        notes=data.notes,
    )
    return IncidentResponse.model_validate(incident)
