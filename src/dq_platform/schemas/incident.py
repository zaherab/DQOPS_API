"""Incident schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from dq_platform.models.incident import IncidentSeverity, IncidentStatus


class IncidentResponse(BaseModel):
    """Schema for incident response."""

    id: UUID
    check_id: UUID
    status: IncidentStatus
    severity: IncidentSeverity
    title: str
    description: str | None
    first_failure_at: datetime
    last_failure_at: datetime
    failure_count: int
    resolved_at: datetime | None
    resolved_by: str | None
    resolution_notes: str | None
    acknowledged_at: datetime | None
    acknowledged_by: str | None
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_")
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class IncidentStatusUpdate(BaseModel):
    """Schema for updating incident status."""

    status: IncidentStatus
    by: str
    notes: str | None = None
