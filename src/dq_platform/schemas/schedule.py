"""Schedule schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ScheduleCreate(BaseModel):
    """Schema for creating a schedule."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    check_id: UUID
    cron_expression: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Cron expression (e.g., '0 */6 * * *' for every 6 hours)",
    )
    timezone: str = Field(default="UTC", max_length=50)


class ScheduleUpdate(BaseModel):
    """Schema for updating a schedule."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    cron_expression: str | None = Field(None, min_length=1, max_length=100)
    timezone: str | None = Field(None, max_length=50)
    is_active: bool | None = None


class ScheduleResponse(BaseModel):
    """Schema for schedule response."""

    id: UUID
    name: str
    description: str | None
    check_id: UUID
    cron_expression: str
    timezone: str
    is_active: bool
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
