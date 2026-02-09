"""Job schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from dq_platform.models.job import JobStatus


class JobResponse(BaseModel):
    """Schema for job response."""

    id: UUID
    check_id: UUID
    status: JobStatus
    celery_task_id: str | None
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_")
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobCreateResponse(BaseModel):
    """Schema for job creation response."""

    job_id: UUID
    celery_task_id: str | None
    status: JobStatus
