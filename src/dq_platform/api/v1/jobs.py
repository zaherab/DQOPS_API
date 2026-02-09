"""Job endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query

from dq_platform.api.deps import APIKey, ExecutionServiceDep
from dq_platform.models.job import JobStatus
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.schemas.job import JobResponse

router = APIRouter()


@router.get("", response_model=PaginatedResponse[JobResponse])
async def list_jobs(
    service: ExecutionServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    check_id: UUID | None = None,
    status: JobStatus | None = None,
) -> PaginatedResponse[JobResponse]:
    """List all jobs with pagination and filters."""
    jobs, total = await service.list_jobs(
        offset=offset,
        limit=limit,
        check_id=check_id,
        status=status,
    )
    return PaginatedResponse(
        items=[JobResponse.model_validate(j) for j in jobs],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    service: ExecutionServiceDep,
    api_key: APIKey,
) -> JobResponse:
    """Get a job by ID."""
    job = await service.get_job(job_id)
    return JobResponse.model_validate(job)


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: UUID,
    service: ExecutionServiceDep,
    api_key: APIKey,
) -> JobResponse:
    """Cancel a pending or running job."""
    job = await service.cancel_job(job_id)
    return JobResponse.model_validate(job)
