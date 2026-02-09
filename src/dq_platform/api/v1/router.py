"""Main API router."""

from fastapi import APIRouter

from dq_platform.api.v1 import checks, connections, incidents, jobs, notifications, results, schedules

api_router = APIRouter()

api_router.include_router(connections.router, prefix="/connections", tags=["connections"])
api_router.include_router(checks.router, prefix="/checks", tags=["checks"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(results.router, prefix="/results", tags=["results"])
api_router.include_router(incidents.router, prefix="/incidents", tags=["incidents"])
api_router.include_router(schedules.router, prefix="/schedules", tags=["schedules"])
api_router.include_router(notifications.router, prefix="/notifications/channels", tags=["notifications"])
