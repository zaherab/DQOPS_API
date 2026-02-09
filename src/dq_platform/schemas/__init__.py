"""Pydantic schemas for API request/response."""

from dq_platform.schemas.check import (
    CheckCreate,
    CheckResponse,
    CheckTypeInfo,
    CheckUpdate,
)
from dq_platform.schemas.common import ErrorResponse, PaginatedResponse
from dq_platform.schemas.connection import (
    ColumnInfoResponse,
    ConnectionCreate,
    ConnectionResponse,
    ConnectionUpdate,
    TableInfoResponse,
)
from dq_platform.schemas.incident import IncidentResponse, IncidentStatusUpdate
from dq_platform.schemas.job import JobResponse
from dq_platform.schemas.result import ResultResponse, ResultSummary
from dq_platform.schemas.schedule import ScheduleCreate, ScheduleResponse, ScheduleUpdate

__all__ = [
    # Common
    "PaginatedResponse",
    "ErrorResponse",
    # Connection
    "ConnectionCreate",
    "ConnectionUpdate",
    "ConnectionResponse",
    "TableInfoResponse",
    "ColumnInfoResponse",
    # Check
    "CheckCreate",
    "CheckUpdate",
    "CheckResponse",
    "CheckTypeInfo",
    # Job
    "JobResponse",
    # Result
    "ResultResponse",
    "ResultSummary",
    # Incident
    "IncidentResponse",
    "IncidentStatusUpdate",
    # Schedule
    "ScheduleCreate",
    "ScheduleUpdate",
    "ScheduleResponse",
]
