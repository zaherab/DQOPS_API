"""Result schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class ResultResponse(BaseModel):
    """Schema for check result response."""

    id: UUID
    check_id: UUID
    job_id: UUID
    connection_id: UUID
    target_table: str
    target_column: str | None
    check_type: str
    executed_at: datetime
    actual_value: float | None
    expected_value: float | None
    passed: bool
    execution_time_ms: int | None
    rows_scanned: int | None
    result_details: dict[str, Any] | None
    error_message: str | None

    model_config = {"from_attributes": True}


class ResultSummary(BaseModel):
    """Schema for results summary."""

    total_executions: int
    passed: int
    failed: int
    pass_rate: float
    avg_execution_time_ms: float | None
