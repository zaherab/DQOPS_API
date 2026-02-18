"""ODPS dimension scoring schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class DimensionScore(BaseModel):
    """Score for a single ODPS quality dimension."""

    dimension: str
    score: float | None  # 0-100, None if not assessed
    status: str  # "green" | "amber" | "red" | "not_assessed"
    check_count: int
    passed_count: int
    warning_count: int
    error_count: int
    fatal_count: int


class DimensionScoreResponse(BaseModel):
    """Aggregated dimension scores across all 8 ODPS dimensions."""

    dimensions: list[DimensionScore]
    overall_score: float | None
    assessed_count: int
    total_dimensions: int  # always 8
    computed_at: datetime


class DimensionDailySnapshot(BaseModel):
    """A single day's score for a dimension."""

    date: str
    score: float | None


class DimensionTrendResponse(BaseModel):
    """Trend data for a single dimension over time."""

    dimension: str
    snapshots: list[DimensionDailySnapshot]


class DimensionCheckDetail(BaseModel):
    """Detail of a single check contributing to a dimension."""

    check_id: UUID
    check_name: str
    check_type: str
    category: str
    dimension: str
    target_table: str
    target_column: str | None
    latest_passed: bool | None
    latest_severity: str | None
    latest_executed_at: datetime | None


class DimensionMappingEntry(BaseModel):
    """A single category-to-dimension mapping entry."""

    category: str
    dimension: str
