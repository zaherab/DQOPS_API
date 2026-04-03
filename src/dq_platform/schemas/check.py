"""Check schemas."""

import re
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dq_platform.checks.rules import Severity
from dq_platform.models.check import CheckMode, CheckTimeScale, CheckType

# Pattern for safe SQL identifiers
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$\-]*$")
# Schema identifiers may contain dots (catalog.schema)
_SCHEMA_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$.\-]*$")


def _validate_identifier(value: str | None, field_name: str, *, allow_dot: bool = False) -> str | None:
    """Validate a SQL identifier against injection patterns."""
    if value is None:
        return value
    pattern = _SCHEMA_IDENTIFIER_RE if allow_dot else _IDENTIFIER_RE
    if not pattern.match(value):
        raise ValueError(f"{field_name} contains invalid characters: {value!r}")
    return value


class SeverityThreshold(BaseModel):
    """Severity threshold configuration (DQOps-style)."""

    min_value: float | None = None
    max_value: float | None = None
    min_percent: float | None = None
    max_percent: float | None = None
    max_change_percent: float | None = None
    min_count: int | None = None
    max_count: int | None = None


class RuleParameters(BaseModel):
    """Rule parameters with severity levels (DQOps-style).

    Example:
        {
            "warning": {"max_percent": 5.0},
            "error": {"max_percent": 10.0},
            "fatal": {"max_percent": 20.0}
        }
    """

    warning: SeverityThreshold | None = None
    error: SeverityThreshold | None = None
    fatal: SeverityThreshold | None = None


class CheckCreate(BaseModel):
    """Schema for creating a check."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    connection_id: UUID
    check_type: CheckType
    check_mode: CheckMode = CheckMode.MONITORING
    time_scale: CheckTimeScale | None = None
    target_schema: str | None = Field(None, max_length=255)
    target_table: str = Field(..., min_length=1, max_length=255)
    target_column: str | None = Field(None, max_length=255)
    partition_by_column: str | None = Field(None, max_length=255)
    parameters: dict[str, Any] = Field(default_factory=dict)
    rule_parameters: RuleParameters | None = None
    metadata: dict[str, Any] | None = None

    @field_validator("target_schema")
    @classmethod
    def validate_target_schema(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_schema", allow_dot=True)

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str) -> str:
        return _validate_identifier(v, "target_table")  # type: ignore[return-value]

    @field_validator("target_column")
    @classmethod
    def validate_target_column(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_column")

    @field_validator("partition_by_column")
    @classmethod
    def validate_partition_by_column(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "partition_by_column")


class CheckUpdate(BaseModel):
    """Schema for updating a check."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    target_schema: str | None = Field(None, max_length=255)
    target_table: str | None = Field(None, min_length=1, max_length=255)
    target_column: str | None = Field(None, max_length=255)
    partition_by_column: str | None = Field(None, max_length=255)
    parameters: dict[str, Any] | None = None
    rule_parameters: RuleParameters | None = None
    metadata: dict[str, Any] | None = None
    is_active: bool | None = None

    @field_validator("target_schema")
    @classmethod
    def validate_target_schema(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_schema", allow_dot=True)

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_table")

    @field_validator("target_column")
    @classmethod
    def validate_target_column(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_column")

    @field_validator("partition_by_column")
    @classmethod
    def validate_partition_by_column(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "partition_by_column")


class CheckResponse(BaseModel):
    """Schema for check response."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    description: str | None
    connection_id: UUID
    check_type: CheckType
    check_mode: CheckMode
    time_scale: CheckTimeScale | None
    target_schema: str | None
    target_table: str
    target_column: str | None
    partition_by_column: str | None
    parameters: dict[str, Any]
    rule_parameters: dict[str, Any] | None
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_")
    is_active: bool
    created_at: datetime
    updated_at: datetime


class CheckTypeInfo(BaseModel):
    """Schema for check type information."""

    type: str
    description: str
    is_column_level: bool
    category: str | None = None


class RunCheckRequest(BaseModel):
    """Schema for running a check."""

    triggered_by: str = "api"
    partition_value: str | None = None  # For partitioned checks


class BatchRunRequest(BaseModel):
    """Schema for batch running checks."""

    check_ids: list[UUID]
    triggered_by: str = "api"


class CheckExecutionDetail(BaseModel):
    """Schema for check execution details."""

    check_id: UUID
    check_type: str
    check_name: str
    severity: Severity
    passed: bool
    sensor_value: float | None
    expected: Any
    actual: Any
    message: str
    executed_sql: str | None
    executed_at: datetime


class CheckPreviewRequest(BaseModel):
    """Schema for previewing a check (dry run)."""

    connection_id: UUID
    check_type: CheckType
    target_schema: str | None = Field(None, max_length=255)
    target_table: str = Field(..., min_length=1, max_length=255)
    target_column: str | None = Field(None, max_length=255)
    parameters: dict[str, Any] = Field(default_factory=dict)
    rule_parameters: RuleParameters | None = None

    @field_validator("target_schema")
    @classmethod
    def validate_target_schema(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_schema", allow_dot=True)

    @field_validator("target_table")
    @classmethod
    def validate_target_table(cls, v: str) -> str:
        return _validate_identifier(v, "target_table")  # type: ignore[return-value]

    @field_validator("target_column")
    @classmethod
    def validate_target_column(cls, v: str | None) -> str | None:
        return _validate_identifier(v, "target_column")
