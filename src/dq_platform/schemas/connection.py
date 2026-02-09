"""Connection schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from dq_platform.models.connection import ConnectionType


class ConnectionCreate(BaseModel):
    """Schema for creating a connection."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    connection_type: ConnectionType
    config: dict[str, Any] = Field(
        ...,
        description="Connection configuration (host, port, database, user, password, etc.)",
    )
    metadata: dict[str, Any] | None = None


class ConnectionUpdate(BaseModel):
    """Schema for updating a connection."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


class ConnectionResponse(BaseModel):
    """Schema for connection response."""

    id: UUID
    name: str
    description: str | None
    connection_type: ConnectionType
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="metadata_")
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TableInfoResponse(BaseModel):
    """Schema for table info response."""

    schema_name: str
    table_name: str
    table_type: str
    row_count: int | None = None


class ColumnInfoResponse(BaseModel):
    """Schema for column info response."""

    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    default_value: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
