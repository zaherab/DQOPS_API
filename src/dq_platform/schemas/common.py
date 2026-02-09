"""Common schemas used across the API."""

from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response wrapper."""

    items: list[T]
    total: int
    offset: int
    limit: int

    @property
    def has_more(self) -> bool:
        """Check if there are more items."""
        return self.offset + len(self.items) < self.total


class ErrorDetail(BaseModel):
    """Error detail."""

    message: str
    type: str
    field: str | None = None


class ErrorResponse(BaseModel):
    """Error response."""

    error: ErrorDetail


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str


class MessageResponse(BaseModel):
    """Simple message response."""

    message: str


class TestConnectionResponse(BaseModel):
    """Connection test response."""

    success: bool
    message: str | None = None
