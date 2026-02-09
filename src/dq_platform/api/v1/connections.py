"""Connection endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query, status

from dq_platform.api.deps import APIKey, ConnectionServiceDep
from dq_platform.models.connection import ConnectionType
from dq_platform.schemas.common import PaginatedResponse, TestConnectionResponse
from dq_platform.schemas.connection import (
    ColumnInfoResponse,
    ConnectionCreate,
    ConnectionResponse,
    ConnectionUpdate,
    TableInfoResponse,
)

router = APIRouter()


@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    data: ConnectionCreate,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> ConnectionResponse:
    """Create a new data source connection."""
    connection = await service.create(
        name=data.name,
        connection_type=data.connection_type,
        config=data.config,
        description=data.description,
        metadata_=data.metadata,
    )
    return ConnectionResponse.model_validate(connection)


@router.get("", response_model=PaginatedResponse[ConnectionResponse])
async def list_connections(
    service: ConnectionServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    connection_type: ConnectionType | None = None,
) -> PaginatedResponse[ConnectionResponse]:
    """List all connections with pagination."""
    connections, total = await service.list_connections(
        offset=offset,
        limit=limit,
        connection_type=connection_type,
    )
    return PaginatedResponse(
        items=[ConnectionResponse.model_validate(c) for c in connections],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> ConnectionResponse:
    """Get a connection by ID."""
    connection = await service.get(connection_id)
    return ConnectionResponse.model_validate(connection)


@router.put("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    data: ConnectionUpdate,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> ConnectionResponse:
    """Update a connection."""
    connection = await service.update(
        connection_id=connection_id,
        name=data.name,
        description=data.description,
        config=data.config,
        metadata_=data.metadata,
    )
    return ConnectionResponse.model_validate(connection)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> None:
    """Delete a connection (soft delete)."""
    await service.delete(connection_id)


@router.post("/{connection_id}/test", response_model=TestConnectionResponse)
async def test_connection(
    connection_id: UUID,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> TestConnectionResponse:
    """Test if a connection is valid."""
    try:
        success = await service.test_connection(connection_id)
        return TestConnectionResponse(success=success, message="Connection successful")
    except Exception as e:
        return TestConnectionResponse(success=False, message=str(e))


@router.get("/{connection_id}/schemas", response_model=list[str])
async def get_schemas(
    connection_id: UUID,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> list[str]:
    """Get list of schemas in the data source."""
    return await service.get_schemas(connection_id)


@router.get(
    "/{connection_id}/schemas/{schema}/tables",
    response_model=list[TableInfoResponse],
)
async def get_tables(
    connection_id: UUID,
    schema: str,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> list[TableInfoResponse]:
    """Get list of tables in a schema."""
    tables = await service.get_tables(connection_id, schema)
    return [TableInfoResponse(**t.__dict__) for t in tables]


@router.get(
    "/{connection_id}/schemas/{schema}/tables/{table}/columns",
    response_model=list[ColumnInfoResponse],
)
async def get_columns(
    connection_id: UUID,
    schema: str,
    table: str,
    service: ConnectionServiceDep,
    api_key: APIKey,
) -> list[ColumnInfoResponse]:
    """Get list of columns in a table."""
    columns = await service.get_columns(connection_id, schema, table)
    return [ColumnInfoResponse(**c.__dict__) for c in columns]
