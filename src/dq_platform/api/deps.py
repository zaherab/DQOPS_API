"""API dependencies for dependency injection."""

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.core.security import verify_api_key
from dq_platform.db.session import get_db
from dq_platform.services.check_service import CheckService
from dq_platform.services.connection_service import ConnectionService
from dq_platform.services.execution_service import ExecutionService
from dq_platform.services.incident_service import IncidentService
from dq_platform.services.notification_service import NotificationService
from dq_platform.services.result_service import ResultService
from dq_platform.services.schedule_service import ScheduleService

# Type aliases for dependency injection
DBSession = Annotated[AsyncSession, Depends(get_db)]
APIKey = Annotated[str, Depends(verify_api_key)]


async def get_connection_service(db: DBSession) -> ConnectionService:
    """Get ConnectionService instance."""
    return ConnectionService(db)


async def get_check_service(db: DBSession) -> CheckService:
    """Get CheckService instance."""
    return CheckService(db)


async def get_execution_service(db: DBSession) -> ExecutionService:
    """Get ExecutionService instance."""
    return ExecutionService(db)


async def get_result_service(db: DBSession) -> ResultService:
    """Get ResultService instance."""
    return ResultService(db)


async def get_incident_service(db: DBSession) -> IncidentService:
    """Get IncidentService instance."""
    return IncidentService(db)


async def get_schedule_service(db: DBSession) -> ScheduleService:
    """Get ScheduleService instance."""
    return ScheduleService(db)


async def get_notification_service(db: DBSession) -> NotificationService:
    """Get NotificationService instance."""
    return NotificationService(db)


# Typed service dependencies
ConnectionServiceDep = Annotated[ConnectionService, Depends(get_connection_service)]
CheckServiceDep = Annotated[CheckService, Depends(get_check_service)]
ExecutionServiceDep = Annotated[ExecutionService, Depends(get_execution_service)]
ResultServiceDep = Annotated[ResultService, Depends(get_result_service)]
IncidentServiceDep = Annotated[IncidentService, Depends(get_incident_service)]
ScheduleServiceDep = Annotated[ScheduleService, Depends(get_schedule_service)]
NotificationServiceDep = Annotated[NotificationService, Depends(get_notification_service)]
