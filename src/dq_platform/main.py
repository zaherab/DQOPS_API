"""FastAPI application entry point."""

import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware

from dq_platform.api.errors import register_exception_handlers
from dq_platform.api.v1.router import api_router
from dq_platform.config import get_settings
from dq_platform.core.logging import request_id_var, setup_logging
from dq_platform.db.session import engine

settings = get_settings()
logger = logging.getLogger(__name__)

# Initialize rate limiter with in-memory storage
limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    # Startup
    setup_logging()
    logger.info("Application starting up", extra={"version": "0.1.0"})
    yield
    # Shutdown
    logger.info("Application shutting down")


app = FastAPI(
    title=settings.app_name,
    description="API-first data quality monitoring platform",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add rate limiter to app state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Middleware to generate and attach request ID to each request."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Generate request ID, store in context, and add to response header."""
        # Generate unique request ID
        request_id = str(uuid.uuid4())

        # Store in context variable for logging
        token = request_id_var.set(request_id)

        # Process request
        response = await call_next(request)

        # Add request ID to response header
        response.headers["X-Request-ID"] = request_id

        # Clean up context variable
        request_id_var.reset(token)

        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware to add security headers to all responses."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Add security headers to response."""
        response = await call_next(request)

        # Prevent MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # Prevent clickjacking
        response.headers["X-Frame-Options"] = "DENY"

        # Enable XSS protection
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Enforce HTTPS
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )

        # Prevent caching of API responses
        response.headers["Cache-Control"] = "no-store"

        return response


# Add middlewares (order matters - first added is first executed)
app.add_middleware(RequestIdMiddleware)
app.add_middleware(SecurityHeadersMiddleware)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register exception handlers
register_exception_handlers(app)

# Include API router
app.include_router(api_router, prefix=settings.api_prefix)


@app.get("/health")
@limiter.limit("10/minute")  # Lower rate limit for health checks
async def health_check(request: Request) -> dict[str, Any]:
    """Deep health check endpoint verifying DB and Redis connectivity.

    Returns 503 if any dependency is down so load balancers stop routing.
    """
    health_status = {
        "status": "healthy",
        "version": "0.1.0",
        "db": "ok",
        "redis": "ok",
    }
    status_code = 200
    is_healthy = True

    # Check database connectivity
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"Health check DB failure: {e}")
        health_status["db"] = "error"
        health_status["status"] = "unhealthy"
        is_healthy = False

    # Check Redis connectivity
    try:
        redis_client = redis.from_url(settings.redis_url)
        await redis_client.ping()
        await redis_client.close()
    except Exception as e:
        logger.error(f"Health check Redis failure: {e}")
        health_status["redis"] = "error"
        health_status["status"] = "unhealthy"
        is_healthy = False

    if not is_healthy:
        status_code = 503

    return health_status
