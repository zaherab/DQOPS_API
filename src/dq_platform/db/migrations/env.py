"""Alembic migration environment."""

import asyncio
import logging
import socket
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import async_engine_from_config

from dq_platform.models.base import Base

logger = logging.getLogger("alembic.env")

CONNECT_MAX_ATTEMPTS = 30
CONNECT_BACKOFF_BASE = 1.0
CONNECT_BACKOFF_CAP = 8.0
TRANSIENT_CONNECT_ERRORS = (ConnectionRefusedError, socket.gaierror, OSError)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations with connection."""
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


def _is_transient_connect_error(exc: BaseException) -> bool:
    """True if exc is a transient DB-not-ready error (vs. auth/schema)."""
    if isinstance(exc, TRANSIENT_CONNECT_ERRORS):
        return True
    if isinstance(exc, DBAPIError | OperationalError):
        cause = exc.__cause__ or exc.orig
        if isinstance(cause, TRANSIENT_CONNECT_ERRORS):
            return True
        msg = str(cause or exc).lower()
        return any(s in msg for s in ("connection refused", "cannot connect", "starting up", "shutting down"))
    return False


async def _connect_with_retry(connectable):
    """Retry connect on transient failures with exponential backoff + jitter."""
    import random

    for attempt in range(1, CONNECT_MAX_ATTEMPTS + 1):
        try:
            return await connectable.connect().__aenter__()
        except Exception as exc:
            if attempt == CONNECT_MAX_ATTEMPTS or not _is_transient_connect_error(exc):
                raise
            delay = min(CONNECT_BACKOFF_CAP, CONNECT_BACKOFF_BASE * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.1)
            logger.warning(
                "DB not ready (attempt %d/%d): %s — retry in %.1fs",
                attempt,
                CONNECT_MAX_ATTEMPTS,
                exc.__class__.__name__,
                delay,
            )
            await asyncio.sleep(delay)


async def run_async_migrations() -> None:
    """Run migrations in async mode."""
    import os

    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        config.set_main_option("sqlalchemy.url", db_url)

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    connection = await _connect_with_retry(connectable)
    try:
        await connection.run_sync(do_run_migrations)
    finally:
        await connection.close()
        await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
