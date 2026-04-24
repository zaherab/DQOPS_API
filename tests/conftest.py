"""Pytest configuration and fixtures."""

import asyncio
import os
from collections.abc import AsyncGenerator, Generator, Sequence
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dq_platform.config import Settings, get_settings
from dq_platform.db.session import get_db
from dq_platform.main import app
from dq_platform.models.base import Base

# ─── SQLAlchemy Result mock helpers ────────────────────────────────────────
# Unit tests that mock the AsyncSession need to mimic how services consume
# `await db.execute(...)` results. Using these helpers instead of hand-rolling
# `mock.all.return_value = [(1,)]` etc. keeps the test shape in sync with the
# idiomatic SQLAlchemy access patterns the service layer actually uses:
#   - COUNT(...) queries:  result.scalar()
#   - Scalar ORM queries:  result.scalar_one_or_none() / scalar_one()
#   - Full-row queries:    result.scalars().all()
# If the service ever switches to a different access pattern, update the
# helper here once instead of patching every test.


def mock_count_result(value: int) -> MagicMock:
    """Mock an SQLAlchemy Result for a COUNT(*) query (consumed via .scalar())."""
    mock = MagicMock()
    mock.scalar.return_value = value
    return mock


def mock_scalars_result(items: Sequence[Any]) -> MagicMock:
    """Mock an SQLAlchemy Result for a multi-row query (consumed via .scalars().all())."""
    mock = MagicMock()
    mock.scalars.return_value.all.return_value = list(items)
    return mock


def mock_scalar_one_result(item: Any | None) -> MagicMock:
    """Mock an SQLAlchemy Result for a single-row query.

    Pass `None` for the "not found" case; the mock then behaves correctly for
    both `.scalar_one_or_none()` and `.scalar()`.
    """
    mock = MagicMock()
    mock.scalar_one_or_none.return_value = item
    mock.scalar.return_value = item
    return mock


# Test database URL - use PostgreSQL for tests (JSONB compatibility)
# Use 'postgres' as hostname when running inside Docker, 'localhost' for local
_default_db_url = "postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform"
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", _default_db_url)


def get_test_settings() -> Settings:
    """Get test settings."""
    return Settings(
        database_url=TEST_DATABASE_URL,
        encryption_key="test-encryption-key-32-bytes-long",
        debug=True,
    )


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def async_engine():
    """Create async test engine."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        pool_size=5,
        max_overflow=0,
    )

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Clean up - drop tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(async_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create async database session for tests with transaction rollback."""
    async_session_factory = sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with async_session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Create async test client."""

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_settings] = get_test_settings

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def sync_client(db_session: AsyncSession) -> Generator[TestClient, None, None]:
    """Create sync test client."""

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_settings] = get_test_settings

    with TestClient(app) as tc:
        yield tc

    app.dependency_overrides.clear()
