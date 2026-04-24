"""Pytest configuration and fixtures."""

import asyncio
import os
from collections.abc import AsyncGenerator, Generator, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from dq_platform.config import Settings, get_settings
from dq_platform.db.session import get_db
from dq_platform.main import app
from dq_platform.models.base import Base  # noqa: F401  (kept for downstream test imports)

# ─── Patch the production engine for tests ────────────────────────────────
# The prod `dq_platform.db.session.engine` is created at import time bound
# to whichever event loop ran the import. In tests, starlette's TestClient
# and pytest-asyncio use different loops; any DB call that goes through
# that module-level engine (notably `/health`, which calls
# `engine.connect()` directly and bypasses the `get_db` dependency
# overrides) errors with "Future attached to a different loop".
#
# NullPool → every checkout is a fresh connection in the current loop.
# Swapping the prod module attribute in place keeps all prod code paths
# working without per-route overrides.


def _install_test_engine() -> None:
    from dq_platform import main as _main_module
    from dq_platform.db import session as _session_module

    db_url = os.getenv("TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform_test")
    _test_engine = create_async_engine(db_url, poolclass=NullPool)
    _test_factory = sessionmaker(
        _test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    _session_module.engine = _test_engine
    _session_module.async_session_factory = _test_factory
    # `main.py` does `from dq_platform.db.session import engine` at the top —
    # that's a local binding; patching the session module alone doesn't
    # update it. Patch both.
    _main_module.engine = _test_engine


_install_test_engine()

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


# Test database URL. Uses a *dedicated* test DB (`dq_platform_test`) so the
# destructive teardown below can safely DROP SCHEMA without clobbering the dev
# DB. Override via TEST_DATABASE_URL in CI. The safety guard in `async_engine`
# refuses to run if the URL doesn't contain "test".
_default_db_url = "postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform_test"
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", _default_db_url)


# ─── Alembic-driven schema setup ───────────────────────────────────────────
# Replaces the older `Base.metadata.create_all`, which can't create the
# TimescaleDB hypertable on `check_results` (no unique constraint on
# `result_id` to back the FK from `incidents`). The real migrations know how
# to omit that FK — so we run them directly and get exact schema parity with
# prod.


def _alembic_config(db_url: str) -> Config:
    """Alembic config pointing at the given DB URL.

    `alembic.ini` is at the repo root (two levels up from `tests/conftest.py`).
    """
    cfg = Config(str(Path(__file__).resolve().parent.parent / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _run_migrations(db_url: str) -> None:
    """Apply all migrations up to head against `db_url`.

    `migrations/env.py:44-48` reads `DATABASE_URL` from the environment; set
    it so the async runner targets the test DB instead of whatever the
    caller's shell had pointing at dev.
    """
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url
    try:
        command.upgrade(_alembic_config(db_url), "head")
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous


async def _ensure_test_db_exists(db_url: str) -> None:
    """Create the test DB if it's missing.

    `CREATE DATABASE` can't run inside a transaction; uses asyncpg directly
    against the admin `postgres` DB in AUTOCOMMIT.
    """
    import asyncpg

    url = make_url(db_url)
    test_db = url.database
    assert test_db, f"No database name in TEST_DATABASE_URL: {db_url}"

    conn = await asyncpg.connect(
        host=url.host,
        port=url.port or 5432,
        user=url.username,
        password=url.password,
        database="postgres",
    )
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", test_db)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{test_db}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_test_db() -> None:
    """Ensure the dedicated test DB exists before any test runs."""
    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_ensure_test_db_exists(TEST_DATABASE_URL))


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
    """Async engine bound to a fully-migrated test DB.

    Setup: runs `alembic upgrade head` so the schema matches prod exactly
    (including the TimescaleDB hypertable on `check_results`, which
    `Base.metadata.create_all` cannot reproduce).

    Teardown: `DROP SCHEMA public CASCADE; CREATE SCHEMA public;` wipes
    tables, hypertables, chunks, enums, and the `alembic_version` row in a
    single statement — cheaper and more reliable than reverse-migrating.
    """
    # Safety: the teardown is destructive. If someone overrides
    # TEST_DATABASE_URL to point at prod/dev, fail fast.
    assert "test" in TEST_DATABASE_URL.lower(), f"TEST_DATABASE_URL must contain 'test' (got: {TEST_DATABASE_URL})"

    # Alembic's `env.py` spawns its own `asyncio.run(...)` which collides
    # with pytest-asyncio's running loop — offload to a thread so the inner
    # loop is independent.
    await asyncio.to_thread(_run_migrations, TEST_DATABASE_URL)

    # NullPool: every checkout makes a fresh connection. Without this,
    # connections get bound to whichever event loop created them — when
    # starlette's TestClient runs a request on its own loop and reuses a
    # pooled asyncpg conn bound to pytest-asyncio's loop, asyncpg raises
    # "Future attached to a different loop". NullPool sidesteps the whole
    # class of cross-loop failures.
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)

    yield engine

    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))

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
async def client(async_engine) -> AsyncGenerator[AsyncClient, None]:
    """Create async test client.

    Fresh session per request, mirroring the real `get_db` (commit on
    success, rollback on error). Avoids sharing `db_session` state across
    request boundaries and keeps semantics identical to production.
    """
    factory = sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_settings] = get_test_settings

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def sync_client(async_engine) -> Generator[TestClient, None, None]:
    """Create sync test client.

    Each request gets a fresh session on starlette's own event loop.
    Reusing the pytest-asyncio `db_session` here crashed with
    "Future attached to a different loop" — starlette's TestClient spins
    up its own loop. NullPool + fresh-per-request session sidesteps the
    cross-loop issue entirely. Test fixtures must `commit()` their writes
    so requests on the fresh session can see them; teardown in
    `async_engine` drops the whole schema between tests.
    """
    factory = sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        # Mirrors the real `get_db` (db/session.py): commit on success,
        # rollback on error. Without the commit, mutations made by route
        # handlers disappear when the session closes, which breaks any
        # test that deletes/creates a row and then reads it back.
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_settings] = get_test_settings

    with TestClient(app, headers={"X-API-Key": "test-key"}) as tc:
        yield tc

    app.dependency_overrides.clear()
