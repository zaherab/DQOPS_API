"""Redis-backed sample cache.

Samples are expensive to refetch — they move raw rows across the wire.
This cache keeps them for as long as the source schema is unchanged.

Redis (not DuckDB) is the backing store: the API runs multiple uvicorn
workers, and DuckDB is single-writer per file — a per-process file would
mean workers never share cache entries. Redis is already a project
dependency (Celery broker) and shares cleanly across workers.

Invalidation:
  - schema fingerprint baked into the cache key — a shape change yields a
    different key, so stale entries are simply never read (and expire via
    TTL).
  - explicit invalidate_table() deletes every entry for a table.

Cache key is namespaced by org_id to prevent cross-tenant collision on
overlapping connection_id spaces.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _ttl_seconds() -> int:
    # Default 24h. A schema change invalidates via key mismatch well before
    # this; the TTL just bounds memory for tables that stop being profiled.
    return int(os.environ.get("DQ_SAMPLE_CACHE_TTL_SECONDS", str(24 * 3600)))


_KEY_PREFIX = "dq:sample:"
_TABLE_INDEX_PREFIX = "dq:sample-index:"


@dataclass
class CacheKey:
    org_id: str
    connection_id: str
    schema: str
    table: str
    schema_fingerprint: str
    seed: int
    n: int

    def digest(self) -> str:
        h = hashlib.sha256()
        for part in (
            self.org_id,
            self.connection_id,
            self.schema,
            self.table,
            self.schema_fingerprint,
            str(self.seed),
            str(self.n),
        ):
            h.update(part.encode())
            h.update(b"|")
        return h.hexdigest()

    def redis_key(self) -> str:
        return _KEY_PREFIX + self.digest()

    def table_index_key(self) -> str:
        """Set key listing all cache entries for one (org, conn, schema, table).

        Lets invalidate_table() find every fingerprint/seed/n variant
        without scanning the whole keyspace.
        """
        h = hashlib.sha256()
        for part in (self.org_id, self.connection_id, self.schema, self.table):
            h.update(part.encode())
            h.update(b"|")
        return _TABLE_INDEX_PREFIX + h.hexdigest()


class SampleCache:
    """Redis-backed cache for sample row payloads.

    Stores each sample as a JSON string with a TTL. Shared across all
    uvicorn workers. Degrades gracefully: if Redis is unreachable, get()
    returns None (cache miss) and put() is a no-op — correctness holds,
    only the speed-up is lost.
    """

    def __init__(self, url: str | None = None) -> None:
        import redis

        self._redis = redis.from_url(  # type: ignore[no-untyped-call]
            url or _redis_url(), decode_responses=True
        )
        self._ttl = _ttl_seconds()

    def get(self, key: CacheKey) -> list[dict[str, Any]] | None:
        """Read sample rows by key. None on miss or Redis failure.

        Fingerprint drift is handled implicitly: a changed fingerprint
        produces a different key, so a stale sample is simply not found.
        """
        try:
            raw = self._redis.get(key.redis_key())
        except Exception:
            return None
        if raw is None:
            return None
        try:
            rows: list[dict[str, Any]] = json.loads(raw)
            return rows
        except (TypeError, ValueError):
            return None

    def put(self, key: CacheKey, rows: list[dict[str, Any]]) -> None:
        """Store sample rows with a TTL. No-op on Redis failure."""
        try:
            payload = json.dumps(rows, default=str)
            pipe = self._redis.pipeline()
            pipe.set(key.redis_key(), payload, ex=self._ttl)
            # Track this entry in the table index so invalidate_table can
            # find it. Index set expires a bit after the entries.
            pipe.sadd(key.table_index_key(), key.redis_key())
            pipe.expire(key.table_index_key(), self._ttl + 3600)
            pipe.execute()
        except Exception:
            # Best-effort cache — never fail the request over a cache write.
            pass

    def invalidate_table(
        self,
        org_id: str,
        connection_id: str,
        schema: str,
        table: str,
    ) -> int:
        """Force-evict every entry for one table. Returns count removed."""
        probe = CacheKey(
            org_id=org_id,
            connection_id=connection_id,
            schema=schema,
            table=table,
            schema_fingerprint="",
            seed=0,
            n=0,
        )
        index_key = probe.table_index_key()
        try:
            members = self._redis.smembers(index_key)
            if not members:
                return 0
            pipe = self._redis.pipeline()
            for m in members:
                pipe.delete(m)
            pipe.delete(index_key)
            pipe.execute()
            return len(members)
        except Exception:
            return 0

    def close(self) -> None:
        try:
            self._redis.close()
        except Exception:
            pass


# Singleton instance for the running service.
_INSTANCE: SampleCache | None = None


def get_sample_cache() -> SampleCache:
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = SampleCache()
    return _INSTANCE


def __reset_sample_cache_for_tests() -> None:
    """Test-only: drop the singleton so a fresh instance is built."""
    global _INSTANCE
    if _INSTANCE is not None:
        _INSTANCE.close()
    _INSTANCE = None
