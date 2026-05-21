"""Base connector interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnInfo:
    """Column metadata."""

    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    default_value: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass
class TableInfo:
    """Table metadata."""

    schema_name: str
    table_name: str
    table_type: str  # 'TABLE' or 'VIEW'
    row_count: int | None = None


class BaseConnector(ABC):
    """Abstract base class for database connectors.

    All connectors must implement this interface to support:
    - Connection lifecycle (connect, disconnect)
    - Query execution
    - Metadata discovery (schemas, tables, columns)
    - Connection testing

    Usage:
        connector = PostgreSQLConnector(config)
        with connector:
            result = connector.execute("SELECT 1")
            schemas = connector.get_schemas()
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize connector with configuration.

        Args:
            config: Connection configuration (host, port, database, user, password, etc.)
                    Supports a ``database_url`` or ``url`` key as a shorthand that is
                    parsed into individual components automatically.
        """
        self.config = self._normalize_config(config)
        self._connection: Any = None

    @staticmethod
    def _parse_database_url(url: str) -> dict[str, Any]:
        """Parse a database URL into individual connection components.

        Supports formats like:
            postgresql://user:password@host:port/database?sslmode=require
            postgresql+asyncpg://user:password@host:port/database
        """
        from urllib.parse import parse_qs, unquote, urlparse

        parsed = urlparse(url)
        result: dict[str, Any] = {}
        if parsed.hostname:
            result["host"] = parsed.hostname
        if parsed.port:
            result["port"] = parsed.port
        if parsed.path and parsed.path.strip("/"):
            result["database"] = parsed.path.strip("/")
        if parsed.username:
            result["user"] = unquote(parsed.username)
        if parsed.password:
            result["password"] = unquote(parsed.password)
        # Handle ssl from query params
        qs = parse_qs(parsed.query)
        if qs.get("ssl", [None])[0] == "true" or qs.get("sslmode", [None])[0] in (
            "require",
            "verify-ca",
            "verify-full",
        ):
            result["ssl"] = True
        return result

    @staticmethod
    def _normalize_config(config: dict[str, Any]) -> dict[str, Any]:
        """Normalize config key aliases to canonical names.

        If a ``database_url`` or ``url`` key is present, it is parsed first
        and individual keys can still override the parsed values.
        """
        normalized = dict(config)

        # Parse database_url / url if present
        url = normalized.pop("database_url", None) or normalized.pop("url", None)
        if url:
            from_url = BaseConnector._parse_database_url(url)
            # URL values are defaults; explicit keys take precedence
            for key, value in from_url.items():
                normalized.setdefault(key, value)

        aliases: dict[str, str] = {
            "username": "user",
            "hostname": "host",
            "dbname": "database",
            "db": "database",
        }
        for alias, canonical in aliases.items():
            if alias in normalized and canonical not in normalized:
                normalized[canonical] = normalized.pop(alias)
            elif alias in normalized and canonical in normalized:
                normalized.pop(alias)  # canonical takes precedence

        # Validate host against SSRF if present
        host = normalized.get("host")
        if host:
            from dq_platform.config import get_settings
            from dq_platform.core.network_validation import validate_host

            settings = get_settings()
            validate_host(host, allow_private=settings.allow_private_network_connections)

        return normalized

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database.

        Raises:
            ConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def execute(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters (dict or tuple).

        Returns:
            List of result rows as dictionaries.

        Raises:
            ExecutionError: If query execution fails.
        """
        pass

    @abstractmethod
    def execute_scalar(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a SQL query and return a single scalar value.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters.

        Returns:
            Single value from the first column of the first row.

        Raises:
            ExecutionError: If query execution fails.
        """
        pass

    async def execute_sql(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query asynchronously and return results.

        Default implementation runs the synchronous execute in a thread.
        Subclasses may override for native async support.

        Args:
            sql: SQL query to execute.
            params: Optional query parameters.

        Returns:
            List of result rows as dictionaries.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.execute, sql, params)

    async def connect_async(self) -> None:
        """Establish connection to the database asynchronously.

        Default implementation calls sync connect in a thread.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.connect)

    async def disconnect_async(self) -> None:
        """Close the database connection asynchronously.

        Default implementation calls sync disconnect in a thread.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.disconnect)

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is valid.

        Returns:
            True if connection is successful.

        Raises:
            ConnectionError: If connection test fails.
        """
        pass

    @abstractmethod
    def get_schemas(self) -> list[str]:
        """Get list of schemas in the database.

        Returns:
            List of schema names.
        """
        pass

    @abstractmethod
    def get_tables(self, schema: str) -> list[TableInfo]:
        """Get list of tables in a schema.

        Args:
            schema: Schema name.

        Returns:
            List of TableInfo objects.
        """
        pass

    @abstractmethod
    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get list of columns in a table.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            List of ColumnInfo objects.
        """
        pass

    def __enter__(self) -> "BaseConnector":
        """Context manager entry - connect to database."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - disconnect from database."""
        self.disconnect()

    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier for safe use in SQL.

        Override in subclasses for database-specific quoting.

        Args:
            identifier: Table, column, or schema name.

        Returns:
            Quoted identifier.
        """
        return f'"{identifier}"'

    # ─── Profile / sample / fingerprint helpers ──────────────────────────
    # Used by dq_platform.profilers for deterministic DQ check emission.
    # See profilers/profile_runner.py and profilers/sample_fetcher.py for
    # the orchestration. SQL is designed to look like generic column
    # profiling so customer DBAs can't reverse-engineer the check intent
    # from query logs.

    def build_profile_sql(
        self,
        schema: str,
        table: str,
        columns: list[dict[str, Any]],
    ) -> str:
        """Build a single SELECT that returns all per-column aggregates.

        Args:
            schema: schema name.
            table: table name.
            columns: list of {name, classification, logical_type}. When
                classification == "PII", min/max/min_len/max_len/distinct
                are skipped — only count + nulls are emitted.

        Returns:
            One SQL string. Result has opaque numeric aliases (c0_a1,
            c0_a2, ...) — no check-type identifiers in output column names.

        Override only if the dialect lacks a standard aggregate fn.
        """
        qschema = self.quote_identifier(schema)
        qtable = self.quote_identifier(table)
        select_terms: list[str] = ["COUNT(*) AS r_a0"]
        for idx, col in enumerate(columns):
            name = col["name"]
            qcol = self.quote_identifier(name)
            classification = col.get("classification")
            is_pii = classification == "PII"
            # MIN/MAX is undefined for boolean columns on most engines
            # (Postgres: "function min(boolean) does not exist"). Skip the
            # min/max aggregates for booleans — count + distinct still run.
            logical = (col.get("logical_type") or "").lower()
            is_bool = logical in ("boolean", "bool")

            # Position-prefixed aliases keep the result schema opaque.
            prefix = f"c{idx}"
            select_terms.append(f"COUNT({qcol}) AS {prefix}_a1")
            if not is_pii:
                select_terms.append(f"COUNT(DISTINCT {qcol}) AS {prefix}_a2")
                if not is_bool:
                    select_terms.append(f"MIN({qcol}) AS {prefix}_a3")
                    select_terms.append(f"MAX({qcol}) AS {prefix}_a4")
                # Length stats only meaningful for text — wrap in a
                # text cast so numeric/date cols don't error.
                cast = self._cast_text_expr(qcol)
                length = self._text_length_expr(cast)
                select_terms.append(f"MIN({length}) AS {prefix}_a5")
                select_terms.append(f"MAX({length}) AS {prefix}_a6")

        return f"SELECT {', '.join(select_terms)} FROM {qschema}.{qtable}"

    def _cast_text_expr(self, qcol: str) -> str:
        """Cast a column reference to text for length measurement.

        Override per dialect — MySQL needs CHAR, Oracle TO_CHAR, BigQuery
        STRING, SQL Server VARCHAR(MAX).
        """
        return f"CAST({qcol} AS VARCHAR)"

    def _text_length_expr(self, text_expr: str) -> str:
        """String-length function. ANSI default LENGTH; SQL Server uses LEN."""
        return f"LENGTH({text_expr})"

    def build_sample_sql(
        self,
        schema: str,
        table: str,
        columns: list[str],
        pk: str,
        n: int,
        seed: int = 1,
        modulus: int = 100_000,
    ) -> str:
        """Build a deterministic sample SQL on the primary-key column.

        Strategy: ORDER BY hash(pk, seed) LIMIT n. Returns exactly n rows
        (or all rows if table smaller). Same seed → same rows. Scales
        from 5-row dev tables to billion-row warehouses without tuning.

        Block-level TABLESAMPLE is avoided because it skews enum/codelist
        inference on clustered tables.

        Args:
            schema: schema name.
            table: table name.
            columns: columns to project. Caller is expected to omit PII.
            pk: column to hash on. Must have decent distribution.
            n: hard cap on rows returned. Capped at MAX_SAMPLE_ROWS by
                the sample_fetcher.
            seed: deterministic seed mixed into the hash.
            modulus: legacy parameter, kept for backwards-compat with
                early callers. Now affects only the bucket spread inside
                the hash expression and does NOT change row count.

        Returns:
            SQL string with seed inlined (integer-coerced for safety).
        """
        qschema = self.quote_identifier(schema)
        qtable = self.quote_identifier(table)
        qpk = self.quote_identifier(pk)
        projected = ", ".join(self.quote_identifier(c) for c in columns) if columns else "*"
        # Mix the seed into the hash bucket so different seeds → different
        # samples on the same table.
        hash_expr = self._hash_mod_expr(qpk, modulus)
        seeded_expr = f"({hash_expr} + {int(seed)})"
        return f"SELECT {projected} FROM {qschema}.{qtable} ORDER BY {seeded_expr} {self._limit_clause(n)}"

    def _limit_clause(self, n: int) -> str:
        """Row-cap clause appended after ORDER BY.

        ANSI default is `LIMIT n` (PG, MySQL, DuckDB, Redshift, BigQuery,
        Snowflake, Spark). Oracle and SQL Server override — they use the
        SQL:2008 `OFFSET/FETCH` form instead.
        """
        return f"LIMIT {int(n)}"

    def _hash_mod_expr(self, qcol: str, modulus: int) -> str:
        """Dialect-specific hash-mod expression on a column.

        Default uses ANSI MOD on a CRC-style hash. Each dialect overrides
        with its native fingerprint function:
            PG/Redshift    HASHTEXT
            BigQuery       FARM_FINGERPRINT
            Snowflake      HASH
            MySQL          CRC32
            SQL Server     CHECKSUM
            Oracle         ORA_HASH
            DuckDB         HASH
            Databricks     HASH
        """
        # ANSI fallback — works on most dialects via CAST + sum-of-codes.
        # Per-dialect overrides preferred for distribution quality.
        return f"MOD(ABS(LENGTH(CAST({qcol} AS VARCHAR))), {int(modulus)})"

    def get_schema_fingerprint(self, schema: str, table: str) -> str:
        """Hash of (column_name, data_type, is_nullable, ordinal_position).

        Used as cache key so sample data is invalidated when the table
        shape changes. Columns are sorted by name before hashing so a
        reordering doesn't trigger a false invalidation.
        """
        import hashlib

        cols = self.get_columns(schema, table)
        # Sort by name. Tuple shape locked: any new field added later
        # would not change the fingerprint of existing tables.
        sorted_cols = sorted(cols, key=lambda c: c.name)
        h = hashlib.sha256()
        for c in sorted_cols:
            h.update(c.name.encode())
            h.update(b"|")
            h.update(c.data_type.encode())
            h.update(b"|")
            h.update(b"1" if c.is_nullable else b"0")
            h.update(b"|")
        return h.hexdigest()
