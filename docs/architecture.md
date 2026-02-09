# Architecture

## System Overview

DQ Platform follows a modular, API-first architecture designed for horizontal scalability and easy deployment.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Gateway (REST)                             │
│                         FastAPI with OpenAPI spec                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                              ▼
┌───────────────┐            ┌─────────────────┐            ┌─────────────────┐
│  Connection   │            │     Check       │            │    Results      │
│   Service     │            │    Service      │            │    Service      │
│               │            │                 │            │                 │
│ - CRUD conns  │            │ - Check CRUD    │            │ - Query results │
│ - Test conn   │            │ - Execute jobs  │            │ - Aggregations  │
│ - Metadata    │            │ - Schedule mgmt │            │ - Trends        │
└───────┬───────┘            └────────┬────────┘            └────────┬────────┘
        │                             │                              │
        ▼                             ▼                              ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           Core Domain Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │
│  │ Connectors  │  │   Sensors   │  │    Rules    │  │    Incidents    │   │
│  │  (Adapters) │  │(SQL Jinja2) │  │  (Python)   │  │   (Grouping)    │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
        │                             │                              │
        ▼                             ▼                              ▼
┌───────────────┐            ┌─────────────────┐            ┌─────────────────┐
│   Database    │            │   Job Queue     │            │  Time-Series    │
│  PostgreSQL   │            │  Celery/Redis   │            │    Storage      │
│               │            │                 │            │                 │
│ - Connections │            │ - Async exec    │            │ - Check results │
│ - Check defs  │            │ - Retries       │            │ - Statistics    │
│ - Schedules   │            │ - Priority      │            │ - Trends        │
│ - Incidents   │            │                 │            │                 │
└───────────────┘            └─────────────────┘            └─────────────────┘
```

## Core Components

### 1. API Layer (FastAPI)

The REST API is the primary interface for all operations. Key characteristics:

- **Stateless** - No session state; all context in requests
- **OpenAPI 3.1** - Auto-generated documentation at `/docs`
- **Authentication** - API key-based via `X-API-Key` header
- **Base Path** - All endpoints under `/api/v1`

### 2. Connection Service

Manages data source connections and metadata discovery.

**Responsibilities:**
- CRUD operations for connection configurations
- Connection testing and health checks
- Schema/table/column metadata discovery
- Credential encryption at rest

**Supported Connectors:**
| Database | Driver | Status |
|----------|--------|--------|
| PostgreSQL | psycopg2 | ✅ Ready |
| MySQL | pymysql | ✅ Ready |
| SQL Server | pyodbc | ✅ Ready |
| BigQuery | google-cloud-bigquery | ✅ Ready |
| Snowflake | snowflake-connector-python | ✅ Ready |
| Redshift | redshift-connector | ✅ Ready |
| Databricks | databricks-sql-connector | ✅ Ready |
| DuckDB | duckdb | ✅ Ready |
| Oracle | oracledb | ✅ Ready |

### 3. Check Service

Defines and executes data quality checks.

**Components:**
- **Sensors** - SQL templates (Jinja2) that measure data characteristics
- **Rules** - Python functions that evaluate sensor output against thresholds
- **Executor** - Runs sensor SQL, applies rules, records results

**Check Execution Flow:**
```
1. Load check definition from database
2. Render SQL template with Jinja2 (table, column, params, partition)
3. Execute SQL against target data source
4. Parse sensor result (numeric value)
5. Apply rule logic (threshold, anomaly, etc.)
6. Record result (pass/fail, value, severity)
7. Update incident if failure detected
```

### 4. Results Service

Stores and queries check execution results.

**Storage Strategy:**
- **PostgreSQL** - Check definitions, schedules, incidents (relational)
- **TimescaleDB** - Check results time-series (hypertable partitioned by time)

**Query Patterns:**
- Latest result per check
- Results over time window (trend analysis)
- Aggregated pass/fail rates by table, schema, connection
- Anomaly detection (rolling averages, seasonal comparison)

### 5. Job Queue (Celery + Redis)

Handles asynchronous check execution and scheduling.

**Job Types:**
- `execute_check` - Run a single check
- `execute_check_batch` - Run multiple checks
- `sync_metadata` - Refresh schema metadata from source

**Scheduling:**
- Celery Beat polls for due schedules
- Cron expressions for flexible timing
- Per-check schedules

### 6. Incident Management

Groups related check failures to reduce alert noise.

**Incident Lifecycle:**
```
Check Fails → Find/Create Incident → Increment Count → Notify (if new)
                     │
                     ▼
              [OPEN] → [ACKNOWLEDGED] → [RESOLVED]
                          │                  │
                          └──────────────────┘
                           (auto-resolve on pass)
```

**Status Values:**
- `open` - New incident detected
- `acknowledged` - Confirmed, being investigated
- `resolved` - Issue fixed

## Data Flow

### Check Execution

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  API     │───▶│  Queue   │───▶│ Executor │───▶│  Source  │
│ Request  │    │  (Redis) │    │ (Worker) │    │    DB    │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
                                      │
                                      ▼
                               ┌──────────┐
                               │  Results │
                               │    DB    │
                               └──────────┘
```

### Scheduled Execution

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│Scheduler │───▶│  Queue   │───▶│ Executor │
│ (Beat)   │    │  (Redis) │    │ (Worker) │
└──────────┘    └──────────┘    └──────────┘
```

## Deployment

### Docker Compose (Development/Production)

The `docker-compose.yml` includes all services needed to run the full stack:

```yaml
services:
  postgres:
    image: timescale/timescaledb:latest-pg16
    container_name: dq-platform-postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: dq_platform
    ports:
      - "5433:5432"  # External port 5433 for local dev tools
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: dq-platform-redis
    ports:
      - "6380:6379"  # External port 6380 for local dev tools
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  migrate:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dq-platform-migrate
    command: alembic upgrade head
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform
      REDIS_URL: redis://redis:6379/0
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/0
      ENCRYPTION_KEY: ${ENCRYPTION_KEY}
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

  api:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dq-platform-api
    command: uvicorn dq_platform.main:app --host 0.0.0.0 --port 8000 --workers 4
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform
      REDIS_URL: redis://redis:6379/0
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/0
      ENCRYPTION_KEY: ${ENCRYPTION_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5

  worker:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dq-platform-worker
    command: celery -A dq_platform.workers.celery_app worker --loglevel=info
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform
      REDIS_URL: redis://redis:6379/0
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/0
      ENCRYPTION_KEY: ${ENCRYPTION_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully

  beat:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dq-platform-beat
    command: celery -A dq_platform.workers.celery_app beat --loglevel=info
    environment:
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@postgres:5432/dq_platform
      REDIS_URL: redis://redis:6379/0
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/0
      ENCRYPTION_KEY: ${ENCRYPTION_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully

volumes:
  postgres_data:
  redis_data:
```

**Key Design Decisions:**
- **Single Dockerfile** - Multi-stage build for all Python services (api/worker/beat/migrate)
- **Health Checks** - Postgres, Redis, and API all have health checks
- **Service Dependencies** - api/worker/beat wait for migrate to complete
- **External Ports** - Postgres on 5433, Redis on 6380 to avoid conflicts with local installs
- **Internal Network** - Services communicate using standard ports (5432, 6379)

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABASE_URL` | PostgreSQL connection URL | Yes | - |
| `REDIS_URL` | Redis connection URL | Yes | - |
| `ENCRYPTION_KEY` | Fernet key for credential encryption | Yes | - |
| `API_KEY_HEADER` | Header name for API key auth | No | `X-API-Key` |
| `DEBUG` | Enable debug mode | No | `False` |
| `CELERY_BROKER_URL` | Celery broker URL | No | `REDIS_URL` |
| `CELERY_RESULT_BACKEND` | Celery result backend | No | `REDIS_URL` |

### Kubernetes (Production)

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dq-platform-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: dq-platform-api
  template:
    metadata:
      labels:
        app: dq-platform-api
    spec:
      containers:
      - name: api
        image: dq-platform:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        - name: ENCRYPTION_KEY
          valueFrom:
            secretKeyRef:
              name: encryption-key
              key: key
        - name: REDIS_URL
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: redis_url
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dq-platform-worker
spec:
  replicas: 5
  selector:
    matchLabels:
      app: dq-platform-worker
  template:
    metadata:
      labels:
        app: dq-platform-worker
    spec:
      containers:
      - name: worker
        image: dq-platform:latest
        command: ["celery", "-A", "dq_platform.workers.celery_app", "worker", "--loglevel=info"]
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        - name: REDIS_URL
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: redis_url
```

## Security Considerations

### Credential Storage
- Connection passwords encrypted at rest (Fernet/AES-256)
- Encryption key from environment variable or secrets manager
- Never logged or returned in API responses

### API Authentication
- API keys with workspace scoping
- Keys hashed in database (bcrypt)

### Network Security
- Connections to data sources over TLS where supported
- Internal services communicate over private network
- No direct database access from public internet

## Extensibility

### Adding a New Connector

1. Implement `BaseConnector` interface:
```python
from dq_platform.connectors.base import BaseConnector

class MyConnector(BaseConnector):
    def connect(self) -> Connection: ...
    def execute(self, sql: str) -> Result: ...
    def get_schemas(self) -> list[str]: ...
    def get_tables(self, schema: str) -> list[dict]: ...
```

2. Register in connector factory:
```python
from dq_platform.connectors.factory import ConnectorFactory
from dq_platform.models.connection import ConnectionType

ConnectorFactory.register_connector(ConnectionType.MYDB, MyConnector)
```

### Adding a New Sensor

1. Create sensor definition in `src/dq_platform/checks/sensors.py`:
```python
class SensorType(str, Enum):
    MY_METRIC = "my_metric"

SENSOR_TEMPLATES: dict[SensorType, str] = {
    SensorType.MY_METRIC: """
        SELECT COUNT(*) FROM {{ schema_name }}.{{ table_name }}
        WHERE {{ column_name }} > {{ threshold }}
    """,
}
```

2. Register in `SENSOR_REGISTRY`

### Adding a New Rule

1. Create rule function in `src/dq_platform/checks/rules.py`:
```python
def evaluate_my_rule(value: float, params: dict) -> RuleResult:
    threshold = params.get("threshold", 0)
    passed = value <= threshold
    return RuleResult(
        passed=passed,
        severity=Severity.ERROR if not passed else None,
        message=f"Value {value} exceeds threshold {threshold}" if not passed else None
    )
```

2. Register in `RULE_REGISTRY`

### Adding a New Check Type

1. Add to `DQOpsCheckType` enum in `src/dq_platform/checks/dqops_checks.py`
2. Create `DQOpsCheck` definition with `sensor_type` and `rule_type`
3. Register in `CHECK_REGISTRY`
4. Add to CheckType model enum in `src/dq_platform/models/check.py`
5. Create migration for enum value
