# DQ Platform

A modern, API-first data quality monitoring platform for validating and monitoring data across multiple sources. Implements a DQOps-equivalent data quality system with ODPS 4.1 dimension scoring.

## Overview

DQ Platform provides automated data quality checks, anomaly detection, and incident management for data teams. Built as a standalone API service with a DQOps-style architecture (Sensors + Rules + Checks). Serves as the data quality engine for the [MLG (Minimum Lovable Governance)](https://github.com/your-org/MLG-Spec-Reader) platform, providing check execution, result storage, and ODPS dimension scoring via REST API.

## Key Features

- **171 DQOps-Style Checks** - Table and column-level validations using sensor + rule architecture
- **76+ Sensors** - Jinja2 SQL templates for measuring data characteristics
- **15 Rule Types** - Threshold, percentage, change detection, and comparison rules
- **Multi-Severity Thresholds** - warning/error/fatal thresholds per check
- **Check Modes** - Profiling, monitoring, and partitioned check execution
- **Multi-Source Support** - PostgreSQL, MySQL, BigQuery, Snowflake, SQL Server, Redshift, DuckDB, Oracle, Databricks
- **API-First Design** - Every operation available via REST API
- **Incident Management** - Group failures, track resolution, reduce alert fatigue
- **Webhook Notifications** - Real-time alerts on check failures and incident lifecycle events
- **ODPS 4.1 Dimension Scoring** - Maps check results to 8 standardized quality dimensions (accuracy, completeness, conformity, consistency, coverage, timeliness, validity, uniqueness)
- **Batch Execution** - Run multiple checks in a single API call
- **Cron Scheduling** - Schedule checks with cron expressions via Celery Beat
- **MLG Integration** - Serves as the DQ engine for the MLG governance platform

## Implemented Check Categories

| Category | Count | Description |
|----------|-------|-------------|
| **Volume** | 4 | row_count, row_count_change_1/7/30_days |
| **Schema** | 6 | column_count, column_exists, column_count_changed, column_list/order/types_changed |
| **Timeliness** | 4 | data_freshness, data_staleness, data_ingestion_delay, reload_lag |
| **Availability** | 1 | table_availability |
| **Nulls** | 5 | nulls_count/percent, not_nulls_count/percent, empty_column_found |
| **Uniqueness** | 6 | distinct/duplicate checks (column + table-level) |
| **Numeric** | 24 | min/max/sum/mean/median_in_range, number_below/above, stddev, variance, percentiles |
| **Text** | 14 | text length, empty/whitespace/regex checks |
| **Patterns** | 13 | email, UUID, IP4, IP6, phone, zipcode validation |
| **PII Detection** | 5 | contains_usa_phone/email/zipcode/ip4/ip6_percent |
| **Geographic** | 4 | invalid/valid latitude/longitude |
| **Boolean** | 2 | true/false percent |
| **DateTime** | 2 | future dates, date in range |
| **Referential** | 2 | foreign key not found/found_percent |
| **Custom SQL** | 10 | sql_condition_failed/passed on table/column, sql_aggregate, sql_invalid |
| **Change Detection** | 18 | nulls/distinct/mean/median/sum_change_1/7/30_days |
| **Cross-Table** | 6 | total_row_count/sum/min/max/average/not_null_count_match_percent |
| **Cross-Source** | 9 | row_count/column_count/sum/min/max/mean/not_null/null/distinct_count_match |
| **Anomaly Detection** | 10 | row_count/data_freshness/nulls_percent/distinct_count/distinct_percent/sum/mean/median/min/max_anomaly |

## Quick Start

### 1. Start the Full Stack (Docker)

```bash
# Clone the repository
git clone <repository-url>
cd dq-platform

# Set up environment
cp .env.example .env
# Edit .env and set ENCRYPTION_KEY (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Start all services (postgres, redis, api, worker, beat, migrate)
docker compose up -d

# Check all services are healthy
docker compose ps

# View logs
docker compose logs -f api
```

### 2. Create a Connection

```bash
# Create a connection to your database
curl -X POST http://localhost:8000/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d '{
    "name": "production-db",
    "connection_type": "postgresql",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "analytics",
      "user": "readonly",
      "password": "***"
    }
  }'

# Test the connection
curl -X POST http://localhost:8000/api/v1/connections/<connection-id>/test \
  -H "X-API-Key: test-key"
```

### 3. Create and Run a Data Quality Check

```bash
# Create a DQOps-style check with severity thresholds
curl -X POST http://localhost:8000/api/v1/checks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d '{
    "name": "email_null_check",
    "check_type": "nulls_percent",
    "check_mode": "monitoring",
    "time_scale": "daily",
    "connection_id": "<connection-uuid>",
    "target_schema": "public",
    "target_table": "users",
    "target_column": "email",
    "rule_parameters": {
      "warning": {"max_percent": 5.0},
      "error": {"max_percent": 10.0}
    }
  }'

# Run the check
curl -X POST http://localhost:8000/api/v1/checks/<check-uuid>/run \
  -H "X-API-Key: test-key"

# Check the results
curl http://localhost:8000/api/v1/results?check_id=<check-uuid> \
  -H "X-API-Key: test-key"
```

### 4. Preview Check (Dry Run)

```bash
# Test a check without saving it
curl -X POST http://localhost:8000/api/v1/checks/validate/preview \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d '{
    "connection_id": "<connection-uuid>",
    "check_type": "row_count",
    "target_table": "users",
    "rule_parameters": {"error": {"min_count": 100}}
  }'
```

### 5. Stop the Stack

```bash
# Stop all services
docker compose down

# Stop and remove volumes (WARNING: deletes all data)
docker compose down -v
```

## API Reference

All endpoints are under `/api/v1` and require `X-API-Key` authentication.

### Connections

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/connections` | Create connection |
| `GET` | `/connections` | List connections (filter by `connection_type`) |
| `GET` | `/connections/{id}` | Get connection |
| `PATCH` | `/connections/{id}` | Update connection |
| `DELETE` | `/connections/{id}` | Delete connection |
| `POST` | `/connections/{id}/test` | Test connection |
| `GET` | `/connections/{id}/schemas` | List schemas |
| `GET` | `/connections/{id}/schemas/{schema}/tables` | List tables |
| `GET` | `/connections/{id}/schemas/{schema}/tables/{table}/columns` | List columns |

### Checks

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/checks` | Create check |
| `GET` | `/checks` | List checks (filter by `connection_id`, `check_type`, `target_table`, `is_active`) |
| `GET` | `/checks/{id}` | Get check |
| `PATCH` | `/checks/{id}` | Update check |
| `DELETE` | `/checks/{id}` | Delete check |
| `POST` | `/checks/{id}/run` | Run check (async via Celery) |
| `POST` | `/checks/{id}/preview` | Preview check (sync, no persistence) |
| `POST` | `/checks/validate/preview` | Preview without saving |
| `POST` | `/checks/batch/run` | Run multiple checks (`check_ids` array) |
| `GET` | `/checks/types` | List all 171 check types (filter by `category`) |
| `GET` | `/checks/categories` | List 21 check categories |
| `GET` | `/checks/modes` | List check modes (profiling, monitoring, partitioned) |
| `GET` | `/checks/time-scales` | List time scales (daily, monthly) |

### Results

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/results` | Query results (filter by `check_id`, `connection_id`, `passed`, `from_date`, `to_date`) |
| `GET` | `/results/summary` | Aggregated pass rate, execution count, avg execution time |

### Incidents

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/incidents` | List incidents (filter by `check_id`, `status`, `severity`) |
| `GET` | `/incidents/{id}` | Get incident details |
| `PATCH` | `/incidents/{id}` | Update status (acknowledge/resolve with `by` and `notes`) |
| `DELETE` | `/incidents/{id}` | Delete incident |

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/jobs` | List background jobs (filter by `check_id`, `status`) |
| `GET` | `/jobs/{id}` | Get job status |

### Schedules

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/schedules` | Create cron schedule |
| `GET` | `/schedules` | List schedules |
| `PATCH` | `/schedules/{id}` | Update schedule |
| `DELETE` | `/schedules/{id}` | Delete schedule |

### Notifications

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/notifications/channels` | Create webhook channel |
| `GET` | `/notifications/channels` | List channels |
| `PATCH` | `/notifications/channels/{id}` | Update channel |
| `DELETE` | `/notifications/channels/{id}` | Delete channel |
| `POST` | `/notifications/channels/{id}/test` | Send test webhook |

### ODPS Dimensions

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/dimensions/scores` | Get scores for all 8 ODPS dimensions (filter by `connection_id`, `check_ids`, `from_date`, `to_date`) |
| `GET` | `/dimensions/mapping` | Get static category-to-dimension mapping |
| `GET` | `/dimensions/{dimension}/trend` | Get daily score trend for a dimension (`days` param, default 30) |
| `GET` | `/dimensions/{dimension}/checks` | Get all checks contributing to a dimension |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Deep health check (DB + Redis connectivity) |

## ODPS 4.1 Dimension Scoring

Check results are automatically mapped to 8 ODPS quality dimensions using a category-to-dimension mapping:

| ODPS Dimension | Check Categories |
|----------------|-----------------|
| **Accuracy** | numeric, statistical, anomaly |
| **Completeness** | nulls |
| **Conformity** | text, patterns, accepted_values, pii |
| **Consistency** | schema, referential, comparison |
| **Coverage** | volume, availability |
| **Timeliness** | timeliness, change, change_detection |
| **Validity** | boolean, datetime, geographic, datatype |
| **Uniqueness** | uniqueness |

Scores are computed using severity-weighted penalties: `passed=0.0`, `warning=1.0`, `error=2.5`, `fatal=5.0`. Scores range from 0-100 with traffic-light status (green >= 80, amber >= 60, red < 60).

## MLG Platform Integration

DQ Platform serves as the data quality engine for the MLG governance platform. MLG connects via `dqops-client.ts` and proxies requests through `/api/dqops/*` routes.

**Integration pattern:**
```
MLG Data Product
  ├── dataQualityId (FK) ──► DQ Profile (defines quality standards/targets)
  └── dqopsConnectionId ──► DQ Platform Connection
        ├── Checks defined and executed in DQ Platform
        ├── Results queried via /api/v1/results
        ├── Incidents tracked via /api/v1/incidents
        └── ODPS dimension scores via /api/v1/dimensions/scores
```

**Division of responsibility:**
- **MLG** defines quality standards (DQ Profiles with ODPS dimension targets), links data products to DQ Platform connections, and displays quality metrics in product specs
- **DQ Platform** implements checks, executes against data sources, stores time-series results, computes ODPS dimension scores, and manages incidents

**Configuration:** MLG connects using `DQOPS_API_URL` and `DQOPS_API_KEY` environment variables.

## Documentation

- [Architecture](docs/architecture.md) - System design, components, and deployment
- [API Specification](docs/api-spec.md) - REST API endpoints and examples
- [DQOps Implementation](docs/DQOPS_IMPLEMENTATION.md) - Implementation status and what's available
- [DQOps Features](docs/DQOPS_FEATURES.md) - Complete DQOps feature specification
- [Features Roadmap](docs/features.md) - Feature roadmap and priorities
- [Database Schema](docs/database-schema.md) - PostgreSQL schema and queries
- [Key Differentiators](docs/differentiators.md) - How we differ from DQOps

## Testing

```bash
# Start the test stack
docker compose up -d

# Run tests inside the container
docker compose exec api pytest tests/ -v

# Or run locally (requires local Python environment)
pytest tests/test_dqops_checks.py -v
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection URL | `postgresql+asyncpg://postgres:postgres@localhost:5432/dq_platform` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379/0` |
| `ENCRYPTION_KEY` | Fernet key for credential encryption | (required) |
| `API_KEY_HEADER` | Header name for API key auth | `X-API-Key` |
| `VALID_API_KEYS` | Comma-separated list of accepted API keys. Empty = dev mode (any non-empty key accepted) | `[]` |
| `RATE_LIMIT_DEFAULT` | Global rate limit for all endpoints | `100/minute` |
| `CORS_ALLOWED_ORIGINS` | Comma-separated list of allowed CORS origins | `["*"]` |
| `DEBUG` | Enable debug mode | `False` |

## Production Deployment

### API Key Authentication

In development, `VALID_API_KEYS` is empty and any non-empty `X-API-Key` header is accepted. For production, set explicit keys:

```env
# On the DQ Platform VPS
VALID_API_KEYS=my-secret-key-1,my-secret-key-2
ENCRYPTION_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
CORS_ALLOWED_ORIGINS=https://your-mlg-domain.com
RATE_LIMIT_DEFAULT=100/minute
```

Requests without a valid key receive `401` (missing) or `403` (invalid). Rejected keys are logged for auditing.

### Connecting MLG to a Remote DQ Platform

On the MLG server, set these environment variables to point at the DQ Platform VPS:

```env
DQOPS_API_URL=http://<vps-ip>:8000
DQOPS_API_KEY=my-secret-key-1
```

MLG's `dqops-client.ts` automatically sends the key as an `X-API-Key` header on every request. No code changes are needed on the MLG side.

### Checklist

1. Generate and set `ENCRYPTION_KEY` (never reuse across environments)
2. Set `VALID_API_KEYS` with at least one strong key
3. Set `CORS_ALLOWED_ORIGINS` to your MLG domain (not `*`)
4. Ensure the VPS firewall allows inbound traffic on the API port (default `8000`)
5. Set `DQOPS_API_URL` and `DQOPS_API_KEY` on the MLG server
6. Verify: `curl -H "X-API-Key: my-secret-key-1" http://<vps-ip>:8000/api/v1/checks` should return `200`

## Tech Stack

- **API Framework:** FastAPI (Python 3.11+)
- **Database:** PostgreSQL (config + metadata)
- **Results Store:** TimescaleDB hypertables (time-series)
- **Job Queue:** Celery + Redis
- **SQL Templating:** Jinja2
- **Migrations:** Alembic

## Project Status

**Phase 1-12 Complete:** 171 DQOps-style checks implemented across 21 categories with 100% DQOps coverage.

See [DQOPS_IMPLEMENTATION.md](docs/DQOPS_IMPLEMENTATION.md) for detailed status.

## License

TBD
