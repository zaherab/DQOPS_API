# DQ Platform

A modern, API-first data quality monitoring platform for validating and monitoring data across multiple sources. Implements a DQOps-equivalent data quality system.

## Overview

DQ Platform provides automated data quality checks, anomaly detection, and incident management for data teams. Built as a standalone API service with a DQOps-style architecture (Sensors + Rules + Checks).

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
| `DEBUG` | Enable debug mode | `False` |

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
