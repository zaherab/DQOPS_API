# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start for Development

```bash
# Start the full stack
docker compose up -d

# Check service status
docker compose ps

# View API logs
docker compose logs -f api

# Run tests
docker compose exec api pytest tests/ -v

# Access API docs: http://localhost:8000/docs
```

## Project Overview

DQ Platform is an API-first data quality monitoring platform for validating and monitoring data across multiple sources. It implements a DQOps-equivalent data quality system.

**Status:** Production Ready - 171 DQOps-style checks implemented (100% DQOps coverage)

## Tech Stack

- **API Framework:** FastAPI (Python)
- **Database:** PostgreSQL (config + metadata)
- **Results Store:** TimescaleDB or ClickHouse (time-series)
- **Job Queue:** Celery + Redis
- **SQL Templating:** Jinja2

## Architecture

The system follows a modular, API-first architecture:

```
API Layer (FastAPI) → Services (Connection, Check, Results) → Core Domain → Storage
```

**Core Components:**
- **Connection Service** - CRUD for data source connections, metadata discovery
- **Check Service** - Define/execute data quality checks using Sensors (SQL/Jinja2) + Rules (Python)
- **Results Service** - Store/query check execution results (time-series)
- **Job Queue** - Async check execution via Celery workers
- **Scheduler** - Celery Beat polls for due cron schedules every 60s, dispatches check jobs
- **Incident Management** - Group failures, track resolution
- **Notification Service** - Webhook notifications on incident lifecycle events (open/resolve)

## DQOps-Style Check System

The platform implements a DQOps-equivalent check system with three core concepts:

### Sensors (SQL Templates)
Sensors are Jinja2 SQL templates that measure data characteristics and return a single numeric value.

```python
from dq_platform.checks.sensors import SensorType, get_sensor

sensor = get_sensor(SensorType.NULLS_PERCENT)
sql = sensor.render({
    "schema_name": "public",
    "table_name": "users",
    "column_name": "email"
})
```

**76+ Sensors Available:**
- **Volume:** row_count, row_count_change_*_days
- **Schema:** column_count, column_exists
- **Timeliness:** data_freshness, data_staleness
- **Nulls:** nulls_count, nulls_percent, not_nulls_count, not_nulls_percent
- **Uniqueness:** distinct_count, distinct_percent, duplicate_count, duplicate_percent, duplicate_record_count
- **Numeric:** min_value, max_value, sum_value, mean_value, median_value, stddev_*, variance_*, percentile
- **Text:** text_min_length, text_max_length, text_mean_length, empty_text_count, whitespace_text_count, regex_match/not_match
- **Geographic:** invalid_latitude_count, invalid_longitude_count
- **Boolean:** true_count, true_percent, false_count, false_percent
- **DateTime:** future_date_count, future_date_percent, date_in_range_count, date_in_range_percent
- **Pattern/Format:** invalid_email, invalid_uuid, invalid_ip4, invalid_ip6, invalid_phone, invalid_zipcode
- **Referential:** foreign_key_not_found_count, foreign_key_found_percent
- **Custom SQL:** sql_condition_failed_count, sql_aggregate_value, sql_condition_on_column, sql_invalid_*
- **Schema Detection:** column_list_hash, column_types_hash

### Rules (Python Functions)
Rules evaluate sensor output against thresholds and determine severity.

```python
from dq_platform.checks.rules import RuleType, evaluate_rule, Severity

result = evaluate_rule(
    RuleType.MAX_PERCENT,
    sensor_value=15.0,
    params={"max_percent": 10.0, "severity": "error"}
)
# result.severity: Severity.ERROR
# result.passed: False
```

**15 Rule Types:**
- **Threshold:** min_value, max_value, min_max_value
- **Percentage:** min_percent, max_percent, min_max_percent
- **Change Detection:** max_change_percent
- **Count:** min_count, max_count, min_max_count
- **Comparison:** equal_to, not_equal_to
- **Boolean:** is_true, is_false
- **Anomaly Detection:** anomaly_percentile

### Checks (Sensor + Rule)
Checks combine sensors and rules into reusable data quality validations.

```python
from dq_platform.checks import DQOpsCheckType, run_dqops_check

result = await run_dqops_check(
    check_type=DQOpsCheckType.NULLS_PERCENT,
    connection_config={"type": "postgresql", "host": "...", ...},
    schema_name="public",
    table_name="users",
    column_name="email",
    rule_params={"max_percent": 5.0, "severity": "error"}
)
```

**171 Checks Implemented:**
- **volume:** 4 checks (row_count, row_count_change_1/7/30_days)
- **schema:** 6 checks (column_count, column_exists, column_count_changed, column_list/order/types_changed)
- **timeliness:** 4 checks (data_freshness, data_staleness, data_ingestion_delay, reload_lag)
- **availability:** 1 check (table_availability)
- **nulls:** 5 checks (nulls_count/percent, not_nulls_count/percent, empty_column_found)
- **uniqueness:** 6 checks (distinct/duplicate_count/percent, duplicate_record_count/percent)
- **numeric:** 24 checks (min/max/sum/mean/median_in_range, number_below/above, stddev, variance, percentiles)
- **text:** 14 checks (text_min/max/mean_length, text_length_below/above_percent, word_count, empty/whitespace)
- **whitespace:** 6 checks (null_placeholder, text_surrounded_by_whitespace, regex matching)
- **patterns:** 13 checks (regex, email, UUID, IP4, IP6, phone, zipcode)
- **date_patterns:** 10 checks (date pattern matching, parsable_to_boolean/integer/float/date, detected_datatype)
- **accepted_values:** 7 checks (text/number_found_in_set, expected_values, country/currency_code)
- **geographic:** 4 checks (invalid/valid latitude/longitude)
- **boolean:** 2 checks (true/false_percent)
- **datetime:** 2 checks (date_values_in_future_percent, date_in_range_percent)
- **pii_detection:** 5 checks (contains_usa_phone/email/zipcode/ip4/ip6_percent)
- **change_detection:** 18 checks (nulls/distinct/mean/median/sum_change_1/7/30_days)
- **cross_table:** 6 checks (total_row_count/sum/min/max/average/not_null_count_match_percent)
- **referential:** 2 checks (foreign_key_not_found/found_percent)
- **custom_sql:** 10 checks (sql_condition_failed/passed on table/column, sql_aggregate, sql_invalid, import_custom)
- **anomaly:** 10 checks (row_count/data_freshness/nulls_percent/distinct_count/distinct_percent/sum/mean/median/min/max_anomaly)
- **comparison:** 9 checks (row_count/column_count/sum/min/max/mean/not_null_count/null_count/distinct_count_match)

## Check Modes

DQOps supports three check modes:

| Mode | Purpose | Storage |
|------|---------|---------|
| **Profiling** | Initial data assessment | 1 result/month (overwrites) |
| **Monitoring** | Continuous quality tracking | 1 result/day or /month |
| **Partitioned** | Per-partition analysis | 1 result/partition/day |

## Multi-Severity Thresholds

Checks support warning/error/fatal thresholds:

```json
{
  "rule_parameters": {
    "warning": {"max_percent": 5.0},
    "error": {"max_percent": 10.0},
    "fatal": {"max_percent": 20.0}
  }
}
```

## API Endpoints

### Checks
- `POST /api/v1/checks` - Create a check
- `GET /api/v1/checks` - List checks
- `GET /api/v1/checks/{id}` - Get a check
- `PATCH /api/v1/checks/{id}` - Update a check
- `DELETE /api/v1/checks/{id}` - Delete a check
- `POST /api/v1/checks/{id}/run` - Run a check (async)
- `POST /api/v1/checks/{id}/preview` - Preview a check (sync)
- `POST /api/v1/checks/validate/preview` - Preview check config without saving

### Check Types & Metadata
- `GET /api/v1/checks/types` - List available check types
- `GET /api/v1/checks/categories` - List check categories
- `GET /api/v1/checks/modes` - List check modes
- `GET /api/v1/checks/time-scales` - List time scales

### Notifications
- `POST /api/v1/notifications/channels` - Create notification channel
- `GET /api/v1/notifications/channels` - List channels
- `GET /api/v1/notifications/channels/{id}` - Get channel
- `PATCH /api/v1/notifications/channels/{id}` - Update channel
- `DELETE /api/v1/notifications/channels/{id}` - Delete channel
- `POST /api/v1/notifications/channels/{id}/test` - Send test webhook

## Connectors

**9 database connectors** supported:
- PostgreSQL (`psycopg2`)
- MySQL (`pymysql`)
- SQL Server (`pyodbc`)
- BigQuery (`google-cloud-bigquery`)
- Snowflake (`snowflake-connector-python`)
- Redshift (`redshift-connector`)
- DuckDB (`duckdb`)
- Oracle (`oracledb`, thin mode)
- Databricks (`databricks-sql-connector`)

## Key Documentation

- `docs/api-spec.md` - REST API endpoints and examples
- `docs/architecture.md` - System design, components, and deployment
- `docs/DQOPS_FEATURES.md` - Complete DQOps feature reference (specification)
- `docs/DQOPS_IMPLEMENTATION.md` - Implementation status and what's available
- `docs/features.md` - Feature roadmap
- `docs/database-schema.md` - PostgreSQL schema and queries
- `docs/differentiators.md` - How we differ from DQOps

## Testing

### Unit Tests (no database required)
```bash
pytest tests/test_dqops_checks.py -v
pytest tests/test_gx_registry.py -v
```

### Integration Tests (requires full Docker stack)

**230+ parametrized tests** covering all 171 check types with both positive and negative cases:

```bash
# Start the full stack
docker compose up -d

# Run all integration tests inside the container
docker compose exec api pytest tests/integration/ -v

# Run specific category
docker compose exec api pytest tests/integration/test_api_checks.py::TestVolumeChecks -v
docker compose exec api pytest tests/integration/test_api_checks.py::TestPatternChecks -v

# With coverage
docker compose exec api pytest tests/integration/ -v --cov=dq_platform
```

### Test Structure
```
tests/
├── conftest.py                      # Unit test fixtures (in-memory SQLite)
├── test_dqops_checks.py             # Unit tests for sensors/rules
├── test_gx_registry.py              # GX expectation registry tests
├── integration/
│   ├── conftest.py                  # Integration fixtures (PostgreSQL)
│   └── test_api_checks.py           # 188+ API integration tests
└── fixtures/
    └── setup_test_data.sql          # Test data for integration tests
```

## Extensibility Patterns

### Adding a Connector
1. Implement `BaseConnector` interface
2. Register in connector factory
3. Add Pydantic config schema

### Adding a Sensor
1. Create sensor definition in `src/dq_platform/checks/sensors.py`
2. Add SQL template with Jinja2
3. Register in `SENSOR_REGISTRY`

### Adding a Rule
1. Create rule function in `src/dq_platform/checks/rules.py`
2. Implement rule logic returning `RuleResult`
3. Register in `RULE_REGISTRY`

### Adding a Check Type
1. Add to `DQOpsCheckType` enum
2. Create `DQOpsCheck` definition with sensor_type and rule_type
3. Register in `CHECK_REGISTRY`
4. Add to CheckType model enum
5. Create migration for enum value
