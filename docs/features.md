# Features Roadmap

## Phase 1: Essential Foundation

### Data Source Connectors

Start with the most common databases to cover majority of use cases.

| Connector | Status | Notes |
|-----------|--------|-------|
| PostgreSQL | ✅ Ready | Most common, covers Aurora, RDS, Cloud SQL |
| MySQL | ✅ Ready | Covers MariaDB, Aurora MySQL |
| SQL Server | ✅ Ready | Enterprise standard |
| BigQuery | ✅ Ready | GCP analytics |
| Snowflake | ✅ Ready | Cloud data warehouse leader |
| Redshift | ✅ Ready | AWS analytics |
| Databricks | ✅ Ready | Unity Catalog, Delta Lake |
| DuckDB | ✅ Ready | Embedded analytics |
| Oracle | ✅ Ready | Enterprise |

**Connector Features:**
- Connection configuration with encrypted credentials
- Connection testing and health checks
- Schema/table/column metadata discovery
- Connection pooling

### Table-Level Checks

| Check Type | Priority | Description |
|------------|----------|-------------|
| `row_count` | P0 | Current row count |
| `row_count_min` | P0 | Row count >= threshold |
| `row_count_max` | P0 | Row count <= threshold |
| `row_count_change` | P0 | Percent change from previous run |
| `row_count_anomaly` | P1 | Anomaly detection on row count |
| `schema_column_count` | P0 | Number of columns matches expected |
| `schema_column_exists` | P0 | Specific column exists |
| `schema_column_type` | P1 | Column has expected data type |
| `table_availability` | P0 | Table is queryable |
| `data_freshness` | P0 | Max timestamp within threshold |
| `duplicate_row_count` | P1 | Count of duplicate rows |

### Column-Level Checks

| Check Type | Priority | Description |
|------------|----------|-------------|
| `null_count` | P0 | Count of NULL values |
| `null_percent` | P0 | Percentage of NULL values |
| `not_null` | P0 | No NULL values allowed |
| `distinct_count` | P0 | Count of unique values |
| `duplicate_count` | P0 | Count of duplicate values |
| `unique` | P0 | All values must be unique |
| `min_value` | P1 | Minimum numeric value |
| `max_value` | P1 | Maximum numeric value |
| `mean_value` | P1 | Average value |
| `sum_value` | P1 | Sum of values |
| `min_length` | P1 | Minimum string length |
| `max_length` | P1 | Maximum string length |
| `regex_match_percent` | P1 | Percentage matching pattern |
| `value_in_set` | P1 | All values in allowed list |
| `value_in_range` | P1 | All values within min/max |
| `custom_sql` | P0 | User-defined SQL validation |

### Check Execution Engine

- Jinja2-templated SQL queries
- Async job execution with status tracking
- Job queue with Redis/Celery
- Retry logic with exponential backoff

### Results Storage

- Time-series storage for check results
- Historical trend queries
- Result retention policies
- Export to CSV/JSON

---

## Phase 2: Intelligence & Alerting

### Rule Engine

| Rule Type | Description |
|-----------|-------------|
| `threshold` | Simple min/max comparison |
| `percent_threshold` | Percentage-based limits |
| `change_threshold` | Change from previous value |
| `anomaly_rolling` | Deviation from rolling average |
| `anomaly_seasonal` | Day-over-day, week-over-week comparison |

### Incident Management

- Group related check failures into incidents
- Severity levels: `warning`, `error`, `fatal`
- Status workflow: `open` → `acknowledged` → `resolved`
- Auto-resolve when check passes
- Incident history and audit trail

### Notifications

| Channel | Priority | Notes |
|---------|----------|-------|
| Webhooks | P0 | Generic HTTP POST for any integration |
| Slack | P1 | Channel-based alerts |
| Email | P1 | SMTP-based notifications |
| PagerDuty | P2 | On-call escalation |

### Built-in Scheduling

- Cron expression support
- Per-check schedules
- Per-connection batch schedules
- Schedule enable/disable
- Next run preview

---

## Phase 3: Advanced Features

### Auto-Profiling & Rule Mining

- Automatic statistics collection on tables
- Suggest check configurations based on data patterns
- Detect common data types (email, phone, UUID, etc.)
- Recommend thresholds from historical data

### Partitioned/Incremental Checks

- DATE-based partition support
- Only scan new partitions
- Partition-aware freshness checks
- Reduced query costs on large tables

### Cross-Source Comparison

- Compare row counts across databases
- Column value reconciliation
- Schema comparison between sources
- Data migration validation

### MLG Platform Integration

- Webhook to push results to MLG
- DQ scores feed into Data Product health
- Link checks to Data Product IDs
- Shared connection configurations

---

## Phase Status

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1: Core Checks | ✅ Complete | Volume, schema, nulls, uniqueness, patterns |
| Phase 2: Advanced Checks | ✅ Complete | PII detection, referential integrity, geographic |
| Phase 3: Intelligence | ✅ Complete | Anomaly detection, change detection |
| Phase 4: Cross-Source | ✅ Complete | Cross-source comparisons |
| Phase 5-12: Extended | ✅ Complete | All 171 checks implemented |

---

## Check Categories Reference

### Table-Level Categories

| Category | Description | Example Checks |
|----------|-------------|----------------|
| **Volume** | Row count monitoring | row_count, row_count_change |
| **Schema** | Structure validation | column_count, column_exists |
| **Timeliness** | Data freshness | data_freshness, ingestion_delay |
| **Availability** | Accessibility | table_available, query_timeout |
| **Uniqueness** | Duplicate detection | duplicate_row_count |
| **Custom SQL** | User-defined | custom_sql_check |

### Column-Level Categories

| Category | Description | Example Checks |
|----------|-------------|----------------|
| **Nulls** | NULL value monitoring | null_count, null_percent |
| **Uniqueness** | Distinct value analysis | distinct_count, unique |
| **Numeric** | Number statistics | min, max, mean, sum |
| **Text** | String validation | length, pattern, whitespace |
| **Datetime** | Date/time validation | date_range, format |
| **Integrity** | Referential checks | foreign_key_match |
| **Custom SQL** | User-defined | custom_sql_column |

---

## Non-Goals (Out of Scope)

To keep the platform focused, these features are explicitly out of scope:

- **UI Dashboard** - API-first; UI can be built separately or use Grafana
- **Data Transformation** - DQ only; use dbt/Airflow for transforms
- **Data Lineage** - Defer to MLG or dedicated lineage tools
- **ML-based Anomaly Detection** - Start with statistical methods only
- **Real-time Streaming** - Batch checks only initially
