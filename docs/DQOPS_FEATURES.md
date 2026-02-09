# DQOps Feature Reference

Complete feature catalog for implementing a DQOps-equivalent data quality platform.

---

## Table of Contents

1. [Platform Architecture](#1-platform-architecture)
2. [Check Types & Time Scales](#2-check-types--time-scales)
3. [Table-Level Checks](#3-table-level-checks)
4. [Column-Level Checks](#4-column-level-checks)
5. [Anomaly Detection](#5-anomaly-detection)
6. [Data Observability](#6-data-observability)
7. [Incident Management](#7-incident-management)
8. [Notifications & Integrations](#8-notifications--integrations)
9. [Dashboards & KPIs](#9-dashboards--kpis)
10. [Data Sources](#10-data-sources)
11. [Configuration & APIs](#11-configuration--apis)

---

## 1. Platform Architecture

### Core Components

| Component | Description |
|-----------|-------------|
| **Sensors** | Jinja2 SQL templates that measure data characteristics |
| **Rules** | Python functions that evaluate sensor output against thresholds |
| **Check** | Sensor + Rule combination with parameters |
| **Incident** | Grouped data quality issues for alerting |
| **Data Quality Warehouse** | Time-series storage for all check results |

### Check Execution Flow

```
1. Load check definition (YAML or database)
2. Render SQL template with Jinja2 (table, column, params, partition)
3. Execute SQL against target data source
4. Parse sensor result (numeric value)
5. Apply rule logic (threshold, anomaly, change detection)
6. Determine severity (warning, error, fatal)
7. Record result with timestamp
8. Group into incident if failed
9. Send notifications if new incident
```

### Severity Levels

| Level | Purpose | KPI Impact |
|-------|---------|------------|
| **Warning** | Anomaly detection, early warnings | No impact |
| **Error** | Real data quality issues to fix | Decreases KPI score |
| **Fatal** | Critical issues blocking downstream | Decreases KPI score |

---

## 2. Check Types & Time Scales

### Three Check Modes

| Mode | Purpose | Storage | Use Case |
|------|---------|---------|----------|
| **Profiling** | Initial data assessment | 1 result/month (overwrites) | Exploring new data sources |
| **Monitoring** | Continuous quality tracking | 1 result/day or /month | Production monitoring |
| **Partitioned** | Per-partition analysis | 1 result/partition/day | Large tables, incremental loads |

### Time Scales

| Scale | Monitoring | Partitioned |
|-------|------------|-------------|
| **Daily** | End-of-day status | Per-partition daily results |
| **Monthly** | End-of-month status | Per-partition monthly aggregates |

### Partitioned Checks Configuration

```yaml
# Requires partition_by_column to be set
spec:
  timestamp_columns:
    partition_by_column: created_at  # Any date/datetime column

  partitioned_checks:
    daily:
      nulls:
        daily_partition_nulls_percent:
          warning:
            max_percent: 5.0
```

**Benefits of Partitioned Checks:**
- Only scan new partitions (cost-effective for large tables)
- Detect issues on first day corrupted data arrives
- Support append-only/immutable data patterns
- Enable terabyte/petabyte scale monitoring

---

## 3. Table-Level Checks

### 3.1 Volume Checks

| Check | Description | Parameters |
|-------|-------------|------------|
| `row_count` | Detects empty or undersized tables | `min_count`, `max_count` |
| `row_count_anomaly` | Identifies unusual day-to-day volume changes | `anomaly_percent` |
| `row_count_change` | Compares current volume to last known count | `max_change_percent` |
| `row_count_change_1_day` | Compares volume to previous day | `max_change_percent` |
| `row_count_change_7_days` | Compares volume to one week prior | `max_change_percent` |
| `row_count_change_30_days` | Compares volume to thirty days prior | `max_change_percent` |

### 3.2 Availability Checks

| Check | Description | Parameters |
|-------|-------------|------------|
| `table_availability` | Verifies table is accessible without server errors | None |

### 3.3 Schema Checks

| Check | Description | Parameters |
|-------|-------------|------------|
| `column_count` | Verifies expected number of columns | `expected_count` |
| `column_count_changed` | Detects if column count differs from previous | None |
| `column_list_changed` | Identifies added or removed columns | None |
| `column_list_or_order_changed` | Detects schema changes including reordering | None |
| `column_types_changed` | Identifies changes in column data types | None |

### 3.4 Timeliness Checks

| Check | Description | Parameters |
|-------|-------------|------------|
| `data_freshness` | Time between most recent row and current time | `max_seconds` |
| `data_freshness_anomaly` | Detects anomalies in freshness time series | `anomaly_percent` |
| `data_staleness` | Time since last data load | `max_seconds` |
| `data_ingestion_delay` | Lag between event timestamp and ingestion | `max_seconds` |
| `reload_lag` | Maximum delay between row creation and loading | `max_seconds` |

### 3.5 Uniqueness Checks (Table-Level)

| Check | Description | Parameters |
|-------|-------------|------------|
| `duplicate_record_count` | Counts duplicate rows (across all columns) | `max_count` |
| `duplicate_record_percent` | Percentage of duplicate records | `max_percent` |

### 3.6 Accuracy Checks (Cross-Table)

| Check | Description | Parameters |
|-------|-------------|------------|
| `total_row_count_match_percent` | Compares row count with reference table | `min_percent`, `reference_table` |

### 3.7 Comparison Checks (Cross-Source)

| Check | Description | Parameters |
|-------|-------------|------------|
| `row_count_match` | Compares row counts between sources | `reference_connection`, `reference_table` |
| `column_count_match` | Compares column counts between sources | `reference_connection`, `reference_table` |

### 3.8 Custom SQL Checks (Table-Level)

| Check | Description | Parameters |
|-------|-------------|------------|
| `sql_condition_failed_on_table` | Validates rows against custom SQL | `sql_condition` |
| `sql_condition_passed_percent_on_table` | Min percentage passing custom SQL | `sql_condition`, `min_percent` |
| `sql_aggregate_expression_on_table` | Aggregate calculation within range | `sql_expression`, `min_value`, `max_value` |
| `sql_invalid_record_count_on_table` | Counts records matching SQL query | `sql_query`, `max_count` |
| `import_custom_result_on_table` | Imports external DQ results | `result_table`, `check_name` |

---

## 4. Column-Level Checks

### 4.1 Nulls / Completeness

| Check | Description | Parameters |
|-------|-------------|------------|
| `nulls_count` | Count of null values | `max_count` |
| `nulls_percent` | Percentage of null values | `max_percent` |
| `nulls_percent_anomaly` | Detects unusual null percentage changes | `anomaly_percent` |
| `not_nulls_count` | Verifies minimum non-null values | `min_count` |
| `not_nulls_percent` | Percentage of non-null values | `min_percent` |
| `empty_column_found` | Detects completely empty columns | None |
| `nulls_percent_change` | Null % change from previous | `max_change_percent` |
| `nulls_percent_change_1_day` | Null % compared to yesterday | `max_change_percent` |
| `nulls_percent_change_7_days` | Null % compared to 7 days ago | `max_change_percent` |
| `nulls_percent_change_30_days` | Null % compared to 30 days ago | `max_change_percent` |

### 4.2 Uniqueness

| Check | Description | Parameters |
|-------|-------------|------------|
| `distinct_count` | Distinct value count within range | `min_count`, `max_count` |
| `distinct_percent` | Percentage of distinct values | `min_percent`, `max_percent` |
| `duplicate_count` | Count of duplicate values | `max_count` |
| `duplicate_percent` | Percentage of duplicate values | `max_percent` |
| `distinct_count_anomaly` | Detects unusual distinct count changes | `anomaly_percent` |
| `distinct_percent_anomaly` | Detects unusual distinct % changes | `anomaly_percent` |
| `distinct_count_change` | Distinct count change from previous | `max_change_percent` |
| `distinct_count_change_1_day` | Distinct count vs yesterday | `max_change_percent` |
| `distinct_count_change_7_days` | Distinct count vs 7 days ago | `max_change_percent` |
| `distinct_count_change_30_days` | Distinct count vs 30 days ago | `max_change_percent` |
| `distinct_percent_change` | Distinct % change from previous | `max_change_percent` |
| `distinct_percent_change_1_day` | Distinct % vs yesterday | `max_change_percent` |
| `distinct_percent_change_7_days` | Distinct % vs 7 days ago | `max_change_percent` |
| `distinct_percent_change_30_days` | Distinct % vs 30 days ago | `max_change_percent` |

### 4.3 Numeric / Statistical

| Check | Description | Parameters |
|-------|-------------|------------|
| `number_below_min_value` | Count of values below minimum | `min_value`, `max_count` |
| `number_above_max_value` | Count of values above maximum | `max_value`, `max_count` |
| `number_below_min_value_percent` | Percent below minimum | `min_value`, `max_percent` |
| `number_above_max_value_percent` | Percent above maximum | `max_value`, `max_percent` |
| `number_in_range_percent` | Percent within range | `min_value`, `max_value`, `min_percent` |
| `integer_in_range_percent` | Integer percent within range | `min_value`, `max_value`, `min_percent` |
| `negative_values` | Count of negative values | `max_count` |
| `negative_values_percent` | Percent of negative values | `max_percent` |
| `non_negative_values` | Count of non-negative values | `min_count` |
| `non_negative_values_percent` | Percent of non-negative values | `min_percent` |
| `min_in_range` | Minimum value within range | `min_value`, `max_value` |
| `max_in_range` | Maximum value within range | `min_value`, `max_value` |
| `sum_in_range` | Sum within range | `min_value`, `max_value` |
| `mean_in_range` | Mean within range | `min_value`, `max_value` |
| `median_in_range` | Median within range | `min_value`, `max_value` |
| `sample_stddev_in_range` | Sample std dev within range | `min_value`, `max_value` |
| `population_stddev_in_range` | Population std dev within range | `min_value`, `max_value` |
| `sample_variance_in_range` | Sample variance within range | `min_value`, `max_value` |
| `population_variance_in_range` | Population variance within range | `min_value`, `max_value` |
| `percentile_in_range` | Custom percentile within range | `percentile`, `min_value`, `max_value` |
| `percentile_10_in_range` | 10th percentile within range | `min_value`, `max_value` |
| `percentile_25_in_range` | 25th percentile within range | `min_value`, `max_value` |
| `percentile_75_in_range` | 75th percentile within range | `min_value`, `max_value` |
| `percentile_90_in_range` | 90th percentile within range | `min_value`, `max_value` |

### 4.4 Geographic

| Check | Description | Parameters |
|-------|-------------|------------|
| `invalid_latitude` | Count of invalid latitudes (outside -90..90) | `max_count` |
| `valid_latitude_percent` | Percent of valid latitudes | `min_percent` |
| `invalid_longitude` | Count of invalid longitudes (outside -180..180) | `max_count` |
| `valid_longitude_percent` | Percent of valid longitudes | `min_percent` |

### 4.5 Text / String

| Check | Description | Parameters |
|-------|-------------|------------|
| `text_min_length` | Minimum text length in column | `min_value`, `max_value` |
| `text_max_length` | Maximum text length in column | `min_value`, `max_value` |
| `text_mean_length` | Average text length | `min_value`, `max_value` |
| `text_length_below_min_length` | Count of texts too short | `min_length`, `max_count` |
| `text_length_below_min_length_percent` | Percent of texts too short | `min_length`, `max_percent` |
| `text_length_above_max_length` | Count of texts too long | `max_length`, `max_count` |
| `text_length_above_max_length_percent` | Percent of texts too long | `max_length`, `max_percent` |
| `text_length_in_range_percent` | Percent within length range | `min_length`, `max_length`, `min_percent` |
| `min_word_count` | Minimum word count in column | `min_value`, `max_value` |
| `max_word_count` | Maximum word count in column | `min_value`, `max_value` |

### 4.6 Whitespace / Blanks

| Check | Description | Parameters |
|-------|-------------|------------|
| `empty_text_found` | Count of empty strings (length 0) | `max_count` |
| `empty_text_percent` | Percent of empty strings | `max_percent` |
| `whitespace_text_found` | Count of whitespace-only text | `max_count` |
| `whitespace_text_percent` | Percent of whitespace-only text | `max_percent` |
| `null_placeholder_text_found` | Count of null placeholders ("null", "None", "n/a") | `max_count` |
| `null_placeholder_text_percent` | Percent of null placeholders | `max_percent` |
| `text_surrounded_by_whitespace_found` | Count with leading/trailing spaces | `max_count` |
| `text_surrounded_by_whitespace_percent` | Percent with surrounding whitespace | `max_percent` |

### 4.7 Patterns / Formats

| Check | Description | Parameters |
|-------|-------------|------------|
| `text_not_matching_regex_found` | Count failing regex | `regex`, `max_count` |
| `texts_not_matching_regex_percent` | Percent failing regex | `regex`, `max_percent` |
| `text_matching_regex_percent` | Percent matching regex | `regex`, `min_percent` |
| `invalid_email_format_found` | Count of invalid emails | `max_count` |
| `invalid_email_format_percent` | Percent of invalid emails | `max_percent` |
| `text_not_matching_date_pattern_found` | Count of invalid date formats | `date_format`, `max_count` |
| `text_not_matching_date_pattern_percent` | Percent of invalid date formats | `date_format`, `max_percent` |
| `text_match_date_format_percent` | Percent matching date format | `date_format`, `min_percent` |
| `text_not_matching_name_pattern_percent` | Percent of invalid name formats | `max_percent` |
| `invalid_uuid_format_found` | Count of invalid UUIDs | `max_count` |
| `invalid_uuid_format_percent` | Percent of invalid UUIDs | `max_percent` |
| `invalid_ip4_address_format_found` | Count of invalid IPv4 | `max_count` |
| `invalid_ip6_address_format_found` | Count of invalid IPv6 | `max_count` |
| `invalid_usa_phone_format_found` | Count of invalid USA phones | `max_count` |
| `invalid_usa_phone_format_percent` | Percent of invalid USA phones | `max_percent` |
| `invalid_usa_zipcode_format_found` | Count of invalid USA zipcodes | `max_count` |
| `invalid_usa_zipcode_format_percent` | Percent of invalid USA zipcodes | `max_percent` |

### 4.8 Accepted Values / Domain

| Check | Description | Parameters |
|-------|-------------|------------|
| `text_found_in_set_percent` | Percent of values in allowed set | `expected_values`, `min_percent` |
| `number_found_in_set_percent` | Percent of numbers in allowed set | `expected_values`, `min_percent` |
| `expected_text_values_in_use_count` | Count of expected strings found | `expected_values`, `min_count` |
| `expected_texts_in_top_values_count` | Expected values in top N | `expected_values`, `top_n`, `min_count` |
| `expected_numbers_in_use_count` | Count of expected numbers found | `expected_values`, `min_count` |
| `text_valid_country_code_percent` | Percent of valid 2-letter country codes | `min_percent` |
| `text_valid_currency_code_percent` | Percent of valid currency codes | `min_percent` |

### 4.9 Boolean

| Check | Description | Parameters |
|-------|-------------|------------|
| `true_percent` | Percent of true values | `min_percent`, `max_percent` |
| `false_percent` | Percent of false values | `min_percent`, `max_percent` |

### 4.10 DateTime / Date Validation

| Check | Description | Parameters |
|-------|-------------|------------|
| `date_values_in_future_percent` | Percent of future dates | `max_percent` |
| `date_in_range_percent` | Percent of dates in valid range | `min_date`, `max_date`, `min_percent` |

### 4.11 Data Type Detection / Conversion

| Check | Description | Parameters |
|-------|-------------|------------|
| `detected_datatype_in_text` | Auto-detect data type of text column | None (returns type) |
| `detected_datatype_in_text_changed` | Detects when column data type shifts | None |
| `text_parsable_to_boolean_percent` | Percent convertible to boolean | `min_percent` |
| `text_parsable_to_integer_percent` | Percent convertible to integer | `min_percent` |
| `text_parsable_to_float_percent` | Percent convertible to float | `min_percent` |
| `text_parsable_to_date_percent` | Percent convertible to date | `min_percent` |

### 4.12 PII Detection

| Check | Description | Parameters |
|-------|-------------|------------|
| `contains_usa_phone_percent` | Percent containing USA phone numbers | `max_percent` |
| `contains_email_percent` | Percent containing email addresses | `max_percent` |
| `contains_usa_zipcode_percent` | Percent containing USA zipcodes | `max_percent` |
| `contains_ip4_percent` | Percent containing IPv4 addresses | `max_percent` |
| `contains_ip6_percent` | Percent containing IPv6 addresses | `max_percent` |

### 4.13 Referential Integrity

| Check | Description | Parameters |
|-------|-------------|------------|
| `lookup_key_not_found` | Count of values not in reference dictionary | `reference_table`, `reference_column`, `max_count` |
| `lookup_key_found_percent` | Percent of valid keys in dictionary | `reference_table`, `reference_column`, `min_percent` |

### 4.14 Schema (Column-Level)

| Check | Description | Parameters |
|-------|-------------|------------|
| `column_exists` | Verifies column presence | None |
| `column_type_changed` | Detects changes in column data type | None |

### 4.15 Accuracy (Cross-Table Column)

| Check | Description | Parameters |
|-------|-------------|------------|
| `total_sum_match_percent` | Compares column sum with reference | `reference_table`, `reference_column`, `min_percent` |
| `total_min_match_percent` | Compares minimum values | `reference_table`, `reference_column`, `min_percent` |
| `total_max_match_percent` | Compares maximum values | `reference_table`, `reference_column`, `min_percent` |
| `total_average_match_percent` | Compares averages | `reference_table`, `reference_column`, `min_percent` |
| `total_not_null_count_match_percent` | Compares non-null counts | `reference_table`, `reference_column`, `min_percent` |

### 4.16 Comparisons (Cross-Source Column)

| Check | Description | Parameters |
|-------|-------------|------------|
| `sum_match` | Compares sum across sources | `reference_connection`, `reference_table`, `reference_column` |
| `min_match` | Compares min across sources | `reference_connection`, `reference_table`, `reference_column` |
| `max_match` | Compares max across sources | `reference_connection`, `reference_table`, `reference_column` |
| `mean_match` | Compares mean across sources | `reference_connection`, `reference_table`, `reference_column` |
| `not_null_count_match` | Compares non-null count | `reference_connection`, `reference_table`, `reference_column` |
| `null_count_match` | Compares null count | `reference_connection`, `reference_table`, `reference_column` |
| `distinct_count_match` | Compares distinct count | `reference_connection`, `reference_table`, `reference_column` |

### 4.17 Custom SQL (Column-Level)

| Check | Description | Parameters |
|-------|-------------|------------|
| `sql_condition_failed_on_column` | Validates rows against custom SQL | `sql_condition` |
| `sql_condition_passed_percent_on_column` | Percent passing custom SQL | `sql_condition`, `min_percent` |
| `sql_aggregate_expression_on_column` | Aggregate calculation in range | `sql_expression`, `min_value`, `max_value` |
| `sql_invalid_value_count_on_column` | Count of invalid values from SQL | `sql_query`, `max_count` |
| `import_custom_result_on_column` | Import external DQ results | `result_table`, `check_name` |

---

## 5. Anomaly Detection

### Anomaly Detection Checks

| Check | Description | Algorithm |
|-------|-------------|-----------|
| `sum_anomaly` | Detects unusual changes in totals | Time-series ML |
| `mean_anomaly` | Identifies outlier averages | Time-series ML |
| `median_anomaly` | Flags unusual median changes | Time-series ML |
| `min_anomaly` | Detects unexpected minimum shifts | Time-series ML |
| `max_anomaly` | Detects unexpected maximum shifts | Time-series ML |
| `row_count_anomaly` | Identifies unusual volume changes | Time-series ML |
| `data_freshness_anomaly` | Detects freshness anomalies | Time-series ML |
| `nulls_percent_anomaly` | Detects null percentage anomalies | Time-series ML |
| `distinct_count_anomaly` | Detects distinct count anomalies | Time-series ML |
| `distinct_percent_anomaly` | Detects distinct percent anomalies | Time-series ML |

### Change Detection Checks (Temporal)

| Pattern | 1 Day | 7 Days | 30 Days |
|---------|-------|--------|---------|
| `row_count_change_*` | ✅ | ✅ | ✅ |
| `mean_change_*` | ✅ | ✅ | ✅ |
| `median_change_*` | ✅ | ✅ | ✅ |
| `sum_change_*` | ✅ | ✅ | ✅ |
| `nulls_percent_change_*` | ✅ | ✅ | ✅ |
| `distinct_count_change_*` | ✅ | ✅ | ✅ |
| `distinct_percent_change_*` | ✅ | ✅ | ✅ |

### Anomaly Detection Features

- **Seasonality prediction** - accounts for weekly/monthly patterns
- **Rolling averages** - 7-day, 14-day, 30-day windows
- **Outlier detection** - new min/max values
- **Configurable sensitivity** - `anomaly_percent` threshold
- **Parallel rule execution** - speeds up anomaly detection

---

## 6. Data Observability

### Auto-Profiling

| Feature | Description |
|---------|-------------|
| **Table import profiling** | Automatic statistics collection on new tables |
| **Column type detection** | Detect if text columns are really numeric/date/boolean |
| **Rule mining** | Auto-suggest thresholds from observed data |
| **Pattern recognition** | Detect email, phone, UUID formats automatically |

### Check Patterns (Auto-Activation)

| Pattern Type | Description |
|--------------|-------------|
| **Connection-level** | Apply checks to all tables in a connection |
| **Schema-level** | Apply checks to all tables in a schema |
| **Table name pattern** | Apply checks to tables matching wildcard |
| **Column name pattern** | Apply checks to columns matching wildcard |
| **Column type pattern** | Apply checks to all columns of a type |

### Data Policies

```yaml
# Example: Apply null check to all timestamp columns
patterns:
  - target: column
    column_name_pattern: "*_at"
    column_type: timestamp
    checks:
      nulls:
        nulls_percent:
          warning:
            max_percent: 1.0
```

### Incremental Monitoring

- Only check new partitions
- Skip already-validated data
- Cost-effective for large tables
- Date-based incremental scans

---

## 7. Incident Management

### Incident Statuses

| Status | Description | Assigned To |
|--------|-------------|-------------|
| **Open** | New incident detected | Auto-created |
| **Acknowledged** | Confirmed, assigned for resolution | 2nd level support |
| **Resolved** | Issue fixed | 3rd level support |
| **Muted** | False positive or accepted issue | Any |

### Incident Grouping

Incidents group similar issues by:

| Grouping | Example |
|----------|---------|
| **Table** | All issues on `orders` table |
| **Column** | All issues on `email` column |
| **Data quality dimension** | All completeness issues |
| **Check category** | All null checks |
| **Check type** | All `nulls_percent` checks |

### Incident Features

- **Deduplication** - Same issue doesn't create multiple incidents
- **Similarity clustering** - ML-based grouping of related issues
- **Auto-resolve** - Incident closes when check passes
- **Escalation** - Workflow from detection to resolution
- **Audit trail** - History of status changes
- **Ticketing integration** - Link to Jira, ServiceNow, Azure DevOps

---

## 8. Notifications & Integrations

### Notification Channels

| Channel | Trigger Events |
|---------|----------------|
| **Email** | New incident, status change, resolved |
| **Slack** | New incident, status change |
| **Webhooks** | All events (customizable payload) |
| **PagerDuty** | Fatal severity incidents |

### Notification Filters

```yaml
# Example: Only notify on error+ severity for production tables
filters:
  - name: production_errors
    conditions:
      schema_pattern: "prod_*"
      min_severity: error
    channels:
      - slack_channel: "#data-quality-alerts"
      - email: "data-team@company.com"
```

### Integration Points

| System | Integration Type |
|--------|------------------|
| **Apache Airflow** | Operator for running checks |
| **dbt** | Post-run hooks, test imports |
| **Jira** | Webhook for ticket creation |
| **ServiceNow** | Incident sync |
| **Azure DevOps** | Work item creation |

---

## 9. Dashboards & KPIs

### Dashboard Categories

| Category | Dashboards |
|----------|------------|
| **Current Status** | Table status, column status, connection status |
| **Issue Severity** | Daily issue summary, severity trends |
| **Issue Count** | Failed checks per day, per table, per check type |
| **Data Quality KPIs** | Overall score, by dimension, by table |
| **Check Results** | Detailed execution history with values |
| **Volume** | Row counts, largest tables, empty tables |
| **Schema Changes** | Column additions, removals, type changes |
| **PII** | PII detection results by table/column |
| **Usage** | Check execution stats, errors |

### Data Quality KPIs

| KPI | Calculation |
|-----|-------------|
| **Overall DQ Score** | `(passed_checks / total_checks) * 100` |
| **Table DQ Score** | Score per table |
| **Dimension Score** | Score per quality dimension |
| **Connection Score** | Score per data source |
| **Trend** | Score change over time |

### Quality Dimensions Mapping

| Dimension | Check Categories |
|-----------|------------------|
| **Completeness** | Nulls, empty columns |
| **Uniqueness** | Duplicates, distinct counts |
| **Validity** | Patterns, accepted values, data types |
| **Accuracy** | Cross-table comparisons |
| **Consistency** | Cross-source comparisons |
| **Timeliness** | Freshness, staleness, ingestion delay |
| **Availability** | Table availability |

---

## 10. Data Sources

### Supported Connectors

| Database | Status | Notes |
|----------|--------|-------|
| PostgreSQL | ✅ | Covers Aurora, RDS, Cloud SQL |
| MySQL | ✅ | Covers MariaDB, Aurora MySQL |
| SQL Server | ✅ | Azure SQL, on-prem |
| BigQuery | ✅ | GCP analytics |
| Snowflake | ✅ | Cloud DW leader |
| Redshift | ✅ | AWS analytics |
| Databricks | ✅ | Unity Catalog, Delta Lake |
| Oracle | ✅ | Enterprise |
| Athena | ✅ | AWS serverless |
| Presto | ✅ | Distributed SQL |
| Trino | ✅ | Distributed SQL |
| DuckDB | ✅ | Embedded analytics |
| Spark | ✅ | Big data |
| SingleStoreDB | ✅ | Real-time analytics |
| CSV Files | ✅ | Local files |
| Iceberg | ✅ | Table format |

### Connection Features

- Encrypted credentials at rest
- Connection pooling
- Health check / test connection
- Schema/table/column metadata discovery
- Connection-level default settings

---

## 11. Configuration & APIs

### Configuration Methods

| Method | Use Case |
|--------|----------|
| **YAML Files** | Git-versioned, CI/CD friendly |
| **REST API** | Programmatic access |
| **Web UI** | Non-technical users |
| **CLI** | Automation scripts |
| **VS Code Extension** | Developer workflow |

### YAML Structure

```yaml
# .dqops/sources/my_connection/my_schema/my_table.dqotable.yaml
apiVersion: dqo/v1
kind: table
spec:
  # Timestamp column for partitioned checks
  timestamp_columns:
    partition_by_column: created_at
    event_timestamp_column: event_time
    ingestion_timestamp_column: loaded_at

  # Profiling checks
  profiling_checks:
    volume:
      profile_row_count:
        warning:
          min_count: 1000

  # Daily monitoring checks
  monitoring_checks:
    daily:
      volume:
        daily_row_count:
          error:
            min_count: 100
      timeliness:
        daily_data_freshness:
          warning:
            max_seconds: 86400

  # Daily partitioned checks
  partitioned_checks:
    daily:
      nulls:
        daily_partition_nulls_percent:
          warning:
            max_percent: 5.0

  # Column-level checks
  columns:
    email:
      profiling_checks:
        patterns:
          profile_invalid_email_format_percent:
            warning:
              max_percent: 1.0
      monitoring_checks:
        daily:
          nulls:
            daily_nulls_percent:
              error:
                max_percent: 0.0
```

### REST API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/connections` | GET/POST | List/create connections |
| `/api/connections/{id}/schemas` | GET | List schemas |
| `/api/connections/{id}/tables` | GET | List tables |
| `/api/tables/{id}/checks` | GET/POST | List/create checks |
| `/api/checks/{id}/run` | POST | Execute check |
| `/api/checks/{id}/results` | GET | Get results |
| `/api/incidents` | GET | List incidents |
| `/api/incidents/{id}` | PATCH | Update incident status |
| `/api/jobs` | GET | List running jobs |
| `/api/dashboards/kpi` | GET | Get KPI scores |

### CLI Commands

```bash
# Run checks
dqo check run --connection=prod_db --table=orders

# Profile a table
dqo table profile --connection=prod_db --table=orders

# Collect statistics
dqo table collect-statistics --connection=prod_db

# Run all daily monitoring checks
dqo check run --check-type=monitoring --time-scale=daily

# Export results
dqo data export --format=parquet --output=./results/
```

---

## Implementation Checklist

### Phase 1: Core Checks (Foundation)
- [ ] Volume checks with change detection
- [ ] Schema change detection
- [ ] Timeliness checks (freshness, staleness)
- [ ] Null/completeness checks
- [ ] Uniqueness checks
- [ ] Pattern validation (regex, email, UUID)

### Phase 2: Anomaly Detection
- [ ] Time-series storage for historical data
- [ ] Rolling average calculation
- [ ] Anomaly detection algorithm (statistical or ML)
- [ ] Change detection (1/7/30 day windows)
- [ ] Seasonality support

### Phase 3: Advanced Checks
- [ ] PII detection
- [ ] Referential integrity
- [ ] Cross-source comparisons
- [ ] Data type detection
- [ ] Whitespace/blank detection
- [ ] Geographic validation

### Phase 4: Check Modes
- [ ] Profiling mode (one-time assessment)
- [ ] Monitoring mode (daily/monthly)
- [ ] Partitioned mode (per-partition)
- [ ] Partition column configuration
- [ ] Incremental execution

### Phase 5: Incident Management
- [ ] 4-status workflow
- [ ] Incident grouping strategies
- [ ] False positive muting
- [ ] Auto-resolve on pass
- [ ] Similarity clustering

### Phase 6: Data Observability
- [ ] Auto-profiling on import
- [ ] Rule mining
- [ ] Check patterns (auto-activation)
- [ ] Data policies
- [ ] Connection/schema/table patterns

### Phase 7: Dashboards & KPIs
- [ ] Data quality KPI calculation
- [ ] Dimension-based scoring
- [ ] Current status dashboards
- [ ] Trend analysis
- [ ] Check results explorer

### Phase 8: Notifications
- [ ] Webhook notifications
- [ ] Slack integration
- [ ] Email notifications
- [ ] Notification filters
- [ ] Ticketing system integration

---

## References

- [DQOps Documentation](https://dqops.com/docs/)
- [Categories of Data Quality Checks](https://dqops.com/docs/categories-of-data-quality-checks/)
- [List of Checks](https://dqops.com/docs/checks/)
- [Incident Management](https://dqops.com/docs/dqo-concepts/grouping-data-quality-issues-to-incidents/)
- [Data Quality Dashboards](https://dqops.com/docs/dqo-concepts/types-of-data-quality-dashboards/)
- [Partitioned Checks](https://dqops.com/docs/dqo-concepts/definition-of-data-quality-checks/partition-checks/)
- [GitHub Repository](https://github.com/dqops/dqo)
