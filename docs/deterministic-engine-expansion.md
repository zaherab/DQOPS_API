# Deterministic Check Engine — Expansion Design

**Status:** Tier 1 + date-range inference **implemented** (engine 15 → 25 emitted
check types). Tier 2 (foreign_key, statistical in-range, cross-table) deferred —
see §8.
**Scope:** `src/dq_platform/profilers/check_emitter.py` and its inputs.
**Question it answers:** "Incorporate all possible checks into the deterministic DQ engine."

---

## 1. TL;DR

The deterministic engine emits **15** check types today. The DQOps check
registry (`DQOpsCheckType`) has **178**. "Incorporate all 178" is **not
achievable** — roughly **120** of them are structurally un-emittable by a
*deterministic, single-run, no-history, no-user-SQL* engine. Forcing them in
recreates the exact phantom-failure and check-churn bugs that migrations
`015`/`016`/`017` were written to clean up.

The realistic, honest target is **15 → ~30 emitted check types**, delivered in
three tiers. Tier 1 (~10 high-value checks) needs only new emit branches —
no schema or profiler changes. Tier 2 unlocks the `accuracy` dimension, which
is *always* `not_assessed` today. Tier 3 is a small inference addition.

---

## 2. How the engine works today

`emit()` in `check_emitter.py` is pure compute — no DB, no network, no LLM.
Same inputs → identical output. It consumes four input streams:

| Input | Produced by | Carries |
|---|---|---|
| Field declarations | contentSchema | logicalType, primaryKey, unique, format, pattern, acceptedValues, acceptedRange, acceptedLength, classification |
| DQ profile | producer promises | per-dimension promise level (`completeness: "99"`, `timeliness: "24h"`, …) |
| Table profile aggregates | `profile_runner.py` | **count, nulls, distinct, min, max, min_len, max_len** — and nothing else |
| Inference results | `inference_engine.py` | format, codelist, enum, regex, length range, numeric range |

Two structural gates constrain every emit decision:

1. **Promised-dimensions-only.** A check emitted on a dimension the DQ profile
   does not promise is treated as an orphan by the caller (`dq-auto-run` in
   MLG), deleted, then re-emitted next run — infinite create/delete churn.
   So every branch is gated `if _dim_for(check_type) in promised`.
2. **`thresholds_from_promise()` returns `None` for many rule types.** Percent
   and time-shaped rules project cleanly from a promise. `min_max_count`,
   `min_value`, `min_max_value`, `equal_to`, `is_true`, `anomaly_percentile`
   do **not** — there is no meaningful promise→params mapping, so the check is
   silently skipped unless the emitter supplies absolute params itself
   (as it does for `row_count`).

### 2.1 The 15 currently emitted

| Check type | Trigger | Dimension |
|---|---|---|
| `nulls_count` | primaryKey | completeness |
| `nulls_percent` | completeness promised | completeness |
| `distinct_percent` | primaryKey / unique | uniqueness |
| `text_found_in_set_percent` | acceptedValues / inferred enum (text) | validity |
| `number_found_in_set_percent` | acceptedValues (numeric) | validity |
| `number_in_range_percent` | acceptedRange / inferred numeric range | conformity |
| `text_length_in_range_percent` | acceptedLength / inferred length range | conformity |
| `text_matching_regex_percent` | pattern / format / inferred regex | conformity |
| `text_valid_country_code_percent` | format=country_code / ISO-3166 inference | accuracy |
| `text_valid_currency_code_percent` | format=currency_code / ISO-4217 inference | accuracy |
| `data_freshness` | table freshnessColumn | timeliness |
| `row_count` | table expectedRowCount | coverage |
| `total_row_count_match_percent` | table consistentWith | accuracy |
| `column_count` | conformity promised | conformity |
| `column_list_changed` | consistency promised | consistency |

---

## 3. The hard constraints — why "all 178" is impossible

A deterministic single-run emitter cannot produce a check whose sensor needs
information the emitter never has. Four classes are permanently out of scope:

| Class | Count | Why un-emittable | Evidence |
|---|---|---|---|
| **Change-detection** (`*_change`, `*_change_1_day/7_days/30_days`) | ~25 | Sensor SQL queries the `check_results` table **on the monitored DB**, where it does not exist; needs a `check_id` param no caller injects. | Purged by migration `017` |
| **Anomaly** (`*_anomaly`) | ~10 | Needs 30+ historical sensor values. Also `ANOMALY_EXCLUDED` — advisory monitors, never scored into a dimension by design. | `dimension_mapping.py` `ANOMALY_EXCLUDED` |
| **Custom SQL** (`sql_condition_*`, `sql_aggregate_*`, `sql_invalid_*`, `import_custom_result_*`) | ~11 | Needs a user-authored SQL expression / external result. Not inferable from a profile. | Purged by migration `017` |
| **Cross-table / referential** (`foreign_key_*`, `total_*_match`, `*_match`) | ~15 | Needs a `reference_table` param. Renders broken SQL (`MAX()`, empty Jinja var) without it. | Purged by migration `015` |

These ~61 plus their close variants account for the bulk of the 178. The
remainder splits into "emittable now", "emittable with a small new input", and
"genuinely not worth it" (redundant inverses, niche checks).

> **Note on `data_staleness`:** migration `016` purged staleness checks that
> were missing the required `timestamp_column` param. That does **not** make
> the check un-emittable — it makes it un-emittable *without the param*. The
> emitter already holds `table_decl.freshness_column`; passing it as
> `timestamp_column` produces a valid, runnable check. See Tier 1.

---

## 4. Full catalog classification

Verdict legend:
- **NOW** — emittable with only a new branch in `_emit_field_checks` / `_emit_table_checks`.
- **T2-DECL** — needs a new field on `FieldDeclaration` / `TableDeclaration`.
- **T2-PROF** — needs `profile_runner` to collect a new aggregate.
- **T3-INFER** — needs a new inference function.
- **NO** — structurally un-emittable (history / user-SQL / cross-table) or not worth it (redundant inverse, niche).

### 4.1 Volume / Schema / Timeliness (table-level)

| Check | Verdict | Notes |
|---|---|---|
| `row_count` | *emitted* | |
| `row_count_change_1_day/7_days/30_days` | NO | history |
| `column_count` | *emitted* | |
| `column_exists` | NOW | one per declared field; conformity; no rule params (sensor-baseline) |
| `column_count_changed` | NO | redundant — `column_count` already drift-baselines |
| `column_list_changed` | *emitted* | |
| `column_list_or_order_changed` | NOW | stricter variant; emit instead of `column_list_changed` when column order is contractual |
| `column_types_changed` | NOW | **type drift** — genuinely additive vs `column_list_changed` (name drift only); consistency |
| `column_type_changed` (column-level) | NOW | per-column type drift |
| `data_freshness` | *emitted* | |
| `data_staleness` | NOW | pass `freshnessColumn` as `timestamp_column`; timeliness |
| `data_ingestion_delay` | T2-DECL | needs an ingestion-timestamp + event-timestamp column pair |
| `reload_lag` | T2-DECL | same — needs ingestion timestamp columns |
| `table_availability` | NOW | table-level, no params — "is the table queryable"; coverage |

### 4.2 Completeness / Uniqueness

| Check | Verdict | Notes |
|---|---|---|
| `nulls_count` / `nulls_percent` | *emitted* | |
| `not_nulls_count` / `not_nulls_percent` | NO | redundant inverse of nulls checks |
| `empty_column_found` | NO | redundant — `nulls_percent` at 100% covers it |
| `distinct_count` | NO | `min_max_count`, no promise map; `distinct_percent` covers it |
| `distinct_percent` | *emitted* | |
| `duplicate_count` / `duplicate_percent` | NOW (optional) | uniqueness alt; only adds value on non-PK columns flagged `unique` |
| `duplicate_record_count` / `duplicate_record_percent` | NOW | **full-row dedup** — emit when `uniqueness` promised and no PK declared; genuinely additive |

### 4.3 Numeric / Statistical

| Check | Verdict | Notes |
|---|---|---|
| `number_in_range_percent` | *emitted* | |
| `integer_in_range_percent` | NOW | route here instead of `number_in_range_percent` when logicalType=integer |
| `number_below_min_value_percent` / `number_above_max_value_percent` | NOW | from a **one-sided** acceptedRange (only min, or only max, declared) |
| `number_below_min_value` / `number_above_max_value` (count) | NOW (optional) | count-shaped variant of the above |
| `negative_values` / `negative_values_percent` | NOW (conditional) | emit only when acceptedRange lower bound ≥ 0 — a bare "no negatives" guess is unsafe |
| `non_negative_values` / `non_negative_values_percent` | NO | redundant inverse |
| `min_in_range` / `max_in_range` | T2-PROF→NOW | profile **already** has min/max; needs baseline-calibration logic (no promise map for `min_max_value`) |
| `sum_in_range` / `mean_in_range` / `median_in_range` | T2-PROF | `profile_runner` must collect sum / mean / median |
| `sample_stddev_in_range` / `population_stddev_in_range` | T2-PROF | collect stddev |
| `sample_variance_in_range` / `population_variance_in_range` | T2-PROF | collect variance |
| `percentile_in_range` + `_10/_25/_75/_90` | T2-PROF | collect percentiles |

### 4.4 Text / Pattern / Format

| Check | Verdict | Notes |
|---|---|---|
| `text_length_in_range_percent` | *emitted* | |
| `text_length_below_min_length_percent` / `text_length_above_max_length_percent` | NOW | from a one-sided acceptedLength |
| `text_length_below_min_length` / `text_length_above_max_length` (count) | NOW (optional) | count variant |
| `text_min_length` / `text_max_length` / `text_mean_length` | T2-PROF | `min_max_value` rule; profile has min_len/max_len but no mean_len |
| `text_matching_regex_percent` | *emitted* | |
| `texts_not_matching_regex_percent` / `text_not_matching_regex_found` | NO | redundant inverse |
| `empty_text_percent` / `empty_text_found` | NOW | **hidden-null detector** — `''` that `nulls_percent` misses; conformity |
| `whitespace_text_percent` / `whitespace_text_found` | NOW | hidden-null — `'   '` |
| `null_placeholder_text_percent` / `null_placeholder_text_found` | NOW | hidden-null — `'NULL'`, `'N/A'`, `'-'` |
| `text_surrounded_by_whitespace_percent` / `_found` | NOW | leading/trailing whitespace; conformity |
| `min_word_count` / `max_word_count` | NO | niche; no declaration to drive it |
| `invalid_email_format_percent` (+ uuid/ip4/ip6/usa_phone/usa_zipcode) | NOW | route `format=email` etc. here — **more precise than the generic regex** the emitter uses now |
| `invalid_*_format_found` (count variants) | NOW (optional) | count-shaped variant |
| `contains_email_percent` / `contains_usa_phone` / `_usa_zipcode` / `_ip4` / `_ip6` | NOW | **PII-leakage detector** — emit on free-text columns; flags PII in a field not classified as PII |

### 4.5 Boolean / Geographic / DateTime

| Check | Verdict | Notes |
|---|---|---|
| `true_percent` / `false_percent` | T2-DECL | needs a declared expected ratio — a 50/50 guess is not a promise |
| `invalid_latitude` / `invalid_longitude` | T2-DECL | needs the column tagged as lat/long (declaration or name heuristic) |
| `valid_latitude_percent` / `valid_longitude_percent` | T2-DECL | same |
| `date_values_in_future_percent` | NOW | datetime column, conformity — "future timestamps are suspect" needs no declaration |
| `date_in_range_percent` | NOW + T3-INFER | **GAP**: the `check_emitter` docstring (line 20) promises this but no code path exists. NOW = acceptedRange on a date column; T3 = inferred date range |

### 4.6 Accepted values / Datatype

| Check | Verdict | Notes |
|---|---|---|
| `text_found_in_set_percent` / `number_found_in_set_percent` | *emitted* | |
| `expected_text_values_in_use_count` / `expected_numbers_in_use_count` | NOW (optional) | "every declared value actually appears"; needs acceptedValues |
| `expected_texts_in_top_values_count` | NO | niche |
| `text_valid_country_code_percent` / `text_valid_currency_code_percent` | *emitted* | |
| `text_not_matching_date_pattern_percent` / `_found` | NOW | when `format=iso_date` on a **text** column |
| `text_match_date_format_percent` | NOW | same family |
| `text_not_matching_name_pattern_percent` | NO | niche |
| `text_parsable_to_boolean/integer/float/date_percent` | NOW (conditional) | validity — when logicalType says numeric/date but data is stored as text |
| `detected_datatype_in_text` | NO | advisory, not a scored promise |
| `detected_datatype_in_text_changed` | NO | change/history |

### 4.7 Cross-table / Change / Anomaly / Custom SQL

| Family | Verdict | Notes |
|---|---|---|
| `total_row_count_match_percent` | *emitted* | the one cross-table check the engine has an input for |
| `total_sum/min/max/average/not_null_count_match_percent` | T2-DECL | extend `consistentWith` to carry column-level mappings |
| `row_count_match` / `column_count_match` / `sum/min/max/mean_match` / `*_count_match` | T2-DECL | same — needs reference table + column |
| `foreign_key_found_percent` / `foreign_key_not_found` | **T2-DECL** | needs a `references` field on `FieldDeclaration` — **high value, unlocks `accuracy`** |
| all `*_change*` | NO | history |
| all `*_anomaly` | NO | history + excluded from scoring |
| all `sql_*`, `import_custom_result_*` | NO | user-authored SQL / external |

---

## 5. Proposed work

### Tier 1 — new emit branches only (no schema, no profiler changes)

High-value subset (recommended), all in `check_emitter.py`:

1. **`data_staleness`** — in `_emit_table_checks`, alongside `data_freshness`,
   pass `freshnessColumn` as the `timestamp_column` parameter.
2. **Hidden-null detectors** — `empty_text_percent`, `whitespace_text_percent`,
   `null_placeholder_text_percent` on every non-PII text column when
   `completeness` or `conformity` is promised. These catch dirty data
   `nulls_percent` cannot see.
3. **`date_values_in_future_percent`** — on every datetime column when
   `conformity` is promised.
4. **`date_in_range_percent`** — close the documented gap: emit when
   `acceptedRange` is declared on a date/timestamp column (mirror of the
   existing `number_in_range_percent` branch).
5. **`column_types_changed`** — in `_emit_table_checks` when `consistency`
   promised; type drift, additive to `column_list_changed`.
6. **`table_availability`** — when `coverage` promised; table-level, no params.
7. **`duplicate_record_percent`** — table-level full-row dedup when
   `uniqueness` promised and no PK declared.

Lower-value Tier 1 (optional, defer): one-sided range/length variants,
`invalid_*_format_percent` re-routing, `contains_*_percent` PII-leakage,
`column_exists` per field, `duplicate_percent`.

**Risk:** low. Each is a new gated branch; the promised-dims gate and
`dimension`-from-`check_type` derivation already prevent churn. Needs the
matching check types verified runnable on PostgreSQL/MySQL/Oracle (the
project's existing E2E matrix).

### Tier 2 — new inputs

- **`foreign_key_found_percent`** — add `references: {product, table, column}`
  to `FieldDeclaration`; emit when present; maps to `accuracy`. This is the
  single highest-value item: `accuracy` currently *always* resolves to
  `not_assessed` / `REASON_NO_AUTHORITATIVE_SOURCE`.
- **Statistical in-range** (`mean/median/sum/stddev/variance/percentile_in_range`)
  — extend `profile_runner` to collect those aggregates (one extra round-trip
  or wider SELECT), add baseline-calibration in the emitter. `min_in_range` /
  `max_in_range` need only the calibration half — min/max are already profiled.
- **Cross-table `total_*_match`** — extend `consistentWith` schema to carry
  column-level mappings.

**Risk:** medium. `profile_runner` change touches the customer-DB query path
and the opaque-alias re-keying; needs care with PII columns (stats must stay
suppressed for PII, as `count`/`distinct` already are).

### Tier 3 — date-range inference

Add `infer_date_range()` to `inference_engine.py` (parallel to
`infer_numeric_range`), feeding the inferred branch of `date_in_range_percent`.

**Risk:** low, isolated to `inference_engine.py`.

---

## 6. What this does NOT do

It does not add the ~120 change/anomaly/custom-SQL/cross-table checks. They
require a time-series history, a user-authored expression, or a declared
reference the contentSchema does not carry. Emitting them would render broken
SQL and surface phantom failures — the precise defect migrations `015`–`017`
removed. They remain available for **manual** check creation via the API;
they are simply outside the deterministic auto-emit flow.

---

## 7. Recommendation

Land **Tier 1** first — low risk, no schema migration. Then **Tier 2
`foreign_key_found_percent`** as a separate change (the only way to make the
`accuracy` dimension assessable; needs a contentSchema change reviewed on its
own). Defer the statistical-in-range family and cross-table matches until
there is demonstrated demand.

## 8. Implementation status

**Shipped** — engine emits **25** check types (was 15). All in
`check_emitter.py` + `inference_engine.py` + the `profile_routes.py` wiring,
no DB migration, no connector change. New types:

| Check | Trigger | Dimension |
|---|---|---|
| `empty_text_percent` | every text column | conformity |
| `whitespace_text_percent` | every text column | conformity |
| `null_placeholder_text_percent` | every text column (word list `NULL/N/A/NA/NONE`) | conformity |
| `text_surrounded_by_whitespace_percent` | every text column | conformity |
| `date_values_in_future_percent` | every date/timestamp column | conformity |
| `date_in_range_percent` | acceptedRange on a date column, or inferred date range | conformity |
| `column_exists` | per declared field | conformity |
| `column_types_changed` | consistency promised | consistency |
| `table_availability` | coverage promised | coverage |
| `duplicate_record_percent` | uniqueness promised, no PK declared | uniqueness |

Supporting change: `inference_engine.py` gained `infer_date_range()` +
`DateRange`; `InferenceResult.date_range` and the `/profile/run` ↔
`/checks/emit-deterministic` payloads carry it.

Closed the documented gap: `date_in_range_percent` (promised in the module
docstring, never implemented) now has a real code path — rule 4 branches on
column shape (date → `date_in_range_percent`, else → `number_in_range_percent`).

**Not shipped — rationale:**

- `data_staleness` — with only one declared timestamp column it measures the
  same thing as `data_freshness`; emitting it = a duplicate check. Genuinely
  distinct only with a separate ingestion-timestamp column (Tier 2 input).
- `column_list_or_order_changed` — strictly stronger duplicate of the
  already-emitted `column_list_changed`; emitting both double-counts consistency.
- `foreign_key_found_percent`, cross-table `total_*_match`, statistical
  in-range, `true/false_percent`, lat/long — all need an input that does not
  exist yet (a `references` field, extended `consistentWith`, extra
  `profile_runner` aggregates, or a declared expected ratio). Adding the emit
  branch without the input produces zero checks. Tracked as Tier 2.
- `invalid_*_format_percent`, `text_not_matching_date_pattern_percent`,
  `text_parsable_to_*` — these would *replace* the existing regex routing, not
  *add* coverage; a swap with test churn for no new dimension signal.
- `contains_*_percent` (PII leakage) — valuable but emitting 5× on every text
  column unconditionally is spray-and-pray; wants a targeted "must not contain
  PII" declaration first.
