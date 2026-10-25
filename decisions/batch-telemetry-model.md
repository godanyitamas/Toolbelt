---
type: decision
status: active
created: 2026-10-23
topics:
  - observability
  - data-lineage
  - spark-structured-streaming
  - delta-lake
---

# Batch telemetry model: three grains, Spark identity, one raw table

Case: `cases/unified-data-observability-and-lineage.md`

## Context

A file-only `processed_files` reporting table had to be extended to file, message (Kafka; Event Hubs via the Kafka API) and Delta pipelines, so that freshness monitoring, workload trends and batch-level lineage can use common concepts without bespoke monitoring jobs per datasource. All pipelines run through a shared `foreachBatch` reporting wrapper in a shared Core module.

## Constraints

- The wrapper does not know which outputs a job writes; some jobs write several tables, and some run several streaming queries in one session.
- Do not derive identity from checkpoint locations or hand-written job names.
- No `output_id`, `status` or generated keys without a concrete need.
- Capture now what cannot be recovered later; derive the rest.
- No column whose meaning changes by row.

## Options

- **Raw storage:** two tables (wrapper slices + StreamingQueryListener progress) vs one raw table with data-borne keys.
- **Facts grain:** one row per batch vs source/plant-grain facts vs batch rows with membership arrays (`input_scopes`, `upstream_units`).
- **Identity:** session-wide listener vs SparkContext local property vs checkpoint `metadata` file.
- **Write idempotency:** switch every module's data-write `txnAppId` to query id vs use query id only for telemetry.
- **Trend layer (2026-10-25):** hourly trends derived from `batch_facts` vs an hourly table built from raw; derived objects as views vs persisted tables.

## Decision

- **One raw table, one shared writer:** `batch_telemetry`, one row per input object consumed by one batch: a file; a message topic/partition range; an upstream Delta batch within the declared source/plant scope.
- **Facts:** `batch_facts`, exactly one flat row per (`query_id`, `batch_id`), scalar `source_type`, with ready-to-use means, bounds, coverage and completion ages built alongside mergeable sums/counts.
- **Relationships:** `batch_dependencies`, observed producer-batch → consumer-batch edges per consumed Delta table.
- **Hourly trends (2026-10-25):** `hourly_facts`, one row per (`job_id`, `source_id`, processing hour of `batch_end_ts` in UTC), built directly from raw: volume, sums/counts, merged message histograms, exact file size/age percentiles and threshold counts, and processing-time percentiles over the hour's distinct batches. Serves every time-window monitoring purpose.
- **Layout (2026-10-25):** raw clustered on (`job_name`, `batch_date`), `hourly_facts` on (`job_name`, `hour_date`); no partitioning. `job_name` leads because the readers, people and reports, know jobs by name. A renamed job splits name-filtered history; trend continuity uses `job_id`.
- **`batch_facts` and `hourly_facts` are persisted Delta tables (2026-10-25)**; other derived objects (daily rollups, daily file percentiles from raw) start as views and are materialized only when measured cost or retained history requires it.
- **Identity:** (`query_id`, `batch_id`) from the SparkContext local property `sql.streaming.queryId` read on the batch thread; `job_id` for continuity; run ids per attempt; job/task names as labels only.
- **Stamp on write, record on read:** producers propagate their query/batch identity in output data; consumers record the upstream keys they read.
- **Telemetry idempotency only:** telemetry appends use `txnAppId` = query id, `txnVersion` = batch id. Module data-write transaction ids are not changed in this migration.
- **Messages:** source type `MESSAGE` with `message_*` columns and `message_system` (`KAFKA` | `EVENT_HUBS`); semantics are Kafka-consumer-API semantics. Histogram details: `decisions/message-histogram-contract-v1.md`.

## Why

- Data-borne keys made the second (listener) table unnecessary: it answered a missing-data problem, not a storage need.
- Membership arrays cannot attribute batch-wide metrics to plants or sources; exploding them duplicates duration and workload.
- With several queries per session, a session-wide "current id" can be overwritten by the last-started stream; as a `txnAppId` that can make Delta silently skip writes.
- The hand-written name + date `txnAppId` mixes unit and continuity identity: a checkpoint reset with an unchanged date skips writes; a changed date with a kept checkpoint duplicates a replayed batch. Fixing that is a separate, riskier change than telemetry.
- Monitoring jobs and dashboards today recompute their statistics on every refresh. Computing them once into persisted tables is cheap on a clustered, predictively optimized raw table and is expected to make reads faster and monitoring cheaper (expectation, not measured).
- `batch_facts` serves batch-based reports and visualizations; `hourly_facts` serves time-window trends. Hourly is built from raw, not from `batch_facts`: facts add nothing for mergeable hourly statistics, file percentiles need raw anyway, and an hourly aggregation of raw does not depend on whole-batch formation (except processing time).
- Hourly rows key on `job_id`, not `query_id`: the query id changes after a checkpoint reset, which would break the trend line for the same job and source.
- Daily file percentiles are not needed by current requests; a view over raw, pruned by `batch_date`, suffices while raw is retained.

## Consequences

- Batch ancestry is resolvable from stamps; exact contributing files/messages for a downstream output are not.
- `sql.streaming.queryId` is an internal key (a runtime upgrade could remove it) and requires `sparkContext`, so dedicated job clusters; not available under Spark Connect.
- Facts correctness depends on whole-batch formation, which is not established when Delta readers split one telemetry commit.
- Backlog ("what's waiting") needs evidence beyond successful-input rows: inventories or broker end offsets.
- Persisted derived tables are only as correct as their refresh: late or replayed batches and split commits must rewrite recent hours and batches (e.g. `replaceWhere` over a lookback) rather than append only. Both builds carry the integrity checks (duplicate raw rows, one source type per batch, histogram totals vs counts).
- Daily file percentiles cannot be derived from hourly rows and are available only while raw is retained. `batch_date` is a UTC day; a local-day report spans two `batch_date` values.
- Hourly processing-time percentiles assign each batch to the hour it ended.

## What would make me reconsider

- H1 fails: the local property is not stable across restarts, not new after a checkpoint reset, or not distinct per concurrent query.
- Replay tests (H2/H3) show telemetry duplication or interaction with module writes.
- Commit-split experiments show partial facts that cannot be reconciled cheaply.
- A streaming stateful operator is introduced (outputs could derive from earlier batches).
- A move to serverless or standard access mode removes `sparkContext`.
- The derived-table builds turn out costly, or monitoring reads do not get cheaper.
- Daily or longer-window file percentiles are needed beyond raw retention (→ file histograms).
- The rewrite lookback for late batches cannot be bounded.
