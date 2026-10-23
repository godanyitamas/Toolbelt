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

## Decision

- **One raw table, one shared writer:** `batch_telemetry`, one row per input object consumed by one batch: a file; a message topic/partition range; an upstream Delta batch within the declared source/plant scope.
- **Facts:** `batch_facts`, exactly one flat row per (`query_id`, `batch_id`), scalar `source_type`, with ready-to-use means, bounds, coverage and completion ages built alongside mergeable sums/counts.
- **Relationships:** `batch_dependencies`, observed producer-batch → consumer-batch edges per consumed Delta table.
- **Derived objects start as views**; materialize only when measured cost or retained history requires it.
- **Identity:** (`query_id`, `batch_id`) from the SparkContext local property `sql.streaming.queryId` read on the batch thread; `job_id` for continuity; run ids per attempt; job/task names as labels only.
- **Stamp on write, record on read:** producers propagate their query/batch identity in output data; consumers record the upstream keys they read.
- **Telemetry idempotency only:** telemetry appends use `txnAppId` = query id, `txnVersion` = batch id. Module data-write transaction ids are not changed in this migration.
- **Messages:** source type `MESSAGE` with `message_*` columns and `message_system` (`KAFKA` | `EVENT_HUBS`); semantics are Kafka-consumer-API semantics. Histogram details: `decisions/message-histogram-contract-v1.md`.

## Why

- Data-borne keys made the second (listener) table unnecessary: it answered a missing-data problem, not a storage need.
- Membership arrays cannot attribute batch-wide metrics to plants or sources; exploding them duplicates duration and workload.
- With several queries per session, a session-wide "current id" can be overwritten by the last-started stream; as a `txnAppId` that can make Delta silently skip writes.
- The hand-written name + date `txnAppId` mixes unit and continuity identity: a checkpoint reset with an unchanged date skips writes; a changed date with a kept checkpoint duplicates a replayed batch. Fixing that is a separate, riskier change than telemetry.

## Consequences

- Batch ancestry is resolvable from stamps; exact contributing files/messages for a downstream output are not.
- `sql.streaming.queryId` is an internal key (a runtime upgrade could remove it) and requires `sparkContext`, so dedicated job clusters; not available under Spark Connect.
- Facts correctness depends on whole-batch formation, which is not established when Delta readers split one telemetry commit.
- Backlog ("what's waiting") needs evidence beyond successful-input rows: inventories or broker end offsets.

## What would make me reconsider

- H1 fails: the local property is not stable across restarts, not new after a checkpoint reset, or not distinct per concurrent query.
- Replay tests (H2/H3) show telemetry duplication or interaction with module writes.
- Commit-split experiments show partial facts that cannot be reconciled cheaply.
- A streaming stateful operator is introduced (outputs could derive from earlier batches).
- A move to serverless or standard access mode removes `sparkContext`.
