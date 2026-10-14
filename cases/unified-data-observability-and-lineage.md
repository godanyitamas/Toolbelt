---
type: case
status: active
created: 2026-10-11
topics:
  - observability
  - data-lineage
  - spark-structured-streaming
  - delta-lake
  - kafka
  - databricks
  - freshness
  - file-ingestion
---

# Designing unified processing telemetry across files, Kafka, and Delta

> **Handoff status:** The design is settled enough to build: stamp on write, record on read, one table. A POC is being implemented in the shared core module (schema v0 exists, migration path defined). **Behavior is not yet verified.** Open review items and hypotheses are listed below. Rejected alternatives are recorded so they are not re-proposed without new evidence.

## Problem

The existing `processed_files` reporting concept is useful for file-based processing, but it does not cover all relevant source types. The goal is to extend the telemetry foundation to file, Kafka, and Delta-based pipelines so that operational monitoring and lineage investigations can use consistent concepts without forcing incompatible source semantics into an artificial universal model.

The system should help answer:

- Is data being processed and made available in time for downstream consumers?
- Is delay caused by late arrival, time waiting, or slow processing?
- Which expected inputs have not been processed?
- How is processing behavior changing over time, and how does it relate to workload properties such as file size?
- Which source, processing stage, and execution are associated with a downstream result?
- When someone asks which files contributed to a downstream result, what level of lineage can we reliably provide?
- Can telemetry support alerting, support workflows, and reporting without bespoke monitoring jobs per datasource?

---

## Initial model (before the design sessions)

Preserved as written at the start; later sections show what was challenged.

- I want a unified telemetry foundation that can be joined across pipeline stages. Every source does **not** need the same grain or the same metadata.
- Natural grains: files = one row per file; Kafka = possibly one row per message; Delta = one row per micro-batch with the consumed source position captured separately.
- I was skeptical of a `processing_observations` / `processed_inputs` split with an `observation_id`: introduced before a need was established. Start with one table.
- No `output_id` by assumption: the shared reporting code runs before the job-specific writes and does not know the outputs, especially for multi-target jobs.
- No `status` column without a specific need.
- For files, a batch identity might not be necessary because the row grain is already the file.

---

## Observations and constraints

### Existing approach and motivation

- `processed_files` is file-oriented.
- Other monitoring uses custom metrics and separate monitoring jobs. Some alerts depend on a monitoring job crashing, which detects a failed implementation rather than the business condition.
- Per-datasource monitoring logic creates operational and maintenance overhead.
- A support-team morning email is an existing delivery surface; business dashboards may also benefit.
- At least one delay was missed, which motivates better freshness observation.
- Critical sources have downstream consumers; late or bad data has reputational impact.
- Databricks Data Quality Monitoring has been considered; no decision. Concern: profiling cost on large tables.

### Facts about the current setup

- Every pipeline (file, Kafka, Delta) runs through a shared `foreachBatch` reporting wrapper in a shared **core** module that all job modules extend. Reporting lives in core.
- The wrapper builds its record at batch start and writes it at batch end.
- The wrapper does not know which outputs a job writes; some jobs write several tables.
- Some jobs run **multiple streaming queries** in one Spark session.
- Jobs run on dedicated job clusters. All writes are append-only.
- Jobs perform multiple writes inside one `foreachBatch`, sequentially on the batch thread, each to a different table; no job writes the same table twice in one batch. No query uses streaming stateful operators, and no job writes in parallel threads (Futures / thread pools). Established by reading the current modules, not yet checked at runtime.
- Most jobs in all layers already propagate the `foreachBatch` `batchId` (a long) as a column in their output data.
- Idempotent Delta writes currently use `txnAppId` = a hand-written job name with a launch-date postfix, and `txnVersion` = `batchId`.
- Kafka Bronze persists the raw, non-deserialized message together with topic, partition, offset and timestamp.
- Consumed Delta/Parquet data exposes `_metadata` columns: file path, row index within the file, file size, file timestamp.
- Databricks identifiers are read from the SparkSession conf; they are empty strings when not provided (local testing).

### Design constraints (my decisions)

- Do not derive identity from the checkpoint location.
- Do not rely on hand-written job names for identity.
- No `output_id`, `status`, or generated keys without a concrete need.
- One table, one writer.
- Capture now what cannot be recovered later (e.g. file size); compute the rest downstream.
- Prefer explicit, type-specific columns over generic columns that mix different semantics.

### Source-specific facts

- **Files:** path, size, optional filename/functional date, plant/fab identity from the source-folder convention. A path may not identify a unique file instance if files can be overwritten. Detecting never-processed files needs an inventory.
- **Kafka:** message identity is topic + partition + offset; keys repeat or are null.
- **Delta:** commit versions, data files, downstream micro-batches and an upstream job's `batchId` are distinct identities. A Gold query does not inherit Silver's `batchId`.

---

## Design

### Principles

1. **Capture now only what cannot be recovered later**; derive everything else in a downstream gold aggregation.
2. **Unify the questions, not the metadata**: one row shape, explicit type-specific columns, no column whose meaning changes by row.
3. **Identity comes from Spark and the platform**, never typed by hand.
4. **Carry identity in the data across hand-offs** instead of reconstructing it from timing or logs.

### Identity

| Identity | Columns | Lifetime | Supplied by |
|---|---|---|---|
| Processing unit | (`query_id`, `batch_id`) | stable across restarts while the checkpoint persists; a replayed batch reuses it; a new checkpoint gives a new query id | Spark |
| Continuity over time | `job_id` (+ further columns as needed) | stable until the job is recreated | platform conf |
| Attempt | `job_run_id`, `task_run_id` | per run | platform conf |
| Human-readable label | `job_name` | as stable as the platform job name | platform conf |

**Query id: read from the SparkContext local property inside the batch (implemented).**

```scala
val qid = batchDF.sparkSession.sparkContext.getLocalProperty("sql.streaming.queryId")
require(qid != null, "not inside a streaming batch")
```

- Each query runs its micro-batches on its own thread, so concurrent queries in one job each see their own id.
- Chosen over a session-wide listener: with several queries per session, a single "current id" in a listener is overwritten by whichever stream started last. Because the query id is also `txnAppId`, a mixed-up id would not just corrupt telemetry: Delta can skip writes as "already committed" (silent data loss).

**Use the query id as `txnAppId`.** Delta skips a write whose `txnVersion` is at or below the last one recorded for its `txnAppId`, so the app id must change exactly when the checkpoint is reset:

- checkpoint reset but hand-written date unchanged → batch ids restart at 0 and writes are skipped (silent data loss);
- date changed but checkpoint kept → a batch replayed after the relaunch is written twice.

The query id has exactly the required lifetime. The same key identifies a batch in Delta's transaction records, in the data, and in telemetry.

**Process continuity.** The query id resets with a new checkpoint, so it cannot identify a process over time. `job_id` provides continuity until a job is recreated (e.g. Terraform replace, workspace move); a small process dimension (`job_id` → logical process) absorbs that. Consumers select further columns (e.g. `source_id`) to distinguish streams within a job.

### Stamp on write, record on read

- **Stamp on write:** every output row carries the producing batch's `_query_id` and `_batch_id`.
- **Record on read:** every batch writes telemetry rows describing what it consumed: files, Kafka partition ranges, or upstream units (distinct `_query_id`/`_batch_id` in its input).

The data carries only the immediate producer's key; the chain lives in telemetry. Aggregations are fine: Gold's output loses individual Silver keys, but Gold recorded them when it read them.

Scope: this holds because all queries are stateless, so every batch's output is computed only from that batch's input ("what I read" = "what my output came from"). A streaming stateful operator (watermarked aggregation, stream-stream join, `*WithState`) would break both lineage and the input contract: outputs would derive from earlier batches, and the raw input would not be reachable inside `foreachBatch`.

**Multiple outputs stop mattering.** A producer stamps every row of every table it writes, so it never needs to know its outputs. Producer → table relationships appear from readers' rows (`source_id` + upstream key). Exception: terminal tables nobody streams from; their freshness is the writer's end time, and table → writer comes from the dimension or Delta history.

### Grain: one row per input object consumed by one batch

| Source type | Input object | Rows per batch |
|---|---|---|
| File | one landing file | one per file |
| Kafka | one topic-partition range | one per partition |
| Delta | one upstream batch (unit) | one per upstream unit consumed |

- **Offset-level Kafka detail comes from Bronze itself**, which already behaves like a per-message telemetry table. Lookup: Bronze rows where `_query_id` = `query_id`, `_batch_id` = `batch_id`, `partition` = `kafka_partition`.
- Kafka modules compute min/max offsets, min/max timestamps and row counts from the offset-level data, so counts are not inferred from offset spans (offsets can have gaps).
- If `maxFilesPerTrigger` splits an upstream batch's output, it appears in two downstream batches with partial row counts.

### Schema v0 (implemented in the POC)

```text
-- consuming stream
query_id              String     not null
batch_id              Long       not null
job_name              String     not null
job_id                String     not null   empty string when absent
job_run_id            String     not null   empty string when absent
task_run_id           String     not null   empty string when absent
batch_start_ts        Timestamp  not null
batch_end_ts          Timestamp  not null
batch_row_count       Long       not null
source_type           String     not null   file | kafka | delta
source_id             String     not null   folder root | topic | table

-- files
file_path             String     nullable
file_modification_ts  Timestamp  nullable
file_functional_ts    Timestamp  nullable
file_size_bytes       Long       nullable

-- kafka
kafka_partition       Int        nullable
kafka_min_offset      Long       nullable
kafka_max_offset      Long       nullable
kafka_min_ts          Timestamp  nullable
kafka_max_ts          Timestamp  nullable

-- delta
upstream_query_id     String     nullable
upstream_batch_id     Long       nullable   (was String; agreed to change)
```

Physical layout: to be determined.

- **Unique key:** (`query_id`, `batch_id`, `source_id`) + object (`file_path` | `kafka_partition` | (`upstream_query_id`, `upstream_batch_id`)).
- **Writes:** one append per batch with `txnAppId` = query id and `txnVersion` = batch id, so a replayed batch cannot duplicate telemetry.
- **Sparse nulls are intended.** Cost is negligible; the real risk is integrity, guarded with one CHECK constraint per type, e.g.:

```sql
ALTER TABLE telemetry ADD CONSTRAINT file_shape CHECK (
  source_type <> 'file' OR
  (file_path IS NOT NULL AND kafka_partition IS NULL AND upstream_batch_id IS NULL));
```

- **Width growth** is the watch item; fine for three or four source types.
- **Not stored (recoverable or derived):** logical process (dimension); plant/fab (parsed from path); Delta arrival (upstream batch's `batch_end_ts`); durations, waits, lag (gold aggregation); output tables (readers' `source_id`).

### Schema review items

| # | Item | Status |
|---|---|---|
| R1 | `upstream_batch_id` String → Long (every lineage hop joins it to `batch_id`) | Agreed |
| R2 | `batch_row_count` is a batch fact repeated on every row (never sum it across a batch's rows). Per-object counts are needed for Kafka partitions and split upstream batches. Either rename to an object-level count used for every type, or add `object_row_count` next to it | Open |
| R3 | Bronze Kafka rows referencing themselves via `upstream_*`: proposed against, because "upstream = myself" mixes semantics and the lookup above already reaches the messages. Null `upstream_*` = input from outside the lakehouse | Open |
| R4 | Empty-string sentinel for missing Databricks ids hides from `IS NULL` and groups all local runs together. Either nullable + null, or ensure local runs never write to the shared table | Open |
| R5 | `job_name` must be the platform job name, not the dated `txnAppId` string | To confirm |
| R6 | `file_functional_ts`: keep if read from file content; if parsed from the file name it is recoverable (fine to keep as convenience) | To confirm |
| R7 | Kafka timestamp type (create vs log-append) not stored; keep it in the source dimension so waits can be interpreted | Open |
| R8 | Candidate `kafka_bytes` (sum of message sizes), the Kafka equivalent of file size | Candidate |
| R9 | A stable task identifier (`task_key`) if multi-task jobs need separating over time; `task_run_id` changes per run | Open |

### Column rationale

**`source_id`** = the dataset read; same meaning for every type. Uses: part of object identity (a Kafka partition is meaningless without its topic); the only place table names appear in lineage; business-facing grouping key for freshness; distinguishes streams within a job; scope for missing-file detection.
Rules: Delta = full three-part Unity Catalog name; Kafka = topic (prefix cluster if several); files = folder root at the level where expectations are defined. Capture it where the stream is defined (shared read helper); tag each input with `lit(source_id)` before a union or join.

**Kafka columns:** partition (offsets, lag, ordering are per partition; topic aggregates hide a stuck partition); min/max offset (lineage via Bronze lookup; continuity check: next batch's min = previous max + 1, gaps can mean data lost to Kafka retention and silently skipped with `failOnDataLoss` off; lag = partition end offset − max); min/max timestamp (data age, worst wait). All recoverable from stamped Bronze; stored as a deliberate precompute for cheap monitoring that outlives Bronze retention.

**File columns:** modification time is per-file arrival; size is the workload property that is not recoverable once files are removed.

### Derived objects

- **Gold aggregation** over the telemetry table: per batch (duration, worst wait, volume), per process (staleness), per source (data age), lineage hops.
- **Process dimension:** `job_id` (+ `source_id`) → logical process, owner, expected cadence/deadline, tables written (needed for terminal tables). Business can own expectations; engineering owns collection.

### Freshness

1. **What's done:** duration `end − start`; worst wait `start − earliest arrival` (file modification time, Kafka min timestamp, Delta upstream batch end); staleness `now − max(batch_end_ts)` per process; data age per source.
2. **What's waiting:** Kafka lag (partition end offset − max consumed); files = inventory entries with no row; Delta: not answered in units. Telemetry records what a batch read, not what it wrote, so a unit that wrote zero rows to a table cannot be told apart from one not yet consumed. v1 answers Delta freshness in time: staleness per process and delay (downstream `batch_end_ts` − upstream `batch_end_ts`).

Alert when staleness exceeds the expected cadence from the dimension: the business condition, not a crashing monitoring job. "Late arrival" needs an expectation; observed timestamps alone cannot say late.

Caveat: staleness alone cannot tell "pipeline stuck" from "no new input" (a stateless stream with no new data runs no batches, so a low-volume source looks stale overnight). A sharper condition from telemetry alone: downstream is stale **and** upstream has a batch that ended after the latest upstream batch downstream consumed. It can still fire falsely when upstream's newest batches wrote zero rows to the table (same limit as above).

### Lineage

Walk from a Gold batch: Gold rows (`upstream_*` = Silver unit) → Silver's rows (`upstream_*` = Bronze unit; one branch per `source_id` when Silver reads several tables) → Bronze rows (files / Kafka ranges) → landing files by path, or exact messages by the Bronze lookup.

- Gives exactly which upstream batches and objects a batch read (level B); level A via timestamps.
- Not per record (level C); if ever needed, file path + row index from `_metadata` is an exact record address.
- Limits: horizon bounded by Bronze retention; Kafka offsets unique only while a topic exists; legacy data without `_query_id` falls back to (`source_id`, `batch_id`) + time ordering.

### Guardrails

- **Id mix-up check:** batches of one query never overlap in time. Overlapping windows for the same `query_id`, or the same (`query_id`, `batch_id`) from different streams, mean ids were mixed.
- **Kafka continuity check:** per (`source_id`, `kafka_partition`), each batch's min offset follows the previous batch's max offset.
- **CHECK constraints** per source type.
- **Stateless check:** core asserts that the query's progress reports no state operators (`stateOperators` empty), so the scope condition on aggregations is enforced, not assumed.
- **Identity captured once:** the wrapper reads the query id on the batch thread and passes it down as a value; modules never read the local property themselves (local properties are copied to child threads only at thread creation, so a reused pool thread can carry another query's id).
- **One idempotent write per (table, batch):** a second write to the same table with the same `txnAppId`/`txnVersion` is skipped silently. If ever needed: union first, or use a fixed write-site suffix on the app id.
- **Telemetry written last**, so a replay never leaves telemetry timestamps from a failed attempt.
- **Writer options only:** `txnAppId`/`txnVersion` are set per write, never as session confs (which would apply to every Delta write in the session).

### Example (illustrative values)

| Batch | Window | Telemetry rows (consumed) | Output stamp |
|---|---|---|---|
| Bronze `qB/4812` | 10:00:05–10:03:40 | file `a_1010` 412 MB; file `b_1010` 3 MB | `bronze.readings`: `qB/4812` |
| Bronze `qK/20931` | 10:01:00–10:01:07 | kafka `topic_x` partition 3, offsets 88100–88949 | `bronze.events`: `qK/20931` |
| Silver `qS/377` | 10:04:10–10:09:30 | `bronze.readings` ← `qB/4812`; `bronze.events` ← `qK/20931` | `silver.readings`: `qS/377` |
| Gold `qG/52` | 10:10:00–10:12:20 | `silver.readings` ← `qS/377` | `gold.kpis`: `qG/52` |

Gold 52 traces back to `qS/377` → `qB/4812` (files `a_1010`, `b_1010`) and `qK/20931` (partition 3, offsets 88100–88949).

---

## Migration plan

- **Core owns reporting.** Legacy `processed_files` reporting is now optional, disabled through an existing parameter. New reporting is mandatory: modules must provide their input dataset, and core computes the telemetry rows.
- **Dual reporting** is supported, so each job migrates to the new core version once; legacy reporting can then be switched off or its code removed.
- **Validate the input contract in core:** per `source_type`, check required columns (`_metadata` for files; partition/offset/timestamp for Kafka; `_query_id`/`_batch_id` for Delta) and fail with a clear message, or fall back to null `upstream_query_id` for legacy Delta data.
- **Migrate producers before consumers** (Bronze → Silver → Gold): a consumer can only record `upstream_query_id` once its upstream stamps `_query_id`.
- **Switch `txnAppId` to the query id after a clean stop**, so no batch written under the old app id replays under the new one.
- **Reconcile before switching legacy off:** compare legacy `processed_files` with new file rows per batch (paths, counts, sizes) for a period.

---

## Alternatives considered and set aside

| Alternative | Why considered | Why set aside | Status |
|---|---|---|---|
| Two tables: `slice` (wrapper) + `progress` (StreamingQueryListener) | Delta offsets appeared reachable only via progress events | Data-borne stamp makes offsets unnecessary | Fallback; draft `ProgressListener.scala` exists |
| Query id from a session-wide listener | Simple propagation into the batch | Jobs run multiple queries; one shared "current id" can be overwritten, and the id is also `txnAppId` | Replaced by local property; usable only as a cross-check keyed by query name |
| Commit `userMetadata` stamp | Link commits to producing batch | Superseded by stamping the data | Possible tie-breaker for multi-writer tables |
| Modification-time window matching | Link data files to producer batch windows | Heuristic; needs table → writer mapping | Fallback for jobs without stamp columns |
| Delta one row per consumed data file | Per-file arrival and lineage without offsets | Heavier; upstream unit is exact | Fallback |
| Producer record from the target table's Delta log (`txn` actions carry query id + batch id) | Unit-level "what's waiting" for Delta without `output_id` | v1 answers Delta waiting in time; needs verification (empty appends, log retention) | Fallback if unit-level pending is ever needed |
| Kafka one row per message | Per-message detail | Duplicates Bronze at data volume | Rejected |
| Reading checkpoint `metadata` for query id | Simple source of query id | Violates constraint | Rejected |
| Generic columns / struct per type / table per type | Avoid sparse nulls | Mixed semantics / clumsier SQL / UNION everywhere | Rejected |

---

## Hypotheses to verify

| # | Hypothesis | POC step |
|---|---|---|
| H1 | The local property returns the persistent query id: stable across restarts, new after a checkpoint reset, distinct per concurrent query | 1 |
| H2 | `txnAppId` = query id gives correct idempotency: replays skipped, nothing skipped after a checkpoint reset, including a kill between two writes of one batch (first skipped on replay, second written) | 2 |
| H3 | Telemetry append with the same `txnAppId`/`txnVersion` cannot duplicate on replay | 2 |
| H4 | Stamping and per-batch aggregations add acceptable overhead | 3 |
| H5 | Recorded upstream units are complete (every input row stamped; split upstream batches appear in both readers) | 3 |
| H6 | Multi-source streams and multi-output jobs are handled by `source_id` tagging and stamping | 4 |
| H7 | The lineage walk returns exactly the true contributing upstream units and files | 6 |
| H8 | The legacy fallback (batch id + time ordering) is acceptable until old data ages out | 7 |
| H9 | New file rows match legacy `processed_files` during dual reporting | 7 |

---

## POC plan

**Step 0: Setup and acceptance queries.** Write the v1 questions as acceptance queries: staleness per process, delay decomposition for one file, missing-file detection, Kafka continuity/lag per partition, lineage from a Gold batch to files and Kafka ranges. Sandbox pipelines on synthetic data whose rows carry their own file name and row number (ground truth), including a job running two queries.

**Step 1: Identity (H1).** Log the query id and `batchId` in every batch of the two-query job; restart; reset one checkpoint. Pass: ids distinct per query, stable across restart, new after reset.

**Step 2: Idempotency (H2, H3).** Use the query id as `txnAppId` for data and telemetry writes. Kill after the data write but before the checkpoint commit; kill between two data writes of one batch; restart. Reset a checkpoint and confirm new writes land.

**Step 3: Stamp and record (H4, H5).** Implement stamping, the consumed-units aggregation, file rows and Kafka partition rows in core. Compare with ground truth; measure per-batch overhead.

**Step 4: Multi-source and multi-output (H6).** A Silver job joining two Bronze tables; a job writing two output tables.

**Step 5: Failure and replay.** Kill mid-batch at different points; confirm one row per object per unit and that the guardrail checks behave.

**Step 6: Gold aggregation and acceptance queries (H7).** Build the gold aggregation, a hand-filled process dimension and CHECK constraints; run the acceptance queries; compare the lineage walk with ground truth.

**Step 7: Migration checks (H8, H9).** Run a stream on legacy data without `_query_id`; run dual reporting and reconcile against `processed_files`.

**Step 8: Evaluate and write up.** Mark each hypothesis confirmed, refuted or unclear with evidence; estimate production volume; update "What changed" and "Conclusion"; consider a `decisions/` entry.

---

## Decisions

**Made:** one table, one writer; grains (file / Kafka partition / upstream unit); offset-level Kafka detail from Bronze; query id from the SparkContext local property; query id as `txnAppId`; `upstream_batch_id` as Long; explicit type-prefixed columns; legacy reporting optional, new reporting mandatory, dual reporting during migration; Delta "what's waiting" answered in time, not units (the Delta-log producer record stays as a fallback, not rejected).

**Pending:** R2–R9 above; physical layout (`partitionBy` / clustering, retention); table name and ownership; rollout order and timing of the `txnAppId` switch.

## Open questions

- Which freshness expectations exist, who owns them, and which conditions alert through which channel?
- Do expectations describe processing cadence or data-arrival cadence? (Staleness alone cannot tell a stuck pipeline from no new input.)
- What should a support engineer answer from the morning email or a dashboard?
- What does the functional/filename date mean, and how are file overwrites handled?
- How will the file inventory be produced for missing-file detection?
- Which Kafka topics use create time vs log-append time?
- Is Bronze retention long enough for the required lineage horizon?
- Where do Databricks DQM features fit, if at all?

---

## What changed in my understanding (draft: confirm or rewrite)

- Identity is several things with different lifetimes (unit, continuity, attempt, label). The hand-written name + date mixed two of them, and as `txnAppId` it is also a correctness risk.
- The query id persists across restarts; `runId` is what changes every run. It lives on the stream's thread, not the session, which matters once a job runs several queries.
- Start/end are batch facts, so per-object rows need a unit key; aggregation belongs in a separate gold process.
- Lineage does not need output knowledge: stamping data with the producer's key and recording consumed keys on read handles multiple outputs.
- The two-table design was a response to a missing piece of data, not a modeling need; once the data carried the key, one table sufficed.
- Working backwards from how the table is used ("what can't be recovered later?") removed most of the machinery.
- (Candidate, from review) Dropping output knowledge is free for backward lineage, but forward questions ("what's pending?") need a producer record from somewhere.

## Connections to existing cases

Potential connections, not evidence that the problem is already solved:

- `cases/productionizing-a-benchmark-driven-streaming-architecture.md`: control-tower tables as correctness/readiness coordination.
- `cases/databricks-cost-intelligence-loop.md`: `job_id`/`job_run_id` in telemetry should enable cost per process or batch.
- `cases/databricks-comment-metadata-ownership.md`: business owns freshness expectations; engineering owns the mechanism.
- `cases/streaming-file-parser-rework.md`: file size vs duration per batch is the signal that would have surfaced large-file pressure earlier.
- `cases/repartitioning-large-delta-table.md`: retention and lifecycle operations bound long-lived assumptions; job recreation during a workspace move would split `job_id` continuity.

## Reusable lesson (provisional)

Unify the questions and semantics the monitoring system must answer, not every source's entity grain or metadata. Define what a row means, and what evidence it contains, before choosing keys, splitting tables, or claiming lineage.

Candidate additions (draft):

- Capture at write time only what cannot be recovered later; derive the rest downstream.
- Separate identities by lifetime and let the platform supply them.
- Carry identity in the data across hand-offs instead of reconstructing it from timing or logs.
- When an identifier also drives correctness (idempotency), source it from the narrowest scope that owns it: the stream's thread, not the session.
