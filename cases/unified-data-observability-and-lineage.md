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

> **Handoff status (2026-10-19):** The POC uses module-specific builders and one shared raw telemetry writer in Core. Local contract and storage-normalization edits exist, but compilation/regression tests were blocked by unavailable Maven; deployment, replay, identity lifetime and performance remain unverified. The raw grains and telemetry identity are chosen; derived-facts grain and incremental completeness are not settled. Identity edges establish batch ancestry, not exact contributing-file or downstream statistical populations.

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

## What I currently think

The useful common envelope is identity, processing timing and input volume, followed by source-specific evidence. A shared row shape and stable keys are not enough: each metric needs a declared grain, statistical population, timestamp meaning and coverage. Local stage statistics, ancestral context, processing cadence and backlog answer different questions. The investigation update below records why I no longer treat them as interchangeable.

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

- Every pipeline (file, Kafka, Delta) runs through a shared `foreachBatch` reporting wrapper in a shared **Core** module that all job modules extend. Each implementing module builds its source-specific telemetry; Core stamps the consuming identity/timing, normalizes storage types and writes the shared raw table.
- The source builder receives the input Dataset, reporting config and the same batch-start `Instant` as processing. Reporting writes at batch end; age calculations must not silently use a later reporting-time clock.
- The wrapper does not know which outputs a job writes; some jobs write several tables.
- Some jobs run **multiple streaming queries** in one Spark session.
- Jobs run on dedicated job clusters. All writes are append-only.
- Jobs perform multiple writes inside one `foreachBatch`, sequentially on the batch thread, each to a different table; no job writes the same table twice in one batch. No query uses streaming stateful operators, and no job writes in parallel threads (Futures / thread pools). Established by reading the current modules, not yet checked at runtime.
- One Kafka ingestor module uses a different config type from all other modules.
- Most jobs in all layers already propagate the `foreachBatch` `batchId` (a long) as a column in their output data.
- Idempotent Delta writes currently use `txnAppId` = a hand-written job name with a launch-date postfix, and `txnVersion` = `batchId`.
- Kafka Bronze persists the raw, non-deserialized message together with topic, partition, offset and timestamp.
- The production legacy `processed_files` table (file-ingesting jobs only, roughly 50–75 file datasources) is about 65 GB with many small files, most likely not maintained by OPTIMIZE/VACUUM. Not yet broken down: live data vs old file versions vs per-file overhead (`DESCRIBE DETAIL`: `sizeInBytes`, `numFiles`, row count).
- Downstream readers of the telemetry table will include ad-hoc queries, monitoring jobs and one large archiving job. Goal: move monitoring jobs and dashboards onto the gold aggregation rather than the raw table.
- Consumed Delta/Parquet data exposes `_metadata` columns: file path, row index within the file, file size, file timestamp.
- Databricks job identifiers are **not** available from the SparkSession conf: `conf.getOption("spark.databricks.job.*")` returned null on a real job run. Read as SparkContext local properties instead, job id, job run id and task run id are returned; there is no job name. (Observed in a JAR job; which local property is the job run vs the task run is not yet verified.)
- The query id local property (`sql.streaming.queryId`) returned a value both locally and on Databricks. This shows the mechanism works, not yet that the value has the persistent lifetime (H1).

### Design constraints (my decisions)

- Do not derive identity from the checkpoint location.
- Do not rely on hand-written job names for identity.
- No `output_id`, `status`, or generated keys without a concrete need.
- One table, one writer.
- Capture now what cannot be recovered later (e.g. file size); compute the rest downstream.
- Prefer explicit, type-specific columns over generic columns that mix different semantics.
- `sql.streaming.queryId` is an internal Spark key, not a public API: a runtime upgrade could rename or drop it (fails loudly through the `require`).
- `sparkContext` is not available under Spark Connect (serverless / standard access mode), so both the query id and the local-property job ids depend on dedicated job clusters. A constraint for any future compute migration.

### Source-specific facts

- **Files:** path, size, optional filename/functional date, plant/fab identity from the source-folder convention. A path may not identify a unique file instance if files can be overwritten. Detecting never-processed files needs an inventory.
- **Kafka:** message identity is topic + partition + offset; keys repeat or are null. Kafka timestamps can mean create time or log-append time, not necessarily time spent waiting in Kafka.
- **Delta:** commit versions, data files, downstream micro-batches and an upstream job's `batchId` are distinct identities. A Gold query does not inherit Silver's `batchId`.

---

## Investigation update: 2026-10-19

### Evidence inspected and its limits

- **Observed in local builders:** a file builder repeated the batch's file count on every file row. Summing those rows overcounted inputs. For a `binaryFile` Dataset, one row represents one file, so each file telemetry row needs `input_unit_row_count = 1`. A parsed-file Dataset can have many rows per file; that is a different input-row population.
- **Observed storage failure:** `SUM(DECIMAL(24,6))` produced `DECIMAL(34,6)`, while the persisted `BigDecimal` contract was `DECIMAL(38,18)`. Attaching a case-class encoder with `.as[CaseClass]` did not normalize the underlying DataFrame expression type. The custom decimal annotation affected the TableBuilder schema, not the incoming expression.
- **Local change, not deployed confirmation:** the writer now explicitly casts to the storage contract. An existing table still needs schema migration; Decimal/null round-trip and deployed behavior have not been confirmed. Nullable persisted numeric fields use boxed numbers, while input fields use `Option`.
- **Observed in generalized file, Kafka and Delta exports:** Kafka input-message counts matched non-null timestamp counts in the inspected sample. This establishes coverage for that sample only, not a guarantee for all inputs.
- Some Delta input rows lacked upstream query identity. Other rows had complete identities pointing to historical producer batches outside the export interval. Unknown identity and an unresolved known reference are separate coverage failures; neither proves that telemetry is globally missing.
- The same upstream batch appeared in two consumer batches, consistent with split reads. This does not establish exact per-file or per-message attribution. No complete measured Bronze-to-Gold traversal was established.
- Local Core edits exist, but compilation/regression-test execution was blocked by unavailable Maven. These discussions completed no performance, replay or identity-lifetime verification.

### Monitoring examples changed the question

Two existing monitoring patterns expose different signals. A file-delay check calculates per-file timestamp-to-completion delay, its own processing duration, and minimum/maximum/average timestamp age before processing. A cadence check calculates time since last completion and historical completion gaps. An inspected example had recent active batches consuming old source data: healthy processing cadence did not imply fresh input.

File modification time is not automatically landing arrival time. Kafka timestamp age is not automatically queue wait. Source lateness needs an expectation, while backlog needs independent inventory or source-position evidence. Successful-input telemetry alone cannot identify everything still waiting.

**Design direction:** persist timestamps and population evidence; calculate "age now" and policy thresholds at monitoring evaluation. Start from the expected process/source list and left-join observations so a source with no rows does not disappear. Historical gap checks need the preceding completion at the lookback boundary, not only completions inside the window.

### Stable identity did not settle statistical lineage

An immediate edge says that a consumer batch read rows stamped by a producer batch. Walking those edges supplies ancestral batch, file and Kafka-range context. It does not prove that every ancestor file contributed to a selected downstream output, especially after filtering, joins, duplication or multiple outputs.

For an **identical population retained exactly once**, mean age at consumer end is producer mean age at producer start plus elapsed time from that start to consumer end. Synthetic example: a 10-second mean plus 40 seconds elapsed gives a 50-second mean. If only a filtered subset survives, weighting the whole producer mean by the consumed-row count cannot recover that subset's mean. Mixed inputs need compatible timestamp meanings and population weights; duplicated rows need explicit multiplicity.

Likewise, scalar partition percentiles cannot be combined into a downstream p95. Ancestor timestamp bounds describe ancestor populations, not necessarily the exact consumed subset. Shared ancestor populations must be deduplicated across converging paths when summarizing ancestral context; exact downstream statistics instead require evidence at the downstream population.

Coverage must name its denominator: covered identity groups, input rows and valid timestamps are not interchangeable. Missing identity must remain distinguishable from a known reference outside available history.

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
| Continuity over time | `job_id` (+ further columns as needed) | stable until the job is recreated | platform parameters / local properties |
| Attempt | `job_run_id`, `task_run_id` | per run | platform parameters / local properties |
| Human-readable label | `job_name`, `task_name` | as stable as the platform names | platform parameters |

**Query id: read from the SparkContext local property inside the batch (implemented).**

```scala
val qid = batchDF.sparkSession.sparkContext.getLocalProperty("sql.streaming.queryId")
require(qid != null, "not inside a streaming batch")
```

- Each query runs its micro-batches on its own thread; the intended isolation of the local-property value still needs the concurrent-query acceptance test.
- **Source of truth:** the checkpoint's `metadata` file, where Spark persists the id on first start and reads it back on restart. Documented views: `StreamingQuery.id`, listener events. The implementation reads the internal local property; equality with `StreamingQuery.id` and the expected lifetime remain H1, not a completed validation. The constraint "do not derive identity from the checkpoint location" is about not depending on paths or parsing Spark's internal files by hand.
- Chosen over a session-wide listener: with several queries per session, a single "current id" can be overwritten by whichever stream started last. A mixed-up id corrupts telemetry and, wherever it drives `txnAppId`, can make Delta skip writes as "already committed".

**Use query id / batch id for telemetry idempotency, not as a universal module-write migration.** Telemetry uses `txnAppId` = query id and `txnVersion` = batch id. Propagating query identity in output data, often through an existing technical job-name column, is a separate decision from changing the module's data-write transaction identifiers. This migration explicitly does **not** require universal changes to those identifiers.

The earlier reason for considering a broader switch remains a conditional failure-mode analysis. Delta skips a write whose `txnVersion` is at or below the last one recorded for its `txnAppId`:

- checkpoint reset but hand-written date unchanged → batch ids restart at 0 and writes are skipped (silent data loss);
- date changed but checkpoint kept → a batch replayed after the relaunch is written twice.

The expected checkpoint lifetime makes query id a candidate for any separately approved data-write change, subject to H1 and replay verification. Here, the required check is that the propagated stamp really equals the producer's telemetry identity; resemblance between job names is not identity matching.

**Process continuity.** The query id resets with a new checkpoint, so it cannot identify a process over time. `job_id` provides continuity until a job is recreated (e.g. Terraform replace, workspace move); a small process dimension (`job_id` → logical process) absorbs that. Consumers select further columns (e.g. `source_id`) to distinguish streams within a job.

### Reporting contract

Reporting takes a small trait in core that both config types extend. It exposes only what reporting cannot obtain elsewhere: the Databricks job identity (job id, job name, job run id, task name, task run id) and the existing monitoring catalog/schema (existing core behaviour, out of scope here). It deliberately excludes:

- runtime values: query id and batch id, which come from the stream;
- per-input values: `source_id` and `source_type`, set by the shared read helper because one module can read several sources.

The intended identity values come from job parameters defined uniformly in Terraform. The trait has no default implementations, so a config that lacks a member should fail to compile instead of reporting a wrong value; local compilation has not yet been run. Missing platform ids are nullable, not empty-string sentinels. The current contract also includes `layer`, `task_name` and nullable `plant`.

**Accepted trade-off:** the parameter extraction is implemented identically in both config types (two copies) rather than in one shared parser. Judged acceptable because the parameters are defined once in Terraform and are not expected to change. Risk: the copies drift if one is edited without the other.

### Stamp on write, record on read

- **Stamp on write:** propagate the producing query and batch identity in output data. `_query_id` / `_batch_id` are the conceptual pair used here; an existing technical column can carry query identity only if its value matches producer telemetry.
- **Record on read:** every batch writes telemetry rows describing what it consumed: files, Kafka partition ranges, or grouped upstream units in its input. Grouping establishes unique units; a redundant `distinct` after that grouping adds no meaning.

The data carries only the immediate producer's key; the chain lives in telemetry. Recording Silver keys before a Gold aggregation preserves batch dependencies, not exact attribution of every Silver row to every Gold output.

Scope: current module inspection found no streaming stateful operators. That bounds streaming ancestry to the current micro-batch input, but does not mean every input contributed to every output; other joins/lookups also need explicit evidence. A streaming stateful operator (watermarked aggregation, stream-stream join, `*WithState`) would additionally allow outputs to derive from earlier batches, and the original raw input would not be reachable inside `foreachBatch`.

**Multiple outputs do not require an output id for batch ancestry.** A producer can stamp each table without reporting its output list; producer-to-table relationships appear from readers' rows (`source_id` + upstream key). This does not reveal which producer inputs fed each target. For terminal tables, writer completion is only a processing-completion proxy; table-to-writer mapping and actual availability need the dimension or Delta history.

### Grain: one row per input object consumed by one batch

| Source type | Input object | Rows per batch |
|---|---|---|
| File | one file in the input Dataset | one per file |
| Kafka | one topic-partition range | one per topic/partition |
| Delta | one upstream batch (unit) | one per upstream unit consumed, within the declared source/plant scope |

- **Offset-level Kafka detail comes from Bronze itself**, which already behaves like a per-message telemetry table. Lookup: Bronze rows where `_query_id` = `query_id`, `_batch_id` = `batch_id`, `partition` = `kafka_partition`.
- `input_unit_row_count` replaces the repeated `batch_row_count`: files count input Dataset rows per file (one for `binaryFile`); Kafka counts input messages per topic/partition; Delta counts input rows grouped by upstream query/batch within the consuming scope.
- These counts are additive within that declared consuming-input scope. They are not comparable business units across layers: one file, its parsed records and messages are different populations.
- Kafka modules compute counts from message rows, not offset spans (offsets can have gaps).
- If `maxFilesPerTrigger` splits an upstream batch's output, it appears in two downstream batches with partial row counts.

### Current raw contract (local POC; storage migration not verified)

```text
-- consuming stream
query_id              String     not null
batch_id              Long       not null
job_name              String     not null
job_id                String     nullable
job_run_id            String     nullable
task_name             String     task label
task_run_id           String     nullable
layer                 String     processing-layer label
plant                 String     nullable
batch_start_ts        Timestamp  not null
batch_end_ts          Timestamp  not null
batch_date            Date       derived explicitly from batch_end_ts in UTC
input_unit_row_count  Long       not null   input rows for this unit/scope
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
kafka_timestamp_count                     Long           nullable
kafka_age_at_batch_start_sum_seconds       Decimal(38,18) nullable

-- delta
upstream_query_id     String     nullable
upstream_batch_id     Long       nullable   persisted; input remains Option[String]
```

Optional approximate Kafka p50/p95 metrics are described below without asserting an unverified physical type here. The input/persisted `upstream_batch_id` type mismatch remains R1. The explicit writer cast addresses storage normalization, not proof that every input conversion and existing table schema is correct.

Date decision and proposed physical layout:

- `batch_date` is a real `DATE`, explicitly derived from `batch_end_ts` in UTC by the writer. It is **not** implemented as a generated column.
- Liquid clustering on (`batch_date`, `job_name`) plus predictive optimization. `job_name` because people filter by it and `job_id` does not survive job recreation; a rename only affects the label.
- Reasoning: the table is append-only in time order, so files are already roughly time-ordered and min/max statistics skip on time without partitions; identity is the interleaved dimension that clustering fixes. Clustering keys can change later without a rewrite.
- Volume: file rows ≈ legacy `processed_files` rows (the sizing baseline). Kafka is one row per batch × partition, which is not automatically the smaller grain: a high-partition topic with frequent triggers can produce more rows per day than a file source.
- Watch items: predictive optimization only runs on Unity Catalog managed tables; clustering is applied mostly during OPTIMIZE, so the newest data is unclustered until compaction. Appends from many writers do not conflict with OPTIMIZE.

- **Intended logical key:** (`query_id`, `batch_id`, `source_id`) + object (`file_path` | `kafka_partition` | (`upstream_query_id`, `upstream_batch_id`)), including any further grouping scope such as plant. This is a grain definition, not an observed uniqueness constraint; legacy missing identities need explicit handling.
- **Writes:** one append per batch with `txnAppId` = query id and `txnVersion` = batch id; replay deduplication is intended but still needs H3.
- **Sparse nulls are intended.** Prefer explicit type-prefixed raw columns over structs. Integrity checks are proposed, not established deployment behavior; for example:

```sql
ALTER TABLE telemetry ADD CONSTRAINT file_shape CHECK (
  source_type <> 'file' OR
  (file_path IS NOT NULL AND kafka_partition IS NULL AND upstream_batch_id IS NULL));
```

- **Width growth** is the watch item; fine for three or four source types.
- **Not stored as new raw facts:** logical process (dimension); durations and monitoring policies (derived); output relationships (readers' `source_id`). Plant is retained in the current contract. Upstream completion is resolvable context, not necessarily the instant a particular target became available.

### Schema review items

| # | Item | Status |
|---|---|---|
| R1 | Persisted `upstream_batch_id` is Long but input remains `Option[String]`; verify conversion/null behavior and migrate existing storage | Not fully resolved |
| R2 | Repeated batch count replaced by `input_unit_row_count` at declared consuming-unit grain; `binaryFile` uses one per file | Decision/local change; acceptance test pending |
| R3 | Avoid Bronze Kafka self-references in `upstream_*`; Bronze lookup already reaches messages. Null upstream identity on external input differs from a missing stamp on Delta input | Open; coverage must distinguish these cases |
| R4 | Missing Databricks ids are nullable rather than empty-string sentinels | Contract updated; round-trip pending |
| R5 | `job_name` must be the platform job name, not the dated `txnAppId` string | To confirm |
| R6 | `file_functional_ts`: keep if read from file content; if parsed from the file name it is recoverable (fine to keep as convenience) | To confirm |
| R7 | Kafka timestamp type (create vs log-append) not stored; keep it in the source dimension so waits can be interpreted | Open |
| R8 | Candidate `kafka_bytes` (sum of message sizes), the Kafka equivalent of file size | Candidate |
| R9 | `task_name` is in the current contract; verify whether it supplies the required stable task grouping rather than assuming `task_run_id` does | Verification open |

### Column rationale

**`source_id`** = the dataset read; same meaning for every type. Uses: part of object identity (a Kafka partition is meaningless without its topic); the only place table names appear in lineage; business-facing grouping key for freshness; distinguishes streams within a job; scope for missing-file detection.
Rules: Delta = full three-part Unity Catalog name; Kafka = topic (prefix cluster if several); files = folder root at the level where expectations are defined. Capture it where the stream is defined (shared read helper); tag each input with `lit(source_id)` before a union or join.

**Kafka columns:** offsets describe consumed position and support diagnostics/Bronze lookup. A discontinuity from the previous maximum plus one is a signal to investigate, not proof of data loss: legitimate offset gaps exist. Offset-distance backlog needs an independently observed broker end offset (exclusive); distance from the next consumed position is not necessarily a message count.

For each topic/partition, age is batch start minus each non-null Kafka timestamp. `kafka_timestamp_count` and `kafka_age_at_batch_start_sum_seconds` retain valid coverage and additive age evidence; their batch mean is total sum divided by total valid count when that count is nonzero, not an equal average of partition means. Null timestamps are excluded, coverage is reported against input-message count, and negative ages are retained as evidence of future timestamps or clock/semantic differences. Existing min/max timestamps give the observed population's oldest/newest ages.

Optional approximate `kafka_age_at_batch_start_p50_seconds` / `kafka_age_at_batch_start_p95_seconds` describe each partition population; scalar percentiles cannot be combined into batch or downstream percentiles. Timestamp create-time versus log-append-time semantics remain a source-level requirement.

**File columns:** modification time is timestamp evidence, not automatically landing arrival. Size is the workload property that may be unrecoverable once files are removed. Functional timestamps need their own meaning, separate from arrival and completion.

### Derived objects

**Start with views, not a new family of physical tables.** One row per (`query_id`, `batch_id`) remains a viable starting point for `batch_facts`, with nested source/plant summaries if needed. A flat source-level facts grain was also proposed, not decided. Either must preserve source/plant detail and expose the common processing envelope: identities, timing/duration, input volume, then source-specific summaries. Duration is not additive across source-level rows that repeat the same batch.

Source-specific structs can be convenient in derived facts without changing the explicit columns in raw telemetry. Keep locally observed batch facts distinct from dynamically resolved ancestry and monitoring policies.

| Object | What it is | When it becomes a table |
|---|---|---|
| `batch_telemetry` | raw table, one row per object read | already is |
| `batch_facts` | candidate batch-grain view: ids, layer, start/end, duration, input counts/bytes/objects, source/plant summaries and upstream-unit references | if measured query cost or history requirements justify materialization; grain still open |
| Lineage edges / enriched facts | views resolving immediate identities and ancestral context; coverage remains explicit | if measured lookup cost or retained history requires it |
| Process status | view: expected processes/sources left-joined to latest completion and data-age observations | if the history of evaluated states is needed ("how long was it stale") |
| Daily trends | view: `GROUP BY` process, source, day over `batch_facts` | if dashboards over months of percentiles get slow |
| Kafka continuity | view over the raw Kafka rows (already one row per batch × partition; a topic-level aggregate would hide a stuck partition) | probably never |

**Incremental completeness is not established.** One atomic telemetry commit does not guarantee that a downstream Delta micro-batch receives the whole logical batch: file admission limits can split the commit's files. A stateless aggregation over each reader micro-batch can therefore produce partial `batch_facts`. Run the commit-split acceptance experiment before choosing an aggregation/reconciliation strategy or declaring this correct.

Query-time identity resolution lets a known reference resolve on a later evaluation when retained upstream telemetry arrives. It does not make missing stamps, truncated history or partial local facts complete; unresolved coverage must remain visible.

**Process dimension:** `job_id` (+ `source_id`) → logical process, owner, expected cadence/deadline, tables written (needed for terminal tables). Business can own expectations; engineering owns collection.

### Freshness

1. **What's done:** processing duration is `end - start`; timestamp age at start and timestamp-to-completion delay describe the observed input population. Call them arrival-to-start wait only where the timestamp really measures arrival. Evaluate time since last completion separately from input age; recent completions can process old data.
2. **What's waiting:** file backlog needs an independent inventory; Kafka offset-distance backlog needs an independently observed exclusive end offset and the next consumed position. Neither is available from successful-input rows alone. Delta pending units remain unresolved: a producer that wrote zero rows to a target cannot be distinguished here from one not yet consumed. Upstream/downstream completion differences are stage context, not proof of queue wait or target availability.

Alert against the expected cadence/deadline from the dimension: the business condition, not a crashing monitoring job. Evaluate "age now" and thresholds at query time, starting from expected processes/sources left-joined to observations. "Late arrival" needs an expectation; observed timestamps alone cannot say late. Historical completion-gap queries must include the preceding observation across the lookback boundary.

Caveat: staleness alone cannot tell "pipeline stuck" from "no new input" (a stateless stream with no new data runs no batches, so a low-volume source looks stale overnight). A sharper condition from telemetry alone: downstream is stale **and** upstream has a batch that ended after the latest upstream batch downstream consumed. It can still fire falsely when upstream's newest batches wrote zero rows to the table (same limit as above).

### Lineage

Walk from a Gold batch: Gold rows (`upstream_*` = Silver unit) → Silver's rows (`upstream_*` = Bronze unit; one branch per `source_id` when Silver reads several tables) → Bronze rows (files / Kafka ranges). A Bronze lookup can identify that producer population's messages, not automatically the exact downstream subset.

- With valid stamps, identifies immediate producer-batch dependencies and their ancestral objects (batch-level lineage). It does not establish exact contributing files or messages for a selected downstream output.
- File path + row index from `_metadata` is a physical record address, not automatic provenance across transformations.
- A producer can commit an output that a consumer reads before the producer finishes its other writes/reporting. Producer end is therefore not guaranteed to precede consumer start. Do not enforce nonnegative handoff time as a correctness condition.
- Date windows are lookup optimizations, not identity semantics. A short hardcoded horizon can miss historical or late-reported producer facts. Resolve by exact keys over the available history; any bounded search must report unresolved known references, not claim absence.
- Physical pruning depends on layout. Resolving a query id to a job label may help narrow a lookup, but a label or resemblance between names cannot replace identity equality.
- Limits: telemetry/Bronze retention bound resolution; Kafka offsets are scoped to a topic's lifetime. Legacy (`source_id`, `batch_id`) + timing is a qualified heuristic, not exact lineage.
- Do not substitute full producer-population counts, means, percentiles or timestamp bounds for downstream subset statistics. Distinguish ancestry coverage from row/timestamp coverage, and deduplicate shared ancestors when paths converge.

### Proposed guardrails (implementation/acceptance not established)

- **Id mix-up check:** unexpected overlapping windows or keys shared by different streams should trigger investigation. Attempt/replay timestamps also need examination before diagnosing an id mix-up.
- **Kafka continuity check:** investigate per-partition discontinuities without treating each offset gap as data loss.
- **CHECK constraints** per source type.
- **Stateless check:** propose checking that progress reports no state operators (`stateOperators` empty); module inspection is not runtime enforcement.
- **Identity captured once:** read query id on the batch thread and pass it as a value; reused pool threads can retain another query's local properties. Verify stamps against producer telemetry.
- **One idempotent write per (table, batch):** a second write to the same table with the same `txnAppId`/`txnVersion` is skipped silently. If ever needed: union first, or use a fixed write-site suffix on the app id.
- **Telemetry written last:** intended to describe completed processing, but write/checkpoint failure windows still need replay tests. It does not make multiple data writes and telemetry one transaction.
- **Writer options only:** `txnAppId`/`txnVersion` are set per write, never as session confs (which would apply to every Delta write in the session).

### Example (illustrative values)

| Batch | Window | Telemetry rows (consumed) | Output stamp |
|---|---|---|---|
| Bronze `qB/4812` | 10:00:05–10:03:40 | file `a_1010` 412 MB; file `b_1010` 3 MB | `bronze.readings`: `qB/4812` |
| Bronze `qK/20931` | 10:01:00–10:01:07 | kafka `topic_x` partition 3, offsets 88100–88949 | `bronze.events`: `qK/20931` |
| Silver `qS/377` | 10:04:10–10:09:30 | `bronze.readings` ← `qB/4812`; `bronze.events` ← `qK/20931` | `silver.readings`: `qS/377` |
| Gold `qG/52` | 10:10:00–10:12:20 | `silver.readings` ← `qS/377` | `gold.kpis`: `qG/52` |

In this synthetic example, Gold 52 has ancestral context `qS/377` → `qB/4812` (files `a_1010`, `b_1010`) and `qK/20931` (partition 3, offsets 88100–88949). The keys alone do not prove that both files or every message contributed to a selected Gold result.

---

## Migration plan

- **Modules build source evidence; Core owns persistence.** Legacy `processed_files` reporting is optional through an existing parameter. New reporting requires modules to implement a builder receiving input Dataset, config and processing's batch-start `Instant`; Core adds consuming identity/timing and normalizes storage types before writing.
- **Dual reporting** is supported, so each job migrates to the new core version once; legacy reporting can then be switched off or its code removed.
- **Validate the input/storage contracts:** check source-specific required fields; legacy missing identity must be explicit coverage, not a silent success. Verify Decimal/null round-trip, input `Option[String]` to persisted Long conversion, and existing-table migration.
- **Migrate producers before consumers** (Bronze → Silver → Gold): a consumer can only record `upstream_query_id` once its upstream stamps `_query_id`.
- **Separate stamp propagation from data-write idempotency.** Use query/batch identity for telemetry and verify the propagated values match it. Do not universally switch modules' existing data-write `txnAppId` values in this migration. Any later switch needs its own replay/clean-stop plan.
- **Reconcile before switching legacy off:** compare legacy `processed_files` with new file rows per batch (paths, counts, sizes) for a period.

---

## Alternatives considered and set aside

| Alternative | Why considered | Why set aside | Status |
|---|---|---|---|
| Two tables: `slice` (wrapper) + `progress` (StreamingQueryListener) | Delta offsets appeared reachable only via progress events | Data-borne stamp makes offsets unnecessary | Fallback; draft `ProgressListener.scala` exists |
| Query id from a session-wide listener | Simple propagation into the batch | Jobs run multiple queries; one shared "current id" can be overwritten, corrupting identity and telemetry idempotency | Replaced by local property; usable only as a cross-check keyed by query name |
| Commit `userMetadata` stamp | Link commits to producing batch | Superseded by stamping the data | Possible tie-breaker for multi-writer tables |
| Modification-time window matching | Link data files to producer batch windows | Heuristic; needs table → writer mapping | Fallback for jobs without stamp columns |
| Delta one row per consumed data file | Physical-file context without offsets | Heavier; upstream batch identity answers immediate dependency questions, but not exact downstream file attribution | Fallback if physical-file evidence is needed |
| Producer record from the target table's Delta log (`txn` actions carry app id + batch id) | Unit-level "what's waiting" for Delta without `output_id` | Initial focus is completion context; target-log interpretation still needs verification (app-id mapping, empty appends, log retention) | Fallback if unit-level pending is needed; module app ids are not universally query ids |
| Value object built per module (`Config => ReportingContext`) | Decouples config classes from reporting; easier to test | Two config types, both owned by core; the trait gives the same compile-time guarantee with less code | Fallback if a config outside core's control must report |
| Kafka one row per message | Per-message detail | Duplicates Bronze at data volume | Rejected |
| Partitioning by `job_name` × `process_date` (+ `layer`) | Partition pruning on identity and time, the main query pattern | Tiny partitions (a table this size needs no partitioning) and a small file per batch per job partition; `layer` has 3 skewed values. Lineage would need cross-day partition reads anyway | Rejected; date-only partitioning remains possible for cheap retention |
| Multiple physical gold tables (`batch_facts`, `lineage_edges`, `process_status`, `daily_rollup`, plus `kafka_partition_facts`) | One table per kind of question | No measured need yet; query-time views can resolve later-arriving ancestry without persisting edges. This does not solve incomplete raw coverage or incremental batch splitting | Set aside; materialize a view only when evidence demands it |
| Reading checkpoint `metadata` for query id | Simple source of query id | Violates constraint | Rejected |
| Generic columns / struct per type / table per type in raw telemetry | Avoid sparse nulls | Mixed semantics / clumsier SQL / UNION everywhere | Rejected for raw; source-specific structs remain possible in derived facts |

---

## Hypotheses to verify

| # | Hypothesis | POC step |
|---|---|---|
| H1 | The local property returns the persistent query id: equal to the documented `StreamingQuery.id`, stable across restarts, new after a checkpoint reset, distinct per concurrent query | 1 |
| H2 | Identity propagation leaves existing module data-write behavior unchanged through replay, checkpoint reset and a kill between writes; any broader transaction-id change requires separate approval and tests | 2 |
| H3 | Telemetry append with the same `txnAppId`/`txnVersion` cannot duplicate on replay | 2 |
| H4 | Stamping and per-batch aggregations add acceptable overhead | 3 |
| H5 | Recorded upstream units are complete (every input row stamped; split upstream batches appear in both readers) | 3 |
| H6 | Multi-source streams and multi-output jobs are handled by `source_id` tagging and stamping | 4 |
| H7 | The lineage walk returns correctly bounded batch ancestry with explicit coverage, without claiming exact contributing files or downstream subset statistics | 6 |
| H8 | The legacy fallback (source + batch id + timing) is a useful qualified heuristic; ambiguity can be detected rather than presented as exact lineage | 7 |
| H9 | New file rows match legacy `processed_files` during dual reporting | 7 |
| H10 | Liquid clustering + predictive optimization keep file counts and lookup cost acceptable at production commit rates | 3, 6 |
| H11 | Counts/sums yield the expected Kafka mean and coverage with null/future timestamps; optional percentile cost is acceptable | 3 |
| H12 | Derived facts stay complete when file admission limits split one telemetry commit across reader micro-batches | 6 |
| H13 | Decimal/null values and upstream batch ids round-trip through normalization and the migrated table schema | 3, 7 |

---

## POC plan

**Step 0: Setup and acceptance queries.** Write acceptance queries for file timestamp-to-completion delay and its duration/age decomposition, completion cadence/gaps (including the lookback predecessor), missing expected sources, inventory-based missing files, Kafka partition diagnostics, and batch ancestry to file/Kafka context. Use synthetic rows carrying file name/row number as ground truth, including a job running two queries and a recently active process reading old data.

**Step 1: Identity (H1).** Log `StreamingQuery.id` in `main` after `start()` and the local-property id in the first batch (must be equal; keep this log to re-check after runtime upgrades). Log the query id and `batchId` in every batch of the two-query job; restart; reset one checkpoint. Pass: ids distinct per query, stable across restart, new after reset.

**Step 2: Idempotency (H2, H3).** Use query id / batch id for telemetry only; retain the modules' existing data-write transaction identifiers. Kill after data write but before telemetry/checkpoint completion and between two data writes; restart and check both data behavior and telemetry deduplication. Reset a checkpoint and record behavior explicitly; do not turn this experiment into an unapproved data-write migration.

**Step 3: Stamp, populations and storage (H4, H5, H11, H13).** Compare module builders plus the Core writer with ground truth. Test one input row per binary file versus parsed rows per file; Kafka count/sum and optional approximate percentiles with uneven partitions, null and future timestamps; grouped Delta input rows; Decimal/null and upstream-id round-trip. Measure overhead with optional metrics enabled/disabled, not just the base writer.

**Step 4: Multi-source and multi-output (H6).** A Silver job joining two Bronze tables; a job writing two output tables. Verify actual stamp equality with producer telemetry, source/plant preservation and the limits of per-target attribution.

**Step 5: Failure and replay.** Kill mid-batch at different points; confirm one row per object per unit and that the guardrail checks behave.

**Step 6: Facts and ancestry acceptance (H7, H10, H12).** Prototype the batch-grain view and compare a flat source-level alternative without declaring the grain settled. Use a hand-filled expected-process/source dimension and proposed checks. Test an unchanged population versus a filtered subset, duplicated/joined/mixed inputs, one producer split across consumers, and converging paths sharing an ancestor. Compare ancestry and downstream statistics with ground truth, explicitly demonstrating what cannot be attributed. Split a single telemetry commit across Delta reader micro-batches; choose reconciliation/aggregation only after observing completeness.

**Step 7: Migration checks (H8, H9, H13).** Separate missing identity from a known producer outside the export/retention window. Run legacy input and dual reporting; reconcile file paths/counts/sizes against `processed_files`. Migrate the existing storage schema and confirm the Decimal/null fix after deployment. Resume compilation/regression tests once Maven is available.

**Step 8: Evaluate and write up.** Mark each hypothesis confirmed, refuted or unclear with evidence; estimate production volume; update "What changed" and "Conclusion"; consider a `decisions/` entry.

---

## Decisions

**Made:** one shared raw table/writer; module-specific builders using processing's batch-start clock; consuming-unit input counts; grains (file / Kafka topic-partition / upstream unit within declared scope); offset-level Kafka detail from Bronze; query id from the SparkContext local property; query/batch id for telemetry idempotency only; no universal module data-write transaction-id change; explicit type-prefixed raw columns; nullable platform ids and retained source/plant detail; explicit UTC `DATE` from batch end, not a generated column. Legacy reporting remains optional with dual reporting during migration. These are design/local-code decisions, not completed runtime acceptance.

**Refined:** persisted `upstream_batch_id` is Long, but the input type is still `Option[String]` (R1). Delta completion context is observable; pending units/queue wait are not established. Batch ancestry does not imply exact contributing objects or downstream statistical populations. Lineage edges, enriched facts, status and trends start as views.

**Leaning:** job identity (`job_id`, `job_run_id`, `task_run_id`, `job_name`, `task_name`) from Databricks dynamic value references passed as JAR task parameters (`{{job.id}}`, `{{job.run_id}}`, `{{task.run_id}}`, `{{job.name}}`, `{{task.name}}`), parsed once in `main`; local properties as fallback for the ids; null when absent. Verification would address R5 and R9. Proposed guards: a deploy-time parameter check, a runtime warning on Databricks without them, and monitoring for null `job_id`. Not yet tested: overridden JAR parameters, repair runs / task retries, and run id vs task run id on a multi-task job.

**Pending:** unresolved schema review items and acceptance of local changes; physical layout/retention, table name/ownership and rollout order; `batch_facts` grain and commit-split completeness; storage migration/deployed verification; identity lifetime, replay/concurrent-query behavior and metric cost. A universal data-write `txnAppId` switch is not pending rollout scope.

## Open questions

- Which freshness expectations exist, who owns them, and which conditions alert through which channel?
- Do expectations describe processing cadence or data-arrival cadence? (Staleness alone cannot tell a stuck pipeline from no new input.)
- What should a support engineer answer from the morning email or a dashboard?
- What does the functional/filename date mean, and how are file overwrites handled?
- How will the file inventory be produced for missing-file detection?
- Which Kafka topics use create time vs log-append time?
- Is Bronze retention long enough for the required lineage horizon?
- What does the legacy 65 GB consist of (live data, unvacuumed old versions, small-file overhead)?
- How does the archiving job read the telemetry table (incremental stream or date ranges)? It is a heavy reader, so it affects layout.
- Where do Databricks DQM features fit, if at all?
- If per-partition Kafka lag is needed, where does the partition end offset come from: Spark progress `latestOffset` (partly reverses the listener decision) or broker / Event Hubs metrics outside telemetry?
- What evidence identifies the exact downstream population when ancestry is insufficient, and how will each coverage denominator be exposed?
- Which derived-facts grain preserves source/plant detail without repeated batch durations being summed, and how will split telemetry commits be reconciled?

---

## Conclusion (provisional, 2026-10-19)

The POC supports a coherent direction for shared raw telemetry, but not a verified end-to-end monitoring or lineage solution. Builder/count and Decimal observations exposed concrete contract problems; the local fixes still need compilation, storage migration and deployed acceptance. The inspected exports establish limited local evidence, not a complete Bronze-to-Gold traversal.

The stronger conclusion is about reasoning: a schema can unify storage while leaving its metrics semantically incompatible. I need to establish the row grain, counted population, timestamp meaning and evidence coverage before composing ages, asserting lineage or diagnosing backlog. Stable batch keys make dependencies resolvable; they do not supply the missing population evidence.

## What changed in my understanding?

- Identity is several things with different lifetimes (unit, continuity, attempt, label). The hand-written name + date mixed two of them, and as `txnAppId` it is also a correctness risk.
- Spark's query-id lifetime is the intended basis for identity, unlike per-run `runId`; this internal local-property implementation still needs restart/reset/concurrent-query verification. Propagating it does not require changing every module's write idempotency.
- Start/end are batch facts, while input counts belong to a consuming-unit population. Repeating a batch count or summing repeated durations changes the answer. Derived-facts grain remains a design question.
- Stamping and recording immediate keys gives batch dependencies without an output list, not exact file/message attribution for every downstream output. Full producer means/bounds and scalar percentiles do not automatically describe a consumed subset.
- The two-table design was a response to a missing piece of data, not a demonstrated modeling need; data-borne keys support one shared raw table. This does not settle derived-facts grain or materialization.
- Working backwards from how the table is used ("what can't be recovered later?") removed most of the machinery.
- Recent processing can consume old data. Completion cadence, source timestamp age, source lateness and backlog need separate evidence and expectations.
- One atomic writer commit is not necessarily one complete reader micro-batch. Producer completion also need not precede consumer start; implementation convenience is not a temporal correctness guarantee.
- Storage annotations and encoders do not normalize incoming Spark expression types. The persisted schema and actual DataFrame types must agree.
- Backward ancestry can work without enumerating outputs, but exact per-target attribution and forward questions ("what's pending?") need additional evidence.

## Connections to existing cases

Potential connections, not evidence that the problem is already solved:

- `cases/productionizing-a-benchmark-driven-streaming-architecture.md`: control-tower tables as correctness/readiness coordination.
- `cases/databricks-cost-intelligence-loop.md`: `job_id`/`job_run_id` in telemetry should enable cost per process or batch.
- `cases/databricks-comment-metadata-ownership.md`: business owns freshness expectations; engineering owns the mechanism.
- `cases/streaming-file-parser-rework.md`: file size vs duration per batch is the signal that would have surfaced large-file pressure earlier.
- `cases/repartitioning-large-delta-table.md`: retention and lifecycle operations bound long-lived assumptions; job recreation during a workspace move would split `job_id` continuity.

## Reusable lesson (provisional)

A common row schema and stable identity are insufficient without explicit grain, statistical population, timestamp meaning and evidence coverage. Keep local stage statistics, ancestral context, processing cadence and backlog separate; compose them only where the required evidence survives. Carry identity across hand-offs, but never promote a dependency edge into stronger attribution than it proves.
