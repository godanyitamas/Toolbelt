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
  - event-hubs
  - databricks
  - freshness
  - file-ingestion
---

# Designing unified processing telemetry across files, message streams, and Delta

> **Handoff status (2026-10-22, updated after candidate tests):** The user chose to keep the tested histogram candidates for now. Core owns a pure `MessageHistograms` object (`Version = 1`, `AgeSecondsBounds`, `ValueBytesBounds`), not histogram calculations or validation. Modules supply counts/sums/min/max and three histograms: age at start, age at completion, and binary value bytes. Message raw grain is topic/partition within source scope (timestamp type was later dropped from grain and schema; see the timestamp-type revision); means remain in facts. Local Core definitions/schema/writer compile and seven focused tests passed; these edits are uncommitted and not deployed verification. A separately adapted Kafka ingestion attachment passed seven focused local Spark tests using its extracted telemetry method, not a full external-module build. The POC remains active.
>
> **Contract and evidence limits:** Version 1 has 29 age bounds / 30 buckets for each age array and 83 size bounds / 84 buckets. One-day exact-reference comparisons support retaining the candidates, not a numerical error guarantee or cross-day stability. Completion uses a separately built histogram with the same end instant persisted as `batch_end_ts`; merging start histograms and shifting by average duration cannot recover daily completion percentiles. No bounds lookup table exists. The raw table is in test and intended for jar/table redeployment; this rename needs no migration of that test table, but deployed round-trip and broader migration checks remain open.
>
> **Previous handoff (2026-10-20):** The chosen model is `batch_telemetry` for raw drill-down, flat `batch_facts` at exactly (`query_id`, `batch_id`) for generic monitoring, and separate `batch_dependencies` for observed batch edges. Means, age bounds and coverage belong in the facts build alongside mergeable sums/counts. Base schemas/results have been inspected; the latest extended SQL and optional platform joins have not been reported executed. Materialization/refresh correctness, replay, identity lifetime and performance remain unverified. Identity edges establish batch ancestry, not exact contributing-file or downstream statistical populations.

## Problem

The existing `processed_files` reporting concept is useful for file-based processing, but it does not cover all relevant source types. The goal is to extend the telemetry foundation to file, message-stream (Kafka, and Event Hubs via the Kafka API), and Delta-based pipelines so that operational monitoring and lineage investigations can use consistent concepts without forcing incompatible source semantics into an artificial universal model.

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

One batch is the grain for generic duration, workload, source-age, cadence and coverage trends; one observed batch dependency is the grain for relationships. Raw telemetry retains plant/file/partition detail. The useful common envelope is identity and processing timing, followed by source-specific input volumes and statistics, not a universal business-row count.

A shared row shape and stable keys are not enough: each metric needs a declared population, timestamp meaning and coverage. Convenient means and bounds should be built once with the facts, while sums/counts remain available for correct weighted rollups. Local stage statistics, ancestral context, processing cadence and backlog still answer different questions.

Histogram bucket counts merge exactly only for compatible bounds/version, metric and population semantics. Percentile interpolation inside the merged buckets is still approximate. Age at start and age at completion need separate arrays for window rollups; timestamp meanings stay separate even within one partition. The tested distributions inform a stable versioned contract, not a promise that future data follows the same shape. Local tests narrow implementation uncertainty without establishing deployed behavior, replay safety or complete lineage.

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
- The source builder uses the input Dataset, reporting config and the same batch-start `Instant` as processing. The adapted Kafka attachment also receives a post-write end instant for completion age; Core persists that same instant. Age calculations must not silently use a later reporting-time clock.
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
- One raw telemetry table, one shared writer; derived facts and dependencies do not automatically require more physical tables.
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
- At this point, local Core compilation/regression tests were blocked by unavailable Maven (later local compilation evidence is recorded under 2026-10-22). These discussions completed no performance, replay or identity-lifetime verification.

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

## Investigation update: 2026-10-20

### What is decided versus observed

**User decisions:** `batch_facts` is exactly one flat row per (`query_id`, `batch_id`), not a primary source/plant-grain fact. `batch_dependencies` holds observed batch relationships separately. The proposed `input_scopes ARRAY<STRUCT<source_type,source_id,plant>>` is rejected: membership does not attribute batch-wide metrics to each plant, and exploding it duplicates those metrics. Raw telemetry remains the drill-down; source summaries are future options only if an actual question requires them.

**Observed evidence:** schema layouts for raw telemetry, facts and dependencies were provided, and earlier base-facts schemas/results were returned. This supports the shape of the base aggregation, not proof of whole-batch formation, refresh correctness or end-to-end lineage. The raw layout includes partition Kafka age metrics and clustering on `job_name`, `batch_date`; no evidence resolves the Scala input `Option[String]` versus persisted BIGINT upstream batch id.

**Latest refinement, not an executed result:** a staged SQL proposal (`prepared` → `aggregated` → `batch_values` → `derived` → `final`) adds ready-to-use means, age bounds, coverage and completion-age columns during the facts build. This supersedes the lean-base-only proposal that left all means to presentation views. An envelope struct is only an intermediate convenience; final facts stay flat. The latest extended SQL has not been reported run.

### What the grain decision protects

A scalar `source_type` in facts requires one source **type** per batch, not necessarily one source id. Selecting an envelope with `any_value` does not validate this or consistency of execution metadata/timing; those invariants need explicit checks. Batch-level duration and workload must not be repeated and then summed through plant membership, dependency fanout or run-context joins.

Ready-to-use metrics and sufficient statistics are complementary. Monitoring should not repeatedly reconstruct each batch's means, but weighted trends must still use the matching sums and counts. The same principle applies to optional platform enrichment: prepare it at a safe join grain rather than multiply batch rows.

---

## Investigation update: 2026-10-22

### Kafka generalized to message sources

**User decisions:** the `KAFKA` source type and `kafka_*` columns become `MESSAGE` and `message_*`, because some jobs consume Event Hubs through its Kafka-compatible endpoint and conform to the same layout (topic, partition, offset, timestamp). A separate `message_system` column (`KAFKA` | `EVENT_HUBS`) distinguishes the broker; `source_id` keeps meaning only "the dataset read". Retain numeric Spark `timestampType`: `0` = CreateTime, `1` = LogAppendTime, `-1` = NoTimestampType. The later refinement made timestamp meaning part of the raw grouping; the revision below then dropped the type from grain and schema.

**What the research found (documentation via search summaries; the Microsoft Learn pages could not be fetched directly, not verified on our event hubs):**

| | Kafka | Event Hubs via Kafka endpoint | Event Hubs via native connector |
|---|---|---|---|
| Position | offset (long, per partition) | Kafka-protocol offset (long, per partition) | string `offset` plus separate long `sequenceNumber` |
| Timestamp | per topic: `CreateTime` (default) or `LogAppendTime` | documented default AppendTime, configurable to `CreateTime` per event hub | `enqueuedTime` |

- Semantics follow the **read API**, not the broker. Kafka-endpoint offsets are not the offsets or sequence numbers shown in the Event Hubs portal; native numeric offsets are not guaranteed by the service.
- Those documentation summaries suggest opposite defaults; actual Event Hubs behavior remains unverified. Combining timestamp meanings could average producer-clock age with broker-arrival age.

**Contract consequence:** `MESSAGE` currently means *Kafka-consumer-API semantics*. A future native Event Hubs reader must not reuse `message_min/max_offset`; it would need an explicit read-API discriminator and its own position definition (e.g. sequence number). "Generic" must not become "same column, different meaning".

**Observed sample:** all measured sources returned `0`, had complete valid timestamps, and showed no negative ages in the inspected day. This is not a broker guarantee. CreateTime is producer-supplied, not proof of actual publication time or business-event time. Age distributions with different timestamp meanings must not be combined; a `MIXED` label after combining them loses evidence and was rejected. The revision below handles this by assumption and onboarding check instead of grain separation.

**Open (superseded by the revision below):** whether `timestampType` returned through the Event Hubs Kafka endpoint reflects the configured append/create behavior. A controlled test needs the configuration and known messages; one observed label does not establish equivalence for all event hubs or configurations.

**Revision (2026-10-22, user decision): `timestampType` dropped from grain and schema.**

- **Why:** its only real benefit is interpreting what an age means (CreateTime: since producer stamping, including producer buffering/clock; LogAppendTime: since broker acceptance). That is a property of the source, not of each message, and every measured source returned `0`. As a grain key it cost extra rows per partition, a more complex continuity check and an unsolved flat-facts design for multiple age populations, all for an unobserved case. A min/max validation column was considered and not chosen.
- **New rule:** message raw grain returns to one row per topic/partition per batch. No `message_timestamp_type` column. The type is still read from the input row only to filter: a timestamp is **valid** only if `timestampType <> -1` and the timestamp is non-null. The same validity condition applies to `message_ts_count`, the start-age sum, both age histograms and message min/max timestamps. Invalid timestamps count only in `input_unit_row_count`.
- **Assumption to verify:** Spark's Kafka source probably converts a `-1` record timestamp to `1969-12-31 23:59:59.999` rather than null; without the filter such messages would appear about 56 years old in the open age tail. Confirm with one synthetic record in the local harness.
- **Accepted losses:** age meaning is now an assumption ("all current sources CreateTime", one observed day), not stored evidence; a later switch of a topic/event hub between create and log-append time would change age meaning silently; H16 (Event Hubs equivalence) has no stored column to verify against.
- **Compensation (proposed):** one onboarding check per new source, e.g. `SELECT DISTINCT timestampType` on a sample read, recorded in the source dimension or the case.
- **Code impact (local Core and attachment, not yet changed):** remove the type from `groupBy`, logical key, Core schema/writer and attachment projection; replace the "separation of mixed timestamp types" test with "a type `-1` message is excluded from age statistics and counted only in input rows". The local test evidence recorded below was produced with the earlier type-grouped implementation.

### Message value size and the population rule

The user had added min/max/median value length (byte length of the Kafka value). Direction now: keep **mergeable** statistics in raw and compute means in facts.

- Null handling follows the timestamp pattern: `message_value_count = count(value)` (non-null only); bytes sum/min/max over non-null values. Null count is derived as `input_unit_row_count - message_value_count`; an empty byte array is a non-null value of length 0. Implemented in the adapted attachment and locally tested, not deployed confirmation.
- "Value bytes" excludes key, headers and protocol overhead; it is not broker throughput. Raw values in Bronze make this recoverable while Bronze is retained, so it is a convenience, not a must-capture.
- Sums, counts, mins and maxes roll up correctly across partitions, batches and days (`SUM(sum)/NULLIF(SUM(count),0)`, `MIN(min)`, `MAX(max)`); medians and percentiles do not.

### Percentiles depend on whether raw holds the population grain

| Median/percentile of… | Where it is computable |
|---|---|
| batches (e.g. daily p95 processing duration) | exactly from `batch_facts`: one row per batch is the population |
| files | exactly from `batch_telemetry`: raw is already one row per file, so daily file percentiles are a plain `percentile` over raw rows (per valid-value population, with coverage reported) |
| messages within one partition range | scalar percentiles could be computed at write time, but would not be combinable; the chosen raw evidence is histograms |
| messages within a batch or a day | compatible histogram counts merge exactly; percentile estimates require approximate within-bucket interpolation, with open-tail limits |

Runtime constraint: the jobs run on Databricks Runtime 16 LTS. Mergeable KLL sketch SQL functions (`kll_sketch_*`) are documented from 18.0; earlier sketches (HLL) are for distinct counts only.

**Percentile options considered.** Medians/percentiles are needed for monitoring. Three options were compared; option B was chosen as the direction to test (see below).

| Option | Gives | Costs / risks |
|---|---|---|
| A. Batch-level median computed in the builder, repeated on each partition row | a statistic over the whole batch's messages (approximate if `percentile_approx` is used); no contract to design | not mergeable: median of batch medians is not the median of a day's messages; same grain-mismatch pattern as the repeated file count, so needs a `batch_` name prefix, a constant-per-batch invariant check in facts, and an explicit exception to the raw-grain rule; extra aggregation unless done with `GROUPING SETS ((partition), ())` |
| B. Fixed-bucket histograms per partition/type row (`ARRAY<BIGINT>` counts) | exact merge of compatible counts to batch/day/rollup; approximate percentiles; exact share-above-threshold at bucket boundaries; works on 16 LTS | versioned bounds/semantics in Core; never mix versions or age meanings; finite-bucket width bounds location uncertainty, open tails do not; arrays bend the explicit-columns preference |
| C. Exact percentiles on demand from Bronze messages over the window | exact; no contract or redundancy; analogous to file percentiles from raw | bounded by Bronze retention; scan cost of Bronze (`timestamp`, `length(value)`) per evaluation, unmeasured |

Deciding evidence, not more argument: (1) write down one or two actual monitoring queries; if all are per batch, A suffices; if any spans a window, B or C. (2) On test data, compute A and B and compare with exact percentiles from Bronze; measure the cost of C for one day on the largest topic.

Historical option B sketch (illustrative module calculation, not the current implementation or a Core helper): one conditional count per bucket inside the partition aggregation, half-open buckets with a first `(-inf, b0)` bucket keeping negative ages visible; proposed facts merge element-wise with `aggregate(collect_list(hist), array_repeat(0L, n), (acc, h) -> zip_with(acc, h, (a, b) -> a + b))`; invariant: histogram total equals the matching valid count. The retained contract and later local evidence follow below.

```scala
def histogram[T](value: Column, bounds: Seq[T]): Column = {
  val edges = bounds.map(lit(_))
  val buckets =
    (value < edges.head) +:
      edges.sliding(2).map { case Seq(lo, hi) => value >= lo && value < hi }.toSeq :+
      (value >= edges.last)
  array(buckets.map(cond => sum(when(cond, 1L).otherwise(0L))): _*)
}
```

### Tested histogram candidates retained (option B; POC still active)

**User decision:** keep the tested candidates for now and update Core. The earlier helper design requiring module-defined bounds and Core aggregation/validation was explicitly rejected. Core owns only the pure constants object, consuming identity/time, storage normalization and persistence of supplied evidence. Modules calculate histograms and sufficient statistics.

**Version 1 contract:**

- `MessageHistograms.Version = 1` identifies **both** bound sets and their semantics. Published bounds cannot change in place; arrays from different versions cannot merge. Old definitions must remain resolvable by future consumers.
- `AgeSecondsBounds` (29 bounds, 30 buckets for **each** age array): `0, 0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 450, 600, 750, 900, 1200, 1500, 1800, 3600, 7200, 21600, 86400`.
- `ValueBytesBounds` is the sorted distinct union of `CEIL(POWER(2.0, i / 4.0))` for integer `i = 0..80` and `[0, 136, 144, 224, 232, 5632, 5760]`: 83 bounds, 84 buckets. This is a **ceiling-rounded quarter-octave** scaffold, not simple powers of two; rounding preserves integer band membership.
- Buckets are `(-infinity, b0)`, `[b0, b1)`, ..., `[last, +infinity)`. Equality belongs to the upper/starting bucket. Negative ages remain in the first bucket; null timestamps/values are excluded from their respective statistics; empty binary values count as zero bytes.
- Three arrays plus `message_hist_version`: `message_age_at_start_hist`, `message_age_at_completion_hist`, `message_value_bytes_hist`. Both age arrays use the same bounds and valid-timestamp population, each totaling `message_ts_count`; size totals `message_value_count`. Non-message rows have null message fields.

**No bounds table yet.** The intended Core-based facts interpretation can use the constants; test notebooks can use explicitly version-labeled definitions. The first dashboard/non-Core consumer needing bucket interpretation, or the first bounds change, remains the trigger to consider a Core-managed lookup table (`version`, `metric`, `bounds`) and shared SQL entry point. Neither exists as a verified implementation here.

**Proposed downstream integrity checks, not Core calculations:** each total matches its valid count, array length is bounds + 1 for that version, and merged groups have one compatible version and timestamp meaning. Higher-order functions in Delta CHECK constraints remain unverified.

**Exact merges versus approximate percentiles:** summing compatible bucket counts loses no further bucket evidence. Cumulative counts with linear interpolation inside a finite bucket still approximate the original quantile. A bucket's uncertainty interval is not a confidence interval. Do not invent a finite percentile in an open tail; expose the bound/tail and retain negative ages. Shares `>=120`, `>=300`, `>=600` seconds are exact at those boundaries, independently of interpolation.

**Start versus completion:** for the same messages in one batch, exact completion-age quantiles equal exact start-age quantiles plus that batch's duration. Bucketed estimates need not preserve this equality. Across batches, shifting a merged start histogram by average duration cannot recover completion quantiles. Build the completion histogram separately from the **same end instant** persisted as `batch_end_ts`; do not subtract start/completion percentile values to infer duration percentiles.

### Measurements that informed the retained candidates

**User-supplied production observations, generalized populations:** value sizes were heterogeneous: traceability events concentrated at exact sizes in tens/hundreds of bytes; static events were narrow around 700 bytes; one smaller event population clustered near 1 KB and 5.6-8 KB; a larger population ranged from hundreds of bytes to about 1 MB (median about 2.9 KB, mean about 82 KB); configuration values had a broad right tail. Initial samples had no null/empty values, not a universal guarantee.

The initial overview used differing `TABLESAMPLE` fractions and pooled history, with approximate percentiles for screening. Later bounded-day tests used exact percentile references. Their counts are not directly comparable to those initial samples. No second-day, weekly or performance evidence has been established; static events and some small topics were not all tested with the final candidate.

**Counterexample to uniform interpolation:** the smaller event population's tested day had 462,773 values. Its unrefined median bucket was about 4871-5793 bytes, but observed values **inside that bucket** were 5652-5790 bytes. Uniform interpolation estimated about 5417 bytes, below every observed value in the bucket. Refinements at 5632/5760 substantially improved that day's median, not proof of uniformity or a global guarantee. The earlier criterion "no bucket should contain most messages" was corrected: real point masses are legitimate; evaluate the questions and measured errors instead.

**Size candidate versus exact same-day references:**

| Generalized population | p50 / p95 / p99 relative error | p50 / p95 / p99 absolute error |
|---|---|---|
| Larger event population | 2.47% / 0.62% / 1.75% | 73 / 3785 / 16032 bytes |
| Smaller event population | 0.03% / 0.34% / 2.39% | 2 / 27 / 210 bytes |
| Traceability events | 2.73% / 0.46% / 1.26% | 3.9 / 1 / 2.9 bytes |
| Configuration values | 3.02% / 1.87% / 7.41% | 476 / 3370 / 19539 bytes |

Configuration had only 1487 values that day: exact p99 about 263735 bytes versus estimated 283274, in `[262144, 311744)`. This sparse-tail result does not automatically justify tuning to one day. Count totals matched and none of the tested quantiles landed in an open tail. No numerical error tolerance has been accepted.

**Age observations, one day of producer CreateTime:**

| Generalized population | Median completion age | Completion p99 | Share >=300s / >=600s |
|---|---|---|---|
| Larger event population | about 6.2s | about 29s | 0% / not reported |
| Smaller event population | about 5.5s | about 611-612s | 1.58% / 1.02% |
| Result events | about 1s | about 2.9s | 0% / not reported |
| Traceability events | about 188.7s | about 1216.8s | 35.88% / 11.54% |

Much of the delayed age was already present at **batch start**. This does not identify Kafka queue wait or slow parsing as the cause. Producer clock/buffering, transport, scheduling and backlog remain competing explanations.

**Age candidate versus exact p50/p95/p99, start and completion tested separately:**

| Generalized population | Maximum absolute error across tested quantiles: start / completion |
|---|---|
| Larger event population | 2.15s / 0.64s |
| Smaller event population | 1.48s / 7.75s |
| Result events | 0.27s / 0.04s |
| Traceability events | 34.24s / 36.53s |

Some short-age relative errors were about 10-11% but small in absolute seconds; traceability p99 error of about 3% meant about 36 seconds. Both scales matter. All count matches were true and no tested percentile was in an open tail. These are measured one-day errors, not guaranteed bounds.

**Time/population caveat:** initial tests used start at/after yesterday's boundary **and** end before today's, excluding crossing-midnight batches. Later age tests used completion at/after yesterday and before today in the session timezone; UTC must be explicit for UTC reporting. This is not an ingestion day defined by message timestamps. Historical ages use supplied batch timestamps, not `current_timestamp`. Existing Bronze end stamps may be expression/pre-write timestamps rather than committed completion; the adapted builder below explicitly uses a post-write end, without retroactively validating old stamps.

### Operational expectation, not yet an alert policy

**User clarification:** Kafka **MESSAGE** jobs should parse and make Bronze data available roughly 2-5 minutes after publication. This is not file ingestion, and the same requirement was not established for Event Hubs. The 120/300-second bounds support candidate policy boundaries; warning/critical labels, allowable percent-late shares and exact SLA semantics remain undecided. Observed slowdown should remain visible, not redefine acceptable delay upwards.

Completion age addresses the requirement more directly than start age, subject to producer timestamp meaning and the difference between a captured post-write instant and actual target availability. Successful telemetry still cannot reveal unconsumed backlog or absent-source cadence.

### Local implementation evidence and its boundary

**Core, local and uncommitted:** definitions/schema/writer compile; seven focused tests passed for the published age vector, exact size formula, version 1, increasing finite bounds and nullable schema compatibility. Additions include completion histogram and numeric `message_timestamp_type` (the latter since dropped by the timestamp-type revision; code change pending). Core neither calculates nor validates module histograms. These tests do not establish deployed writer round-trip, replay, commit-split facts, identity lifetime, retention, performance or complete lineage.

**Separate Kafka ingestion attachment, locally adapted and exported, not checked into Core:** the requested refactor changes the telemetry flow from `buildBatchTelemetry` to `processBatch` returning `Task[Dataset[BatchTelemetryInput]]`; streaming orchestration is named `innerProcessBatch`. Module-local calculations use Core constants and (at test time) grouped by topic/partition/timestamp type; the revision returns this to topic/partition with the `-1` validity filter. Output supplies `MESSAGE` / `KAFKA`, cluster-qualified source identity, offset/timestamp min/max, actual input-row count, valid timestamp count/start-age decimal sum, non-null value count/bytes sum/min/max, three arrays/version, and null FILE/DELTA fields. No approximate scalar partition percentiles remain.

Both registry/static-schema branches retain their existing data writes, `txnAppId` values and query stamps. Start/end are captured with `Task.eval(Instant.now())`; end follows successful data writes (plus registry upsert on that branch), and the same end goes to histogram calculation and Core persistence. Raw batch data is persisted across data/telemetry work and unpersisted in `finally`.

**Reported local checks:** the full attachment parsed; its exact telemetry method was extracted into a local harness, compiled, and passed seven focused Spark tests covering all published age/size boundaries and adjacent microseconds/bytes; negative/null/empty/overflow populations; separation of mixed timestamp types; sparse offsets counted from actual messages; threshold counts; partition merge equal to the combined distribution; empty batches yielding no rows; invalid source identity/reversed timing failing; and projection into the Core schema. This is not a full external-module compilation or deployment: configuration/CLI classes and module dependencies were unavailable, registry execution was not verified, and no new external configuration/CLI definitions were supplied or changed.

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

The intended identity values come from job parameters defined uniformly in Terraform. The trait has no default implementations, so a config that lacks a member should fail to compile instead of reporting a wrong value. Later local Core compilation does not establish compatibility of unavailable external-module configuration/CLI classes or deployed parameter wiring. Missing platform ids are nullable, not empty-string sentinels. The current contract also includes `layer`, `task_name` and nullable `plant`.

**Accepted trade-off:** the parameter extraction is implemented identically in both config types (two copies) rather than in one shared parser. Judged acceptable because the parameters are defined once in Terraform and are not expected to change. Risk: the copies drift if one is edited without the other.

### Stamp on write, record on read

- **Stamp on write:** propagate the producing query and batch identity in output data. `_query_id` / `_batch_id` are the conceptual pair used here; an existing technical column can carry query identity only if its value matches producer telemetry.
- **Record on read:** every batch writes telemetry rows describing what it consumed: files, message partition ranges, or grouped upstream units in its input. Grouping establishes unique units; a redundant `distinct` after that grouping adds no meaning.

The data carries only the immediate producer's key; the chain lives in telemetry. Recording Silver keys before a Gold aggregation preserves batch dependencies, not exact attribution of every Silver row to every Gold output.

Scope: current module inspection found no streaming stateful operators. That bounds streaming ancestry to the current micro-batch input, but does not mean every input contributed to every output; other joins/lookups also need explicit evidence. A streaming stateful operator (watermarked aggregation, stream-stream join, `*WithState`) would additionally allow outputs to derive from earlier batches, and the original raw input would not be reachable inside `foreachBatch`.

**Multiple outputs do not require an output id for batch ancestry.** A producer can stamp each table without reporting its output list; producer-to-table relationships appear from readers' rows (`source_id` + upstream key). This does not reveal which producer inputs fed each target. For terminal tables, writer completion is only a processing-completion proxy; table-to-writer mapping and actual availability need the dimension or Delta history.

### Grain: one row per input object consumed by one batch

| Source type | Input object | Rows per batch |
|---|---|---|
| File | one file in the input Dataset | one per file |
| Message (Kafka; Event Hubs via Kafka API) | one topic-partition range within source scope | one per topic/partition |
| Delta | one upstream batch (unit) | one per upstream unit consumed, within the declared source/plant scope |

- **Offset-level message detail comes from Bronze itself**, which already behaves like a per-message telemetry table. Lookup: Bronze rows where `_query_id` = `query_id`, `_batch_id` = `batch_id`, `partition` = `message_partition`.
- `input_unit_row_count` replaces the repeated `batch_row_count`: files count input Dataset rows per file (one for `binaryFile`); message sources count input messages per topic/partition; Delta counts input rows grouped by upstream query/batch within the consuming scope.
- These counts are additive within that declared consuming-input scope. They are not comparable business units across layers: one file, its parsed records and messages are different populations.
- Message modules compute counts from message rows, not offset spans (offsets can have gaps).
- If `maxFilesPerTrigger` splits an upstream batch's output, it appears in two downstream batches with partial row counts.

### `batch_telemetry`: current raw contract (local message additions; deployment not verified)

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
source_type           String     not null   FILE | MESSAGE | DELTA
source_id             String     not null   folder root | cluster-qualified topic | table

-- files
file_path             String     nullable
file_modification_ts  Timestamp  nullable
file_functional_ts    Timestamp  nullable
file_size_bytes       Long       nullable

-- message (Kafka-consumer-API semantics; renamed from kafka_* on 2026-10-22)
message_system                              String         nullable   KAFKA | EVENT_HUBS
message_partition                           Int            nullable
message_min_offset                          Long           nullable   Kafka-protocol offset
message_max_offset                          Long           nullable
message_min_ts                              Timestamp      nullable
message_max_ts                              Timestamp      nullable
message_ts_count                            Long           nullable   valid timestamps (non-null, timestampType <> -1)
message_age_at_batch_start_sum_seconds       Decimal(38,18) nullable
message_value_count                         Long           nullable   non-null values
message_value_bytes_sum                     Long           nullable
message_value_bytes_min                     Long           nullable
message_value_bytes_max                     Long           nullable
message_age_at_start_hist                    Array<Long>    nullable   30 buckets in version 1
message_age_at_completion_hist               Array<Long>    nullable   30 buckets; same end as batch_end_ts
message_value_bytes_hist                     Array<Long>    nullable   84 buckets in version 1
message_hist_version                         Int            nullable   both bound sets and semantics; currently 1

-- delta
upstream_query_id     String     nullable
upstream_batch_id     Long       nullable   persisted; input remains Option[String]
```

The earlier layout's partition percentiles (`kafka_age_at_batch_start_p50/p95_seconds`) and briefly added scalar median value length are not combinable and are replaced by three mergeable histograms. The local Core schema/writer now include these fields; the module attachment supplies them. Numeric timestamp type was included at test time and is dropped by the revision (code change pending). This is not an inspected deployed schema or a completed POC. The counter is `message_ts_count`, not the earlier proposed `message_timestamp_count`; timestamp type is neither stored nor used as a grain key; a `MIXED` label was never accepted. All message fields are null on non-message rows. The input/persisted `upstream_batch_id` type mismatch remains R1. Explicit writer casts normalize storage, not proof of input conversion or deployed round-trip.

The upstream query-id comment still permits a legacy technical job name. A non-null string is not enough for a join: it must equal producer `query_id` or have an authoritative mapping. No new module data-write idempotency migration was chosen.

Date decision and current layout direction:

- `batch_date` is a real `DATE`, explicitly derived from `batch_end_ts` in UTC by the writer. It is **not** implemented as a generated column.
- The provided raw layout clusters on (`job_name`, `batch_date`); predictive optimization remains the maintenance direction. `job_name` because people filter by it and `job_id` does not survive job recreation; a rename only affects the label. Performance at production commit rates remains unverified.
- Reasoning: the table is append-only in time order, so files are already roughly time-ordered and min/max statistics skip on time without partitions; identity is the interleaved dimension that clustering fixes. Clustering keys can change later without a rewrite.
- Volume: file rows ≈ legacy `processed_files` rows (the sizing baseline). Message sources are one row per batch × source/topic × partition, which is not automatically the smaller grain: a high-partition topic with frequent triggers can produce more rows per day than a file source.
- Watch items: predictive optimization only runs on Unity Catalog managed tables; clustering is applied mostly during OPTIMIZE, so the newest data is unclustered until compaction. Appends from many writers do not conflict with OPTIMIZE.

- **Intended logical key:** (`query_id`, `batch_id`, `source_id`) + object (`file_path` | `message_partition` | (`upstream_query_id`, `upstream_batch_id`)), including any further grouping scope such as plant. This is a grain definition, not an observed uniqueness constraint; legacy missing identities need explicit handling.
- **Writes:** one append per batch with `txnAppId` = query id and `txnVersion` = batch id; replay deduplication is intended but still needs H3.
- **Sparse nulls are intended.** Prefer explicit type-prefixed raw columns over structs. Integrity checks are proposed, not established deployment behavior; for example:

```sql
ALTER TABLE batch_telemetry ADD CONSTRAINT file_shape CHECK (
  source_type <> 'FILE' OR
  (file_path IS NOT NULL AND message_partition IS NULL AND upstream_batch_id IS NULL));
```

- **Width growth** is the watch item; fine for three or four source types.
- **Not stored as new raw facts:** logical process (dimension); durations and monitoring policies (derived); output relationships (readers' `source_id`). Plant is retained in the current contract. Upstream completion is resolvable context, not necessarily the instant a particular target became available.

### Schema review items

| # | Item | Status |
|---|---|---|
| R1 | Persisted `upstream_batch_id` is Long but input remains `Option[String]`; verify conversion/null behavior and migrate existing storage | Not fully resolved |
| R2 | Repeated batch count replaced by `input_unit_row_count` at declared consuming-unit grain; `binaryFile` uses one per file | Decision/local change; acceptance test pending |
| R3 | Avoid Bronze message self-references in `upstream_*`; Bronze lookup already reaches messages. Null upstream identity on external input differs from a missing stamp on Delta input | Open; coverage must distinguish these cases |
| R4 | Missing Databricks ids are nullable rather than empty-string sentinels | Contract updated; round-trip pending |
| R5 | `job_name` must be the platform job name, not the dated `txnAppId` string | To confirm |
| R6 | `file_functional_ts`: keep if read from file content; if parsed from the file name it is recoverable (fine to keep as convenience) | To confirm |
| R7 | Timestamp type (create vs log-append) | Dropped from grain and schema (revision 2026-10-22); `-1` filtered as invalid; age meaning assumed CreateTime from observation; onboarding check proposed |
| R8 | Message equivalent of file size | Sum/min/max, non-null count and size histogram in local attachment; value bytes only; deployment unverified |
| R9 | `task_name` is in the current contract; verify whether it supplies the required stable task grouping rather than assuming `task_run_id` does | Verification open |
| R10 | An upstream identity may still contain a legacy technical job name; require producer-key equality or an authoritative mapping | Joinability not established by non-null values |
| R11 | `MESSAGE` means Kafka-consumer-API semantics; a native Event Hubs reader needs its own read-API discriminator and position column | Contract to state in code/docs |
| R12 | `source_id` for messages qualified by cluster/namespace (`<cluster>/<topic>`, `<namespace>/<event hub>`) so names cannot collide | Cluster-qualified in local Kafka attachment; external config/CLI and rollout unverified |

### Column rationale

**`source_id`** = the dataset read; same meaning for every type. Uses: part of object identity (a message partition is meaningless without its topic); the only place table names appear in lineage; business-facing grouping key for freshness; distinguishes streams within a job; scope for missing-file detection.
Rules: Delta = full three-part Unity Catalog name; message = topic / event hub, prefixed by cluster or namespace (R12); files = folder root at the level where expectations are defined. Capture it where the stream is defined (shared read helper); tag each input with `lit(source_id)` before a union or join.

**Message columns:** offsets describe consumed position (Kafka-protocol offsets, also for Event Hubs read through the Kafka endpoint; not comparable with portal offsets or sequence numbers) and support diagnostics/Bronze lookup. A discontinuity from the previous maximum plus one is a signal to investigate, not proof of data loss: legitimate offset gaps exist. Offset-distance backlog needs an independently observed broker end offset (exclusive); distance from the next consumed position is not necessarily a message count.

For each topic/partition, start age is batch start minus each valid message timestamp (non-null, type not `-1`); completion age uses the captured end instead. `message_ts_count` and `message_age_at_batch_start_sum_seconds` retain valid coverage and additive age evidence; their compatible-population mean is total sum divided by total valid count when nonzero, not an equal average of partition means. Null timestamps are excluded, coverage is reported against input-message count, and negative ages are retained as evidence of future timestamps or clock/semantic differences. Existing min/max timestamps give the observed population's oldest/newest ages.

Scalar partition percentiles cannot be combined into batch or downstream percentiles; histograms per partition row retain mergeable bucket counts. Timestamp meaning is not stored; ages assume the source's observed type (currently CreateTime everywhere measured), which an onboarding check should confirm per new source. Value-size statistics cover non-null values only, with `message_value_count` as their population.

**File columns:** modification time is timestamp evidence, not automatically landing arrival. Size is the workload property that may be unrecoverable once files are removed. Functional timestamps need their own meaning, separate from arrival and completion.

### Derived objects

**Chosen grains, not a mandate for new physical tables.** Inputs feed `batch_telemetry`, from which `batch_facts` and `batch_dependencies` support job/pipeline monitoring, dashboards and alerts. Optional prepared platform views enrich context without changing the batch grain. Locally observed facts, dynamically resolved ancestry and monitoring policies remain distinct.

| Object | What it is | When it becomes a table |
|---|---|---|
| `batch_telemetry` | raw table, one row per object read | already is |
| `batch_facts` | exactly one flat row per (`query_id`, `batch_id`): metadata, scalar source type, duration, source-specific volumes, sufficient statistics and ready-to-use age/coverage metrics | only if query cost/history needs justify it and refresh correctness is established |
| `batch_dependencies` | observed producer-batch → consumer-batch edges per consumed Delta table, with consumed-row counts | only if lookup cost or retained history requires it |
| Enriched facts / ancestry | views joining dependencies to facts while exposing identity/resolution coverage | if measured lookup cost or retained history requires it |
| Process status | view: expected processes/sources left-joined to latest completion and data-age observations | if the history of evaluated states is needed ("how long was it stale") |
| Daily trends | view over batch facts grouped by logical process/time with population-weighted age metrics | if measured dashboard cost requires it |
| Message continuity | view over raw message rows, respecting source/partition/type grain; a topic-level aggregate would hide a stuck partition | probably never |

### `batch_facts`: flat batch metrics with sufficient statistics

**Decision:** group exactly by (`query_id`, `batch_id`). Plant, source id, file and partition remain in raw drill-down, not primary facts dimensions. There is no chosen `input_scopes` array, nested source summary or `upstream_units` array in facts. An optional raw-membership `EXISTS` lookup can select batches touching one source, but their facts still describe the whole batch, not that source's attributed share.

The facts envelope carries execution ids/names, layer, query/batch ids, start/end, UTC batch date and scalar `source_type`. One batch may read several source ids of the same type. Validate one source type and consistent envelope values per batch before choosing one representative; `any_value` is not an invariant check. An intermediate SQL envelope struct is flattened in the final output.

**Timestamp meaning in facts (after the revision):** with timestamp type dropped, facts carry one age population per batch. This relies on all sources in a batch sharing one timestamp meaning, which is an assumption from observation, not a checked invariant. The earlier open question of exposing multiple age populations in flat facts no longer applies unless a non-CreateTime source appears.

The provided/returned **base shape** contains:

| Population | Base fields / sufficient statistics | Interpretation |
|---|---|---|
| Batch | execution metadata, `source_type`, `processing_duration_seconds` | duration is one batch fact, not additive through input/edge subdivisions |
| FILE workload | `file_count`, `known_size_count`, `total_known_bytes`, `largest_bytes` | one binary input row is one file; known bytes are not complete bytes when sizes are missing |
| FILE modification and functional timestamps, separately | timestamp counts and start-age sum/min/max for each timestamp kind | each family has its own valid-timestamp population |
| MESSAGE | `input_message_count`, `timestamp_count` (from raw `message_ts_count`), `age_at_start_sum_seconds`, `min_ts`, `max_ts`, `value_count`, `value_bytes_sum/min/max`, histograms/version | message volume, valid timestamps and non-null values are different counts; age statistics require separate timestamp meanings |
| DELTA | `input_row_count`, `rows_with_complete_upstream_identity` | input rows, with a separate count for complete producer identity |

These file/message/Delta counts are distinct units. There is no universal business-row total to compare across layers.

**Chosen build behavior; latest extended SQL execution unconfirmed:** prepare source evidence, aggregate at batch grain, flatten envelope values, derive metrics, then produce final completion ages (`prepared` → `aggregated` → `batch_values` → `derived` → `final`). Calculate the following during the facts build rather than in every monitor, keeping the base statistics alongside them:

| Metric family | Build-time derivation |
|---|---|
| FILE modification / functional `age_at_start_mean_seconds` | corresponding start-age sum divided by `NULLIF(valid timestamp count, 0)` |
| FILE timestamp coverage, separately for both timestamp kinds | valid timestamp count divided by `NULLIF(file_count, 0)` |
| MESSAGE mean age at start | age sum divided by `NULLIF(timestamp_count, 0)` |
| MESSAGE minimum / maximum age at start | batch start minus `max_ts` (newest) / batch start minus `min_ts` (oldest), in seconds |
| MESSAGE timestamp coverage | timestamp count divided by `NULLIF(input_message_count, 0)` |
| MESSAGE mean value bytes / value coverage | bytes sum divided by `NULLIF(value_count, 0)` / value count divided by `NULLIF(input_message_count, 0)` |
| DELTA identity coverage | complete-identity rows divided by `NULLIF(input_row_count, 0)` |
| FILE modification / functional and MESSAGE `age_at_completion_min/max/mean_seconds` | each matching start-age statistic plus `processing_duration_seconds` |

Use fractional division, preserve negative ages and leave zero-denominator or unknown metrics null. Do not turn missing sizes, timestamps or identities into successful complete coverage. The completion transformation is valid for the same local input population; it does not establish the timestamp distribution of filtered output rows.

For weighted trends, use `SUM(age_at_start_sum_seconds) / NULLIF(SUM(timestamp_count), 0)`, not `AVG(batch_mean)`. For completion-age mean, use numerator `SUM(age_at_start_sum_seconds + timestamp_count * processing_duration_seconds)` divided by the same total valid timestamp count, with a zero-denominator guard. A positive timestamp count with a missing age sum is an integrity failure to expose, not a count to retain while SQL silently drops the numerator. These population checks apply before both batch aggregation and cross-batch rollups.

Message scalar partition percentiles cannot produce batch or downstream p95. Compatible merged start/completion/size histograms provide approximate batch/window percentiles and exact threshold shares at boundaries; completion-window percentiles use completion arrays, not shifted start arrays. The facts build still needs version/shape/population checks. File percentiles (size, timestamp ages) are exact from raw rows over any window, because raw is one row per file.

### `batch_dependencies`: observed batch edges

The edge columns are `upstream_query_id`, `upstream_batch_id`, `downstream_query_id`, `downstream_batch_id`, `source_id`, `consumed_row_count`. Here `source_id` specifically means the **consumed Delta table**, not a generic file folder or message topic.

Proposed view query, not evidence of executed refresh correctness:

```sql
SELECT
  upstream_query_id,
  upstream_batch_id,
  query_id AS downstream_query_id,
  batch_id AS downstream_batch_id,
  source_id,
  SUM(input_unit_row_count) AS consumed_row_count
FROM batch_telemetry
WHERE source_type = 'DELTA'
  AND upstream_query_id IS NOT NULL
  AND upstream_batch_id IS NOT NULL
GROUP BY
  upstream_query_id, upstream_batch_id,
  query_id, batch_id, source_id;
```

This merges plant subdivisions of the same edge, assuming raw uniqueness and additive counts at that grain. Keep known references even when producer facts are not available; do not inner-join them away. Missing upstream identity remains visible through facts coverage, not edge rows. FILE/MESSAGE batches have no upstream lakehouse batch edge; a DELTA batch without edges is not automatically an external root.

Left-join from dependencies to `batch_facts` **twice**, matching upstream query/batch to producer facts and downstream query/batch to consumer facts. This preserves edges with missing facts on either side. Identity completeness is not resolution coverage: a complete recorded pair can still be historical, unavailable or a legacy name requiring authoritative mapping.

The joined view can calculate downstream start/end minus producer end as stage-timing context. Do not clamp negative differences: output may commit before the producer finishes its whole batch. Recursion supplies ancestry or observed downstream consumption, not predicted backlog or configured topology. Job-to-job relationships alone cannot replace actual batch edges. Deduplicate shared ancestors across fanout/converging paths and do not treat whole ancestor-batch metrics as exact filtered downstream statistics.

### Formation and monitoring context remain conditional

**Incremental completeness is not established.** One atomic telemetry commit does not guarantee that a downstream Delta micro-batch receives the whole logical batch: file admission limits can split the commit's files. A stateless aggregation over each reader micro-batch can therefore produce partial `batch_facts`. Run the commit-split acceptance experiment before choosing an aggregation/reconciliation strategy or declaring this correct.

Query-time identity resolution lets a known reference resolve on a later evaluation when retained upstream telemetry arrives. It does not make missing stamps, truncated history or partial local facts complete; unresolved coverage must remain visible.

**Process dimension:** logical job/task continuity, owner, expected cadence/deadline and tables written (needed for terminal tables) must survive query-id changes. `job_id` plus a suitable task/process discriminator is the starting point; source-level expectations can use raw drill-down. Business can own expectations; engineering owns collection.

### Optional platform enrichment, prepared before joining

Public documentation checked: [jobs system tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs) and [billable usage](https://docs.databricks.com/aws/en/admin/system-tables/billing). These are possible context sources, not evidence that the joins have executed:

| Source | Join / preparation |
|---|---|
| `system.lakeflow.jobs` | match `job_id`; select the latest or appropriate as-of SCD2 version instead of joining all history |
| `system.lakeflow.job_run_timeline` | facts `job_id` = timeline `job_id`, facts `job_run_id` = timeline `run_id`; collapse timeline slices to run context first |
| `system.lakeflow.job_task_run_timeline` | match `job_id`, `job_run_id`, plus facts `task_run_id` = task timeline `run_id`; prepare task-run outcome and `compute_ids` without multiplying batches |
| `system.billing.usage` | match `usage_metadata.job_id` / `job_run_id` where populated; handle billing corrections and prepare usage per run, unit and SKU before joining |

Filter system data to the **current workspace first**: job ids are workspace-scoped. This single-workspace design does not require adding `workspace_id` to raw telemetry. Collapse timeline slices and select SCD2 versions before joins; retain usage-unit/SKU distinctions rather than summing incompatible usage. Validate that each batch-preserving enrichment retains one row per batch.

System records can arrive late and are not a live heartbeat. Run usage repeated on every batch is run context, not additive per-batch cost. Cost needs pricing and an explicit allocation rule; a join does not provide either.

### Freshness

1. **What's done:** processing duration is `end - start`; timestamp age at start and timestamp-to-completion delay describe the observed input population. Call them arrival-to-start wait only where the timestamp really measures arrival. Evaluate time since last completion separately from input age; recent completions can process old data.
2. **What's waiting:** file backlog needs an independent inventory; message offset-distance backlog needs an independently observed exclusive end offset and the next consumed position. Neither is available from successful-input rows alone. Delta pending units remain unresolved: a producer that wrote zero rows to a target cannot be distinguished here from one not yet consumed. Upstream/downstream completion differences are stage context, not proof of queue wait or target availability.

Alert against the expected cadence/deadline from the dimension: the business condition, not a crashing monitoring job. Evaluate "age now" and thresholds at query time, starting from expected processes/sources left-joined to observations. "Late arrival" needs an expectation; observed timestamps alone cannot say late. Historical completion-gap queries must include the preceding observation across the lookback boundary.

By the chosen design, batch start/completion means, bounds and coverage belong in `batch_facts`; the current clock and monitoring policies do not. Use local facts for generic trends, dependency-enriched facts for stage timing/ancestral context, and raw rows for plant/file/partition detail.

The clarified Kafka MESSAGE expectation is roughly 2-5 minutes from publication to parsed Bronze availability, not a file or verified Event Hubs SLA. Candidate 120/300-second boundaries permit exact late shares from the histograms; warning/critical policy and tolerated shares are still open. CreateTime and captured batch end are proxies requiring semantic/availability checks, and observed longer gaps must not relax the expectation.

Caveat: staleness alone cannot tell "pipeline stuck" from "no new input" (a stateless stream with no new data runs no batches, so a low-volume source looks stale overnight). A sharper condition from telemetry alone: downstream is stale **and** upstream has a batch that ended after the latest upstream batch downstream consumed. It can still fire falsely when upstream's newest batches wrote zero rows to the table (same limit as above).

### Lineage

Walk `batch_dependencies` from a Gold batch to Silver and then Bronze batch keys, joining facts for context and raw rows for terminal file/message detail. Different consumed Delta tables remain separate edges via `source_id`. A Bronze lookup can identify that producer population's messages, not automatically the exact downstream subset.

- With valid stamps, identifies immediate producer-batch dependencies and their ancestral objects (batch-level lineage). It does not establish exact contributing files or messages for a selected downstream output.
- File path + row index from `_metadata` is a physical record address, not automatic provenance across transformations.
- A producer can commit an output that a consumer reads before the producer finishes its other writes/reporting. Producer end is therefore not guaranteed to precede consumer start. Do not enforce nonnegative handoff time as a correctness condition.
- Date windows are lookup optimizations, not identity semantics. A short hardcoded horizon can miss historical or late-reported producer facts. Resolve by exact keys over the available history; any bounded search must report unresolved known references, not claim absence.
- Physical pruning depends on layout. Resolving a query id to a job label may help narrow a lookup, but a label or resemblance between names cannot replace identity equality.
- Limits: telemetry/Bronze retention bound resolution; Kafka offsets are scoped to a topic's lifetime. Legacy (`source_id`, `batch_id`) + timing is a qualified heuristic, not exact lineage.
- Do not substitute full producer-population counts, means, percentiles or timestamp bounds for downstream subset statistics. Distinguish ancestry coverage from row/timestamp coverage, and deduplicate shared ancestors when paths converge.

### Proposed guardrails (implementation/acceptance not established)

- **Id mix-up check:** unexpected overlapping windows or keys shared by different streams should trigger investigation. Attempt/replay timestamps also need examination before diagnosing an id mix-up.
- **Message continuity check:** investigate per-partition discontinuities without treating each offset gap as data loss.
- **CHECK constraints** per source type.
- **Batch invariants:** verify raw/facts/edge uniqueness, one scalar source type and consistent execution/timing envelope per batch. `any_value` is selection, not validation.
- **Population checks:** downstream checks must validate message age sum/count and value bytes/count populations; both age-array totals against `message_ts_count`, size-array totals against `message_value_count`, array lengths and version compatibility. Core does not aggregate or validate these supplied histograms. Missing sums with positive counts must not dilute weighted metrics.
- **Stateless check:** propose checking that progress reports no state operators (`stateOperators` empty); module inspection is not runtime enforcement.
- **Identity captured once:** read query id on the batch thread and pass it as a value; reused pool threads can retain another query's local properties. Verify stamps against producer telemetry.
- **One idempotent write per (table, batch):** a second write to the same table with the same `txnAppId`/`txnVersion` is skipped silently. If ever needed: union first, or use a fixed write-site suffix on the app id.
- **Telemetry written last:** intended to describe completed processing, but write/checkpoint failure windows still need replay tests. It does not make multiple data writes and telemetry one transaction.
- **Writer options only:** `txnAppId`/`txnVersion` are set per write, never as session confs (which would apply to every Delta write in the session).

### Example (illustrative values)

| Batch | Window | Telemetry rows (consumed) | Output stamp |
|---|---|---|---|
| Bronze `qB/4812` | 10:00:05–10:03:40 | file `a_1010` 412 MB; file `b_1010` 3 MB | `bronze.readings`: `qB/4812` |
| Bronze `qK/20931` | 10:01:00–10:01:07 | message (`KAFKA`) `topic_x` partition 3, offsets 88100–88949 | `bronze.events`: `qK/20931` |
| Silver `qS/377` | 10:04:10–10:09:30 | `bronze.readings` ← `qB/4812`; `bronze.events` ← `qK/20931` | `silver.readings`: `qS/377` |
| Gold `qG/52` | 10:10:00–10:12:20 | `silver.readings` ← `qS/377` | `gold.kpis`: `qG/52` |

In this synthetic example, Gold 52 has ancestral context `qS/377` → `qB/4812` (files `a_1010`, `b_1010`) and `qK/20931` (partition 3, offsets 88100–88949). The keys alone do not prove that both files or every message contributed to a selected Gold result.

---

## Migration plan

- **Modules build source evidence; Core owns persistence.** Legacy `processed_files` reporting is optional through an existing parameter. Modules use input Dataset, config and processing's start/end instants, calculate their own histograms using the pure Core constants, and supply the evidence. Core adds consuming identity/timing and normalizes storage types before writing; the completion histogram and stored end must use the same instant.
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
| Message one row per message | Per-message detail | Duplicates Bronze at data volume | Rejected |
| Partitioning by `job_name` × `process_date` (+ `layer`) | Partition pruning on identity and time, the main query pattern | Tiny partitions (a table this size needs no partitioning) and a small file per batch per job partition; `layer` has 3 skewed values. Lineage would need cross-day partition reads anyway | Rejected; date-only partitioning remains possible for cheap retention |
| Multiple physical gold tables (`batch_facts`, `lineage_edges`, `process_status`, `daily_rollup`, plus `kafka_partition_facts`) | One table per kind of question | No measured need yet; query-time views can resolve later-arriving ancestry without persisting edges. This does not solve incomplete raw coverage or incremental batch splitting | Set aside; materialize a view only when evidence demands it |
| Primary source/plant-grain facts or `input_scopes ARRAY<STRUCT<source_type,source_id,plant>>` in facts | Preserve source/plant membership with batch metrics | Membership cannot attribute batch-wide metrics; exploding scopes duplicates duration/workload. Raw telemetry supplies drill-down | Rejected; primary facts remain exactly one row per batch |
| `upstream_units` array in `batch_facts` | Keep dependencies inside the batch row | The selected relationship grain belongs in separate `batch_dependencies` | Superseded |
| Lean base facts with all means/coverage calculated in presentation views | Keep only mergeable sufficient statistics in facts | Repeats calculations in every monitor; ready-to-use batch metrics can coexist with sums/counts | Superseded by build-time means, bounds, coverage and completion ages |
| Product-named `KAFKA` source type | Original Kafka-only scope | Event Hubs jobs read through the Kafka API share the layout; broker belongs in `message_system`, not the type name or `source_id` | Superseded by `MESSAGE` (2026-10-22) |
| Partition-level median/percentiles in raw | Monitoring needs medians | Not combinable into batch/day percentiles | Superseded by histograms (under test) |
| KLL sketches in raw (`kll_sketch_*`) | Mergeable approximate quantiles in SQL | Documented from Databricks Runtime 18.0; jobs run 16 LTS. A Scala DataSketches aggregator would need UDFs for merging and unverified format compatibility | Revisit after a runtime upgrade |
| Repeated batch-level median on each partition row (option A) | Simple per-batch statistic, no bucket contract | Not mergeable across batches/days; grain-mismatch pattern needing guardrails | Fallback if only per-batch percentiles prove necessary |
| Exact percentiles on demand from Bronze (option C) | Exact, no contract | Bronze retention and scan cost per evaluation (unmeasured) | Fallback / ground truth for histogram error |
| Bounds lookup table from the start | One bounds source readable by SQL consumers | No non-Core consumer yet; test first | Deferred until a dashboard/non-Core consumer or bounds change needs it |
| Module-defined bounds with Core histogram aggregation/validation | Centralize calculations in a reporting helper | User chose the opposite boundary: pure versioned bounds in Core, module-local calculation, Core persistence only | Explicitly rejected (2026-10-22) |
| One combined age population labeled `MIXED` | Keep one row per topic/partition despite different timestamp types | Labeling after aggregation cannot recover separate timestamp meanings | Rejected |
| Timestamp type as raw grain key | Keep age populations with different meanings separate | Only benefit is interpreting age meaning, a per-source property; all observed sources CreateTime; costs extra rows, continuity complexity and an unsolved flat-facts design | Dropped (2026-10-22); `-1` filtered; onboarding check instead |
| `message_timestamp_type_min/max` validation columns | Detect mixing without splitting grain | Not chosen; user preferred dropping the type entirely | Fallback if a non-CreateTime source appears |
| Daily completion quantiles from merged start histogram plus average duration | Avoid a second age array | Different batch durations change the combined distribution; average shifting loses evidence | Rejected; separate completion histogram |
| Reading checkpoint `metadata` for query id | Simple source of query id | Violates constraint | Rejected |
| Generic columns / struct per type / table per type in raw telemetry | Avoid sparse nulls | Mixed semantics / clumsier SQL / UNION everywhere | Rejected for raw; final facts are flat too, with an envelope struct only as a SQL intermediate |

---

## Hypotheses to verify

| # | Hypothesis | POC step |
|---|---|---|
| H1 | The local property returns the persistent query id: equal to the documented `StreamingQuery.id`, stable across restarts, new after a checkpoint reset, distinct per concurrent query | 1 |
| H2 | Identity propagation leaves existing module data-write behavior unchanged through replay, checkpoint reset and a kill between writes; any broader transaction-id change requires separate approval and tests | 2 |
| H3 | Telemetry append with the same `txnAppId`/`txnVersion` cannot duplicate on replay | 2 |
| H4 | Stamping and per-batch aggregations add acceptable overhead | 3 |
| H5 | Recorded upstream units are complete (every input row stamped; split upstream batches appear in both readers) | 3 |
| H6 | Multi-source-id streams of one source type and multi-output jobs preserve input identity; mixed source types are detected rather than collapsed into a scalar fact | 4 |
| H7 | The lineage walk returns correctly bounded batch ancestry with explicit coverage, without claiming exact contributing files or downstream subset statistics | 6 |
| H8 | The legacy fallback (source + batch id + timing) is a useful qualified heuristic; ambiguity can be detected rather than presented as exact lineage | 7 |
| H9 | New file rows match legacy `processed_files` during dual reporting | 7 |
| H10 | Liquid clustering + predictive optimization keep file counts and lookup cost acceptable at production commit rates | 3, 6 |
| H11 | Counts/sums yield expected message age/value means and coverage with null/future timestamps and null/empty values, without mixing timestamp meanings | 3; local attachment checks support populations, not deployed facts |
| H15 | Retained version-1 histograms answer monitoring queries with acceptable absolute/relative error and overhead across days, including start/completion tails | 3; one-day exact comparisons done, tolerance/stability/performance open |
| H16 | `timestampType` returned through the Event Hubs Kafka endpoint reflects the event hub's append/create configuration (no longer stored; relevant only to the onboarding check) | 3 |
| H12 | Derived facts stay complete when file admission limits split one telemetry commit across reader micro-batches | 6 |
| H13 | Decimal/null values and upstream batch ids round-trip through normalization and the migrated table schema | 3, 7 |
| H14 | Envelope/source-type invariants, uniqueness and population checks preserve one correct facts row per batch and additive dependency counts | 3, 6 |

---

## POC plan

**Step 0: Setup and acceptance queries.** Write acceptance queries for file timestamp-to-completion delay and its duration/age decomposition, completion cadence/gaps (including the lookback predecessor), missing expected sources, inventory-based missing files, message partition diagnostics, and batch ancestry to file/message context. Use synthetic rows carrying file name/row number as ground truth, including a job running two queries and a recently active process reading old data.

**Step 1: Identity (H1).** Log `StreamingQuery.id` in `main` after `start()` and the local-property id in the first batch (must be equal; keep this log to re-check after runtime upgrades). Log the query id and `batchId` in every batch of the two-query job; restart; reset one checkpoint. Pass: ids distinct per query, stable across restart, new after reset.

**Step 2: Idempotency (H2, H3).** Use query id / batch id for telemetry only; retain the modules' existing data-write transaction identifiers. Kill after data write but before telemetry/checkpoint completion and between two data writes; restart and check both data behavior and telemetry deduplication. Reset a checkpoint and record behavior explicitly; do not turn this experiment into an unapproved data-write migration.

**Step 3: Stamp, populations and storage (H4, H5, H11, H13, H14, H15, H16).** Compare module builders plus Core persistence with ground truth. Test one input row per binary file versus parsed rows per file; missing file sizes/timestamps; uneven message partitions, null/future timestamps, null/empty values and sparse offsets; grouped Delta input rows; Decimal/null and upstream-id round-trip. Check zero denominators and missing sums with positive counts.

**Partial evidence, not step completion:** Core's seven focused constant/schema tests and the extracted attachment method's seven Spark tests passed locally, including boundaries, population totals, timestamp-type separation (since dropped) and partition merge equivalence. One-day exact-reference p50/p95/p99 comparisons were run for the reported size/start/completion populations. Still needed: deployed writer round-trip and full module/registry execution; downstream version/shape rejection and old-definition resolution; the `-1` timestamp exclusion and the Spark conversion assumption for `-1`; same persisted end instant after successful writes; crossing-midnight/explicit UTC windows; negative/open-tail monitoring behavior; second-day/weekly distribution tests and missing final-candidate topics. Establish acceptable absolute **and** relative error for actual questions rather than tuning to one sparse tail. Test Event Hubs timestamp behavior with controlled evidence. Measure optional-metric and raw-persistence overhead, not just the base writer.

**Step 4: Multi-source and multi-output (H6).** A Silver job joining two Bronze tables; a job writing two output tables. Verify actual stamp equality with producer telemetry, raw source/plant preservation and the limits of per-target attribution. Test one source type across several source ids and explicit detection of a mixed-type batch.

**Step 5: Failure and replay.** Kill mid-batch at different points; confirm one row per object per unit and that the guardrail checks behave.

**Step 6: Facts and ancestry acceptance (H7, H10, H12, H14).** Execute the proposed extended facts build at the settled (`query_id`, `batch_id`) grain; check one flat row per batch, envelope invariants, start/completion metrics, coverage and weighted rollups. Merge completion arrays for completion-window percentiles; verify exact boundary shares separately from interpolation. Test dependencies merging plant subdivisions while preserving known but unresolved references; left-join both producer and consumer facts. Use a hand-filled expected-process/source dimension. Test an unchanged population versus a filtered subset, duplicated/joined/mixed inputs, one producer split across consumers, and converging paths sharing an ancestor. Compare ancestry and downstream statistics with ground truth, explicitly demonstrating what cannot be attributed. Split one telemetry commit across Delta reader micro-batches; choose reconciliation/aggregation only after observing completeness. If platform enrichment is used, prepare SCD2/run/usage context and verify no batch fanout before treating it as usable.

**Step 7: Migration checks (H8, H9, H13).** Separate missing identity from a known producer outside the export/retention window. Run legacy input and dual reporting; reconcile file paths/counts/sizes against `processed_files`. Redeploy the current test table with jars as planned; any retained existing schema needs an explicit migration check. Confirm Decimal/null behavior after deployment. Local Core compilation now has evidence, but full external-module compilation/regression and deployed acceptance remain open.

**Step 8: Evaluate and write up.** Mark each hypothesis confirmed, refuted or unclear with evidence; estimate production volume; update "What changed" and "Conclusion"; consider a `decisions/` entry.

---

## Decisions

**Made:** one shared raw table/writer; module-specific builders using processing's start/end clocks; consuming-unit input counts; grains (file / message topic-partition / upstream unit within declared scope); offset-level message detail from Bronze; query id from the SparkContext local property; query/batch id for telemetry idempotency only; no universal module data-write transaction-id change; explicit type-prefixed raw columns; nullable platform ids and retained source/plant detail; explicit UTC `DATE` from batch end, not a generated column. Legacy reporting remains optional with dual reporting during migration. These are design/local-code decisions, not completed runtime acceptance.

**Settled on 2026-10-20:** `batch_telemetry` is raw; `batch_facts` is exactly one flat row per (`query_id`, `batch_id`) with scalar source type; `batch_dependencies` carries batch edges per consumed Delta table. No primary source/plant grain, scope array or upstream-unit array in facts. Build means, age bounds, coverage and completion ages into facts alongside sums/counts. Raw supplies detailed drill-down. Derived objects start as views; the chosen grain does not prove materialization/refresh correctness.

**Settled on 2026-10-22:** `MESSAGE` / `message_*` with `message_system` for Kafka and Event Hubs via Kafka API. Timestamp type was first kept in the grain, then dropped (revision, same day): no stored type, grain topic/partition, `-1` timestamps invalid, CreateTime assumed from observation with an onboarding check proposed. Raw keeps sums/counts/min/max and three histogram arrays/version; means belong in facts. Keep tested version 1 for now: 29 age bounds (30 buckets each at start/completion), 83 ceiling-rounded quarter-octave/refined size bounds (84 buckets). Core owns pure constants and persistence, not module histogram calculation/validation. Both bound sets and semantics are immutable per published version; never merge versions, and retain resolvable old definitions. No lookup table yet; the first non-Core consumer or bounds change remains its trigger. The test-table rename needs no migration because redeployment is planned, not because deployed storage compatibility was verified.

**Operational clarification:** roughly 2-5 minutes publication-to-parsed-Bronze for Kafka MESSAGE jobs, not file ingestion or a confirmed Event Hubs expectation. The 120/300-second policy boundaries are candidates, not accepted warning/critical labels or percent-late allowances. Completion is more relevant than start, with timestamp/availability caveats intact.

**Refined:** persisted `upstream_batch_id` is Long, but the input type is still `Option[String]` (R1). Complete identity must also be joinable to producer keys or authoritatively mapped from a legacy name. Delta completion context is observable; pending units/queue wait are not established. Batch ancestry does not imply exact contributing objects or downstream statistical populations.

**Leaning:** job identity (`job_id`, `job_run_id`, `task_run_id`, `job_name`, `task_name`) from Databricks dynamic value references passed as JAR task parameters (`{{job.id}}`, `{{job.run_id}}`, `{{task.run_id}}`, `{{job.name}}`, `{{task.name}}`), parsed once in `main`; local properties as fallback for the ids; null when absent. Verification would address R5 and R9. Proposed guards: a deploy-time parameter check, a runtime warning on Databricks without them, and monitoring for null `job_id`. Not yet tested: overridden JAR parameters, repair runs / task retries, and run id vs task run id on a multi-task job.

**Pending:** unresolved schema review items and deployed acceptance of local Core/attachment changes; full external-module/registry execution; numerical error tolerance, second-day/weekly stability and missing-topic tests; layout performance, retention, ownership and rollout order; extended facts SQL with timestamp-meaning separation, envelope/population/uniqueness checks and commit-split completeness; storage round-trip/deployed verification; identity lifetime, replay/concurrent-query behavior, historical lineage resolution and optional metric cost. Optional system-table joins have no execution evidence and are context proposals, not live heartbeat or allocated batch cost. A universal data-write `txnAppId` switch is not pending rollout scope.

## Open questions

- Beyond the clarified Kafka MESSAGE 2-5-minute expectation, which source expectations exist, who owns them, and which conditions alert through which channel? What, if any, is the Event Hubs expectation?
- Do expectations describe processing cadence or data-arrival cadence? (Staleness alone cannot tell a stuck pipeline from no new input.)
- What should a support engineer answer from the morning email or a dashboard?
- What does the functional/filename date mean, and how are file overwrites handled?
- How will the file inventory be produced for missing-file detection?
- Which Kafka topics / event hubs use create time vs log-append time, and does Event Hubs report the expected numeric type? All inspected sources returned CreateTime, not proof for other sources/configurations.
- What absolute/relative errors are acceptable for actual percentile queries and windows? Do retained candidates remain useful on a second day, weekly rollups, static/small topics, sparse tails and negative/open-tail populations?
- What exactly counts as publication and parsed-output availability? How should candidate 120/300-second boundaries translate into warning/critical conditions or percent-late allowances?
- The local attachment retains raw messages through a post-write completion histogram; will full-module deployment preserve that same end instant in storage, including the registry branch, without unacceptable overhead?
- Does Spark's Kafka source deliver a `-1` record timestamp as a 1969 timestamp rather than null? Who runs the per-source timestamp-type onboarding check, and where is the result kept?
- How will future consumers resolve old histogram definitions, reject incompatible versions and share interpolation logic when a non-Core consumer or bounds change triggers a lookup table?
- Is Bronze retention long enough for the required lineage horizon?
- What does the legacy 65 GB consist of (live data, unvacuumed old versions, small-file overhead)?
- How does the archiving job read the telemetry table (incremental stream or date ranges)? It is a heavy reader, so it affects layout.
- Where do Databricks DQM features fit, if at all?
- If per-partition Kafka lag is needed, where does the partition end offset come from: Spark progress `latestOffset` (partly reverses the listener decision) or broker / Event Hubs metrics outside telemetry?
- What evidence identifies the exact downstream population when ancestry is insufficient, and how will each coverage denominator be exposed?
- How will complete batch formation and refresh be reconciled when Delta reader micro-batches split telemetry commits?
- What will enforce envelope/scalar-source-type consistency, unique facts/edges and matching message age and value sums/counts before metrics are published?
- Can legacy upstream names be mapped authoritatively to producer query ids, and how much history is required to resolve known references?
- If platform enrichment proves useful, what as-of/run-slice preparation and pricing/allocation rules preserve batch grain and honest cost attribution?

---

## Conclusion (provisional, updated 2026-10-22)

The model is now settled at three complementary grains: raw input evidence, one flat fact per batch, and observed batch dependency edges. Generic monitoring does not need primary plant/source-grain facts or membership arrays. It does need ready-to-use means, bounds and coverage, with sufficient statistics retained for weighted trends. Base schemas/results were inspected; the latest extended SQL, platform joins and refresh behavior are not verified execution results.

One-day exact-reference measurements now support the user's decision to retain version-1 size/start/completion histogram candidates. Bucket counts merge exactly under a compatible contract; interpolated percentiles do not become exact. The measured size p99 error reached 7.41% on one sparse configuration tail, and age errors reached about 36 seconds for traceability. No accepted error tolerance, cross-day stability or performance evidence follows from retaining the candidates.

The POC is still not a verified end-to-end monitoring or lineage solution. Local Core compilation/seven focused tests and the extracted attachment method's compilation/seven Spark tests narrow earlier implementation uncertainty. They do not establish full external-module/registry execution, deployed storage round-trip, replay, identity lifetime, retention, commit-split facts or a measured Bronze-to-Gold traversal. Core changes are uncommitted local evidence; the adapted attachment is not a module commit/deployment. A chosen grain is not proof that a streaming refresh constructs whole batches.

The stronger conclusion remains about reasoning: establish grain, counted population, timestamp meaning and evidence coverage before composing ages, asserting lineage or diagnosing backlog. Stable keys make dependencies resolvable; convenient derived metrics do not supply missing population evidence.

## What changed in my understanding?

- Identity is several things with different lifetimes (unit, continuity, attempt, label). The hand-written name + date mixed two of them, and as `txnAppId` it is also a correctness risk.
- Spark's query-id lifetime is the intended basis for identity, unlike per-run `runId`; this internal local-property implementation still needs restart/reset/concurrent-query verification. Propagating it does not require changing every module's write idempotency.
- Start/end are batch facts, while input counts belong to a consuming-unit population. Repeating a batch count or summing repeated durations changes the answer. The previously open facts-grain decision is now one row per batch; detailed source/plant questions stay in raw drill-down.
- Stamping and recording immediate keys gives batch dependencies without an output list, not exact file/message attribution for every downstream output. Full producer means/bounds and scalar percentiles do not automatically describe a consumed subset.
- The initial two-table design was a response to a missing piece of data, not a demonstrated raw-storage need; data-borne keys support one raw table. Separate batch facts and dependencies answer genuinely different grains without demanding separate physical tables.
- An array of source/plant memberships cannot allocate batch-wide metrics. Keeping the final facts flat prevents accidental scope explosion from looking like attributed measurements.
- Convenient batch means and mergeable sums/counts are not competing designs. Build both, then use population-weighted sums for trends rather than averaging batch means.
- Histogram counts merge exactly, but interpolation assumes shape inside a bucket. An estimate below all observed values in its median bucket exposed that assumption; exact-size point masses are legitimate, not a reason to require every bucket to be sparsely populated.
- Report absolute and relative percentile error together: a few percent can mean tens of seconds in a slow tail, while 10% can mean a small fraction of a second. A measured bucket interval is uncertainty about location, not a confidence interval or a guaranteed error bound.
- Threshold shares and percentiles answer different questions. Boundary-aligned shares can be exact even when p99 interpolation is approximate; no accepted alert policy follows merely from having 120/300-second bounds.
- Sampled distributions help choose a stable versioned contract, not prove future stability. Screening samples, exact one-day comparisons, cross-day validation and builder performance are separate evidence.
- Start age shows delay already present before processing; completion age better matches parsed-output timeliness. Daily completion quantiles need their own histogram, not an average-duration shift or subtraction of percentile values.
- Timestamp meaning is part of the population grain. CreateTime is producer-supplied, not proof of publication/business event time; grouping first and labeling `MIXED` later destroys that distinction. (Later revision: the type was dropped from the grain because all observed sources share CreateTime; the distinction is now an assumption to check per source, not stored evidence.) A slow observed population does not justify relaxing the stated operational expectation.
- Joining run timelines, SCD2 jobs or billing slices can multiply correct facts. Prepare context first; repeated run usage is not additive per-batch cost.
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

Choose batch grain for generic monitoring and edge grain for relationships; retain raw detail and timestamp meaning for drill-down. Mergeable evidence is not automatically an exact percentile or a compatible population. Evaluate thresholds separately from interpolation, compare absolute and relative error, and capture completion with the same clock used in storage. Keep local statistics, ancestry, cadence and backlog separate; neither a dependency edge nor a passing local test proves more than its measured scope.
