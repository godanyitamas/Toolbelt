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

> **Handoff (2026-10-23)**
>
> - **Settled:** three grains (raw `batch_telemetry`, flat `batch_facts` per batch, `batch_dependencies` edges), Spark/platform identity, one raw table → `decisions/batch-telemetry-model.md`. Persisted `hourly_facts` from raw for trends, `batch_facts` also persisted (2026-10-25). Message histograms version 1, timestamp type dropped → `decisions/message-histogram-contract-v1.md`.
> - **Local code (uncommitted, not deployed):** Core constants/schema/writer compile, seven focused tests passed; the adapted Kafka builder's extracted telemetry method passed seven Spark tests. The timestamp-type grouping and column are now removed locally (2026-10-23). The `-1` validity filter and the 1969 conversion test are deliberately deferred as low priority; fallback guard proposed below.
> - **Measured (one production day):** size/age distributions; histogram percentile errors against exact references; completion-age shares. One population exceeds the 2–5 min expectation (see Evidence).
> - **Not verified:** identity lifetime (H1), replay (H2/H3), deployed round-trip, full module build, facts build, commit-split completeness, lineage traversal, cross-day stability, performance.
> - **Scope clarified (2026-10-23):** engineering delivers metrics; the customer's business side owns alert policy. Monitored families: volume per time window and message age.
> - **Next:** investigate the slow population; second-day histogram comparison; first facts build with histograms; first `hourly_facts` build; H1 and replay tests.

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

One hour per job and source, aggregated directly from raw, is the grain for time-window trends (volume, duration, age, size); one batch is the grain for batch questions, cadence and per-batch reports; one observed batch dependency is the grain for relationships. Raw telemetry retains plant/file/partition detail. The useful common envelope is identity and processing timing, followed by source-specific input volumes and statistics, not a universal business-row count.

A shared row shape and stable keys are not enough: each metric needs a declared population, timestamp meaning and coverage. Convenient means and bounds should be built once with the facts, while sums/counts remain available for correct weighted rollups. Local stage statistics, ancestral context, processing cadence and backlog still answer different questions.

Histogram bucket counts merge exactly only for compatible bounds/version, metric and population semantics. Percentile interpolation inside the merged buckets is still approximate. Age at start and age at completion need separate arrays for window rollups. Each partition row holds one age population; its timestamp meaning is assumed per source and should be checked at onboarding rather than stored. The tested distributions inform a stable versioned contract, not a promise that future data follows the same shape. Local tests narrow implementation uncertainty without establishing deployed behavior, replay safety or complete lineage.

---

## Observations and constraints

### Motivation

- `processed_files` is file-oriented. Other monitoring uses custom metrics and separate monitoring jobs; some alerts depend on a monitoring job crashing, which detects a failed implementation rather than the business condition.
- At least one delay was missed. Critical sources have downstream consumers; late or bad data has reputational impact. A support-team morning email is an existing delivery surface; dashboards may also benefit.
- Databricks Data Quality Monitoring was considered; no decision (concern: profiling cost on large tables).

### Current setup

- Every pipeline runs through a shared `foreachBatch` reporting wrapper in a shared **Core** module. Modules build source-specific telemetry; Core stamps consuming identity/timing, normalizes storage types and writes the raw table.
- Builders use processing's own batch-start instant; the adapted Kafka builder also receives a post-write end instant, which Core persists as `batch_end_ts`.
- The wrapper does not know a job's outputs; some jobs write several tables; some run several streaming queries per session. Dedicated job clusters; append-only writes.
- Writes inside one `foreachBatch` are sequential on the batch thread, one per table; no stateful streaming operators; no parallel write threads (from reading the modules, not runtime-checked).
- Most jobs propagate `batchId` in output data. Idempotent Delta writes use `txnAppId` = hand-written job name + launch date, `txnVersion` = `batchId`.
- Kafka Bronze persists the raw message with topic, partition, offset and timestamp.
- Legacy `processed_files` (roughly 50–75 file datasources) is about 65 GB with many small files, likely never OPTIMIZEd/VACUUMed; composition not broken down.
- Readers of the telemetry table: ad-hoc queries, monitoring jobs, one large archiving job. Goal: monitoring and dashboards read the facts, not raw.
- Databricks job ids are not in the SparkSession conf on a real JAR job run; as SparkContext local properties, job id, job run id and task run id are returned (no job name; which property is job run vs task run is unverified).
- The `sql.streaming.queryId` local property returned a value locally and on Databricks: the mechanism works; its lifetime is H1.

### Constraints (my decisions)

- Identity never from checkpoint locations or hand-written job names.
- No `output_id`, `status` or generated keys without a concrete need.
- One raw table, one shared writer; derived objects do not automatically need physical tables.
- Capture now what cannot be recovered later; compute the rest downstream.
- Explicit type-specific columns over generic columns with mixed semantics.
- `sql.streaming.queryId` is internal (a runtime upgrade could remove it; fails loudly via `require`); `sparkContext` is unavailable under Spark Connect, so identity depends on dedicated job clusters.

### Source-specific facts

- **Files:** path, size, optional functional date, plant identity from folder convention. A path may not identify a unique file instance if files are overwritten. Never-processed files need an inventory.
- **Messages:** identity is topic + partition + offset; keys repeat or are null. Timestamps can mean create time or log-append time, not necessarily queue wait. Semantics follow the **read API**: Event Hubs through the Kafka endpoint yields Kafka-protocol offsets, not portal offsets or sequence numbers; documentation summaries suggest Event Hubs defaults to append time and Kafka to create time (unverified on our event hubs).
- **Delta:** commit versions, data files, downstream micro-batches and an upstream job's `batchId` are distinct identities. A Gold query does not inherit Silver's `batchId`.

---

## Investigation log

### 2026-10-19: populations, storage types, monitoring questions

- **Observed:** a file builder repeated the batch file count on every file row, so sums overcounted. For `binaryFile`, one row is one file (`input_unit_row_count = 1`); parsed rows per file are a different population.
- **Observed:** `SUM(DECIMAL(24,6))` produced `DECIMAL(34,6)` against a `DECIMAL(38,18)` contract; `.as[CaseClass]` and the decimal annotation did not normalize the expression type. The writer now casts explicitly (local change).
- **Observed in exports:** Kafka message counts matched timestamp counts (sample only). Some Delta rows lacked upstream identity; others pointed to producer batches outside the export window. The same upstream batch appeared in two consumer batches (split reads). No complete Bronze-to-Gold traversal.
- **Changed the question:** existing checks showed recent active batches consuming old data. Healthy cadence does not imply fresh input. File modification time is not landing time; message timestamp age is not queue wait; lateness needs an expectation; backlog needs inventory or broker positions.
- **Direction:** persist timestamps and population evidence; compute "age now" and thresholds at evaluation; start from expected processes/sources and left-join observations; gap checks need the predecessor across the lookback boundary.
- **Lineage limit:** an edge says a consumer batch read rows stamped by a producer batch, not that every ancestor file contributed to a given output. Producer means shift to consumer ages only for an identical population retained once; filtered subsets, scalar percentiles and ancestor timestamp bounds do not transfer. Coverage must name its denominator.

### 2026-10-20: facts grain settled

- **Decided:** `batch_facts` exactly one flat row per (`query_id`, `batch_id`); `batch_dependencies` separate; `input_scopes` membership array rejected (cannot attribute batch metrics; exploding duplicates them). Raw is the drill-down.
- **Observed:** schema layouts and base-facts results; supports the base aggregation shape, not refresh correctness or lineage.
- **Decided:** ready-to-use means, bounds, coverage and completion ages are built in the facts build alongside sums/counts (staged SQL proposal, not run).
- A scalar `source_type` requires one source type per batch; `any_value` selects but does not validate envelope consistency.

### 2026-10-22: message sources, histograms, timestamp type

- **Decided:** `KAFKA` → `MESSAGE` with `message_*` columns and `message_system` (`KAFKA` | `EVENT_HUBS`), because Event Hubs jobs read through the Kafka API share the layout. `MESSAGE` means Kafka-consumer-API semantics; a native Event Hubs reader would need its own position columns.
- **Decided:** raw keeps mergeable sums/counts/min/max for value bytes and age; means in facts. Null values: `message_value_count = count(value)`; empty value = 0 bytes.
- **Percentiles:** batch percentiles are exact from facts; file percentiles are exact from raw (one row per file); message percentiles need per-message evidence. Options compared: repeated batch median, histograms, exact from Bronze. **Decided:** histograms, under test, bounds chosen from production measurements (Evidence below).
- **Decided:** Core owns only a pure constants object and persistence; modules calculate histograms. No bounds lookup table until a non-Core consumer or a bounds change needs one.
- **Decided, then revised the same day:** timestamp type was first a grain key, then **dropped** from grain and schema. Its only benefit was interpreting age meaning, a per-source property; every measured source returned CreateTime; as a grain key it added rows, complicated continuity checks and left flat facts with several age populations. Timestamps with type `-1` are treated as invalid. Losses accepted: age meaning is an assumption; a silent create/append switch would go unnoticed.
- **Clarified expectation:** Kafka MESSAGE jobs should make parsed Bronze data available roughly 2–5 minutes after publication (not files; not established for Event Hubs).

Contract details: `decisions/message-histogram-contract-v1.md`.

### 2026-10-25: trend layer

- **Changed the question:** exact hourly file percentiles need raw (batch medians do not merge), and hourly message statistics from raw equal those from facts. Building hourly trends on top of `batch_facts` therefore saves no calculation.
- **Decided:** a persisted `hourly_facts` table built directly from raw, one row per (`job_id`, `source_id`, processing hour in UTC), serving every time-window monitoring purpose; `batch_facts` stays and is also persisted, for batch-based reports and visualizations. Keyed on `job_id` because query ids change after a checkpoint reset.
- **Why persist:** monitoring jobs and dashboards today recompute statistics on every refresh; computing them once on the clustered raw table is expected to be cheap and to make reads faster (not measured). Retention was not the reason.
- **Decided:** daily file percentiles are not needed by current requests; a view over raw pruned by `batch_date` suffices while raw is retained.
- **Accepted cost:** persisted tables need a refresh that rewrites recent hours and batches for late or replayed batches and split commits.

Details: `decisions/batch-telemetry-model.md`.

---

## Evidence

### Histogram candidates versus exact same-day references (one production day)

Generalized populations. Screening used `TABLESAMPLE` and approximate percentiles; these comparisons used exact references on bounded days. Batches crossing midnight were excluded in initial tests; later age tests used completion within the day in the session timezone (UTC must be explicit for UTC reporting).

**Value sizes** were heterogeneous: traceability events at exact sizes in tens/hundreds of bytes; static events narrow around 700 bytes; a smaller event population near 1 KB and 5.6–8 KB; a larger population from hundreds of bytes to about 1 MB (median about 2.9 KB, mean about 82 KB); configuration values with a broad right tail. No null/empty values in samples.

| Population | Size p50 / p95 / p99 relative error | Absolute error (bytes) |
|---|---|---|
| Larger event population | 2.47% / 0.62% / 1.75% | 73 / 3785 / 16032 |
| Smaller event population | 0.03% / 0.34% / 2.39% | 2 / 27 / 210 |
| Traceability events | 2.73% / 0.46% / 1.26% | 3.9 / 1 / 2.9 |
| Configuration values | 3.02% / 1.87% / 7.41% | 476 / 3370 / 19539 |

Configuration had only 1487 values: a sparse tail, not a reason to tune bounds to one day.

**Interpolation counterexample:** in the smaller event population (462,773 values), the unrefined median bucket was about 4871–5793 bytes, but all values inside it were 5652–5790. Uniform interpolation estimated about 5417, below every observed value. Refinements at 5632/5760 fixed that day's median. The earlier criterion "no bucket should hold most messages" was wrong: point masses are legitimate; judge by the questions and measured errors.

**Ages (producer CreateTime):**

| Population | Median completion age | Completion p99 | Share ≥300 s / ≥600 s | Max abs. percentile error, start / completion |
|---|---|---|---|---|
| Larger event population | about 6.2 s | about 29 s | 0% / – | 2.15 s / 0.64 s |
| Smaller event population | about 5.5 s | about 611 s | 1.58% / 1.02% | 1.48 s / 7.75 s |
| Result events | about 1 s | about 2.9 s | 0% / – | 0.27 s / 0.04 s |
| Traceability events | about 189 s | about 1217 s | 35.88% / 11.54% | 34.24 s / 36.53 s |

All histogram totals matched their counts; no tested percentile fell in an open tail. Some short-age relative errors were about 10–11% but small in seconds; a 3% p99 error on traceability meant about 36 s. These are one-day measurements, not guaranteed bounds.

### Observed: one population exceeds the expectation

Against the 2–5 min expectation, traceability events had a **median completion age of about 189 s and 35.9% of messages ≥300 s**; the smaller event population's p99 was about 611 s. Most of the age was already present at batch start, so slow parsing is not the main contributor. Competing explanations: consumer backlog, producer buffering or clock skew (CreateTime is producer-supplied), transport, scheduling. Not investigated yet; proposed discriminating checks in Open questions.

### Local implementation evidence and its boundary

- **Core (local, uncommitted):** constants, schema and writer compile; seven focused tests passed (published age vector, size formula, version, increasing bounds, nullable schema compatibility).
- **Adapted Kafka builder (local, not a module commit):** telemetry computed in the module from Core constants; start and post-write end instants captured once and reused for the completion histogram and persistence; the raw batch stays persisted across data and telemetry work. Its extracted telemetry method passed seven Spark tests: bucket boundaries and adjacent values; negative/null/empty/overflow populations; timestamp-type separation (since removed from the code; tests not re-run after the removal); counts from messages, not offset spans; threshold counts; partition merge equal to the combined distribution; empty batches; invalid identity/timing failing; projection into the Core schema.
- **Not established:** full module compilation (configuration classes unavailable), registry branch execution, deployed round-trip, replay, performance.

---

## Design (summary)

Stable decisions and their reasoning: `decisions/batch-telemetry-model.md` and `decisions/message-histogram-contract-v1.md`. Full column lists and SQL drafts live in the work codebase; this section keeps what the reasoning needs.

### Principles

1. Capture now only what cannot be recovered later; derive the rest downstream.
2. Unify the questions, not the metadata: explicit type-specific columns, no column whose meaning changes by row.
3. Identity comes from Spark and the platform, never typed by hand.
4. Carry identity in the data across hand-offs instead of reconstructing it from timing or logs.

### Identity

| Identity | Columns | Lifetime | Supplied by |
|---|---|---|---|
| Processing unit | (`query_id`, `batch_id`) | stable across restarts while the checkpoint persists; new after a checkpoint reset | Spark local property, read once on the batch thread |
| Continuity | `job_id` (+ process dimension) | until the job is recreated | platform parameters / local properties |
| Attempt | `job_run_id`, `task_run_id` | per run | platform parameters / local properties |
| Label | `job_name`, `task_name` | as stable as platform names | platform parameters |

Reporting config is a small Core trait implemented by both config types, without defaults so a missing member fails to compile; identity values come from job parameters defined in Terraform (leaning: dynamic value references, local properties as fallback, null when absent). Accepted trade-off: the parameter parsing exists twice, once per config type.

### Stamp on write, record on read

Producers stamp `_query_id`/`_batch_id` in output data (an existing technical column counts only if it equals producer telemetry identity). Consumers record what they read. The data carries only the immediate producer's key; the chain lives in telemetry. No output list is needed for backward ancestry; exact per-target attribution is not available.

### Raw grain and contract

| Source type | Row = | Key object |
|---|---|---|
| FILE | one file in the input Dataset | `file_path` |
| MESSAGE | one topic/partition range | `message_partition` |
| DELTA | one upstream batch within the declared source/plant scope | (`upstream_query_id`, `upstream_batch_id`) |

Logical key: (`query_id`, `batch_id`, `source_id`) + key object (+ plant scope). Column groups: consuming envelope (identity, layer, plant, start/end, UTC `batch_date` from end, `input_unit_row_count`, `source_type`, `source_id`); file (path, modification/functional timestamps, size); message (system, partition, min/max offset and timestamp, `message_ts_count`, start-age sum, value count/bytes sum/min/max, three histograms + version); Delta (upstream query/batch id). Sparse nulls by design.

- `source_id` = the dataset read: three-part table name; cluster- or namespace-qualified topic/event hub; folder root at the level where expectations are defined.
- Counts are additive within their consuming scope, not comparable business units across layers. Message counts come from rows, not offset spans.
- Offset-level message detail comes from Bronze by (`_query_id`, `_batch_id`, partition).
- A timestamp is valid only if non-null and `timestampType <> -1`; negative ages are kept as evidence.
- Telemetry append per batch with `txnAppId` = query id, `txnVersion` = batch id.
- Layout: liquid clustering on (`job_name`, `batch_date`), predictive optimization; no partitioning. Message rows (batch × partition) can outnumber file rows.

### Facts and dependencies

- `batch_facts`: one flat row per batch; validate one source type and consistent envelope before selecting it. Build means, bounds, coverage and completion ages alongside sums/counts; zero denominators stay null; missing evidence never becomes complete coverage.
- Weighted trends use `SUM(sum) / NULLIF(SUM(count), 0)`, never `AVG(batch_mean)`. A positive count with a missing sum is an integrity failure.
- Histograms in facts: merge partition arrays per batch (exact); derive exact shares at bounds and interpolated percentiles (flag open tails); keep merged arrays so daily rollups merge facts again. Completion-window percentiles use completion arrays.
- One age population per batch assumes all its sources share one timestamp meaning (observed, not checked).
- `hourly_facts`: one row per (`job_id`, `source_id`, processing hour), built from raw, persisted; refresh rewrites recent hours. Details in `decisions/batch-telemetry-model.md`.
- `batch_dependencies`: (`upstream_query_id`, `upstream_batch_id`, downstream query/batch, consumed Delta table, `consumed_row_count`), summed over plant subdivisions. Keep known references even without producer facts; left-join facts on both sides; do not clamp negative hand-off times.
- **Formation is not established:** a reader's file admission limits can split one telemetry commit, producing partial facts. Run the commit-split experiment before choosing reconciliation.
- Optional platform enrichment ([jobs system tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs), [billable usage](https://docs.databricks.com/aws/en/admin/system-tables/billing)): filter to the workspace, collapse run slices and SCD2 versions before joining, keep one row per batch. Run usage is not per-batch cost without an allocation rule.

### Freshness

- **Done:** processing duration; timestamp age at start and at completion for the observed population; time since last completion, evaluated separately (recent completions can process old data).
- **Waiting:** needs a file inventory or broker end offsets; Delta pending units remain unresolved.
- Alert against expected cadence/deadline from a process dimension, evaluated at query time from expected processes left-joined to observations. Candidate 120/300 s bounds give exact late shares. Observed slowness must not relax the expectation.
- Staleness alone cannot tell "stuck" from "no new input"; sharper: downstream stale **and** upstream has a newer batch than the latest one consumed (still fooled by zero-row writes).

### Monitoring scope (2026-10-23)

**User decisions:**

- **Responsibility split:** the customer's business side owns alert policy (thresholds, warning/critical, tolerated late shares). Engineering delivers metrics detailed enough to build valuable monitoring on, and establishes trends.
- **Accepted measurements:** CreateTime as publication time; `batch_end_ts` (post-write) as availability time. The caveats stay: CreateTime is producer-supplied; batch end is a writer-side proxy for target availability.
- **Expectation ballpark:** roughly 2–5 min from publication to parsed Bronze for Kafka MESSAGE jobs.
- **Two metric families to trend:**
  - **Volume per time window** (primary): `SUM(input_message_count)` per hour/day and source. Messages per batch stays a diagnostic only: it depends on trigger cadence and batch size, and a batch catching up on backlog looks like a volume spike although the source did not change. Growing batch sizes can indicate a consumer falling behind.
  - **Message age:** completion-age shares at the 120/300 s bounds (exact from merged histograms) plus interpolated p50/p95; means from sums.
- **Granularity:** engineering stores hourly (`hourly_facts`); daily is derived. How fast a problem must be noticed, and therefore whether alerts act hourly or report daily, stays with the business side.

**Agent proposals, not decided:**

- *(Decided 2026-10-25: hourly base.)* Histograms and sums merge exactly at any window, so the facts support both granularities; an hourly base with daily rollups is a reasonable default. For a 2–5 min expectation, daily works as a trend, not for reacting. The deciding question for the business side: how fast must someone notice a problem?
- Trend baselines should account for seasonality (shifts, production schedules): compare with the same hour on previous weeks rather than the previous hour.
- Zero volume is invisible as rows (no input, no batch); detect it from the expected-process list left-joined to observations, as above.
- **Fallback for the deferred `-1` filter:** if a batch's `message_min_ts` is implausibly old (e.g. before 2000), the facts build nulls its age metrics and flags it, so one invalid timestamp cannot corrupt means or histograms unnoticed.

### Lineage limits

Walking edges gives batch ancestry and ancestral files/message ranges, not exact contributing objects for a selected output. `_metadata` path + row index is a physical address, not provenance. Producer end need not precede consumer start. Date windows are lookup optimizations: report unresolved known references rather than claiming absence. Retention bounds resolution. Legacy (`source_id`, `batch_id`) + timing is a qualified heuristic. Deduplicate shared ancestors when paths converge.

### Proposed guardrails (not implemented)

Id mix-up check (overlapping windows/keys across streams); per-partition offset continuity (gaps are signals, not loss); CHECK constraints per source type; raw/facts/edge uniqueness and envelope invariants; population checks (histogram totals vs counts, array length, one version per merge); stateless check (`stateOperators` empty); query id captured once on the batch thread; one idempotent write per table and batch; telemetry written last; `txnAppId`/`txnVersion` as writer options only.

### Open schema items

| # | Item | Status |
|---|---|---|
| R1 | Persisted `upstream_batch_id` is Long, input remains `Option[String]` | Conversion and null behavior unverified |
| R3 | Bronze message rows must not self-reference in `upstream_*`; null upstream identity on external input differs from a missing stamp | Coverage must distinguish |
| R5 | `job_name` must be the platform job name, not the dated `txnAppId` string | To confirm |
| R6 | `file_functional_ts` from content vs file name (recoverable) | To confirm |
| R9 | `task_name` as stable task grouping | To verify |
| R10 | Upstream identity may contain a legacy job name; joins need equality or an authoritative mapping | Not established |
| R11 | `MESSAGE` = Kafka-consumer-API semantics; native Event Hubs needs its own columns | To state in code/docs |
| R12 | Message `source_id` qualified by cluster/namespace | In local builder; rollout unverified |

---

## Migration plan

- **Modules build evidence; Core persists.** Modules use input, config and processing's start/end instants and compute their statistics and histograms from the Core constants; Core adds identity/timing, normalizes types and writes. Completion histogram and stored end use the same instant.
- **Dual reporting:** legacy `processed_files` stays optional through an existing parameter; each job migrates to the new Core version once.
- **Validate contracts:** required fields per source type; missing identity as explicit coverage; Decimal/null and upstream-id round-trip; the test table is redeployed with the jars.
- **Producers before consumers** (Bronze → Silver → Gold): consumers can record upstream keys only once producers stamp them.
- **Stamp propagation separate from data-write idempotency:** module `txnAppId` values stay unchanged; any later switch needs its own replay plan.
- **Reconcile before switching legacy off:** compare paths, counts and sizes per batch for a period.

---

## Alternatives considered and set aside

| Alternative | Why set aside | Status |
|---|---|---|
| Two raw tables (wrapper slices + StreamingQueryListener progress) | Data-borne stamps make listener offsets unnecessary | Fallback; draft listener exists |
| Query id from a session-wide listener | Overwritten across concurrent queries | Cross-check only |
| Commit `userMetadata` stamp | Superseded by stamping the data | Tie-breaker for multi-writer tables |
| Modification-time window matching | Heuristic; needs table → writer mapping | Fallback without stamps |
| Delta one row per consumed data file | Heavier; batch identity answers dependency questions | Fallback for physical-file evidence |
| Producer record from the target's Delta log (`txn` actions) | Interpretation unverified (app-id mapping, empty appends, retention) | Fallback for Delta "what's waiting" |
| Per-module reporting value object | Trait gives the same compile-time guarantee with less code | Fallback for configs outside Core |
| Message one row per message | Duplicates Bronze | Rejected |
| Partitioning by job × date (+ layer) | Tiny skewed partitions | Rejected |
| Several physical gold tables | No measured need; views suffice | Two persisted (`batch_facts`, `hourly_facts`) for expected monitoring read cost, not measured; others stay views |
| Source/plant-grain facts, `input_scopes` or `upstream_units` arrays | Membership cannot attribute batch metrics | Rejected |
| Lean facts with all means in presentation views | Repeats work in every monitor | Superseded |
| Product-named `KAFKA` type | Broker belongs in `message_system` | Superseded by `MESSAGE` |
| Partition percentiles; repeated batch median; Bronze on demand; KLL sketches | Not mergeable / no window rollups / scan cost / needs DBR 18 | See histogram decision |
| Module-defined bounds with Core histogram validation | Chose pure constants in Core, calculation in modules | Rejected |
| Timestamp type as grain key; `MIXED` label; min/max validation columns | Per-source property; all sources CreateTime; `MIXED` loses evidence | Dropped / rejected / fallback |
| Daily completion quantiles from start histogram + average duration | Batch durations differ | Rejected |
| Checkpoint `metadata` for query id | Violates constraint | Rejected |
| Generic columns / structs / table per type in raw | Mixed semantics or UNIONs everywhere | Rejected |

---

## Hypotheses and POC status

| # | Hypothesis | Step | Status |
|---|---|---|---|
| H1 | Local-property query id equals `StreamingQuery.id`, is stable across restarts, new after checkpoint reset, distinct per concurrent query | 1 | Mechanism observed; lifetime open |
| H2 | Identity propagation leaves module data writes unchanged through replay, checkpoint reset and kills between writes | 2 | Open |
| H3 | Telemetry append with the same `txnAppId`/`txnVersion` cannot duplicate on replay | 2 | Open |
| H4 | Stamping, aggregations and the persisted batch add acceptable overhead | 3 | Open |
| H5 | Recorded upstream units are complete; split upstream batches appear in both readers | 3 | Split read observed once; completeness open |
| H6 | Multi-source and multi-output jobs preserve input identity; mixed source types are detected | 4 | Open |
| H7 | The lineage walk returns bounded batch ancestry with explicit coverage | 6 | Open |
| H8 | The legacy fallback is a useful qualified heuristic with detectable ambiguity | 7 | Open |
| H9 | New file rows match legacy `processed_files` during dual reporting | 7 | Open |
| H10 | Clustering + predictive optimization keep file counts and lookup cost acceptable | 3, 6 | Open |
| H11 | Counts/sums give correct message means and coverage with null/future/`-1` timestamps and null/empty values | 3 | Local tests partial; `-1` filter deliberately deferred (facts-build fallback proposed) |
| H12 | Facts stay complete when readers split one telemetry commit | 6 | Open |
| H13 | Decimal/null values and upstream batch ids round-trip through storage | 3, 7 | Local cast; deployed open |
| H14 | Envelope invariants, uniqueness and population checks give one correct facts row per batch | 3, 6 | Open |
| H15 | Version-1 histograms answer monitoring queries with acceptable error and overhead across days | 3 | One day measured; stability, performance open; tolerance follows from the business side's thresholds |
| H16 | Event Hubs via Kafka endpoint reports its configured timestamp type | 3 | Open; now only relevant to onboarding checks |

**POC steps:** 0 acceptance queries and synthetic ground truth (two-query job, recent process reading old data); 1 identity; 2 idempotency and replay (telemetry only, no data-write migration); 3 stamps, populations, storage, histograms (second day, `-1`, size tail near 1 MB, overhead); 4 multi-source/multi-output; 5 failure and replay; 6 facts and ancestry (histogram merges, commit split, converging paths, unresolved references); 7 migration checks and legacy reconciliation; 8 evaluate and write up.

---

## Open questions

**Freshness and expectations**

- Why do traceability events exceed the 2–5 min expectation? Proposed checks on Bronze for one day: compare start age with the interval between batch starts (consumer backlog); check whether CreateTime is non-monotonic in offset order within a partition (producer buffering/clock); compare consumed max offsets with broker end offsets where available.
- To agree with the business side: how fast must a problem be noticed (sets hourly vs daily), which thresholds and late shares become warning/critical, and through which channel?
- Which baseline defines a trend (same hour previous weeks, rolling window), and which volume deviation is meaningful?
- Do expectations describe processing cadence or arrival cadence? What should a support engineer read from the morning email or a dashboard?

**Histograms and timestamps**

- What absolute and relative errors are acceptable for the actual queries? Do version-1 bounds hold on a second day, weekly, for static/small topics and near the 1 MB size tail?
- Does Spark deliver a `-1` record timestamp as 1969 rather than null? (Deferred; low priority while all sources are CreateTime.) Who runs the per-source timestamp-type onboarding check, and where is the result kept?
- What does keeping the raw batch persisted across writes cost on large batches?
- How will future consumers resolve old histogram versions and share interpolation logic once a lookup table is needed?

**Facts, lineage and layout**

- How will whole-batch formation be reconciled when readers split telemetry commits?
- How far back must the `hourly_facts` and `batch_facts` builds rewrite to absorb late or replayed batches?
- What will enforce envelope consistency, uniqueness and population checks before metrics are published?
- How much history is needed to resolve known references? Is Bronze retention long enough?
- What evidence identifies the exact downstream population when ancestry is insufficient?
- How does the archiving job read the table? What does the legacy 65 GB consist of?

**Files and platform**

- Where does Databricks DQM fit, if at all? If platform enrichment is used, what allocation rule makes cost attribution honest?

**Parked (not live in this POC; revisit when a concrete need appears)**

- An Event Hubs-specific freshness expectation.
- End offsets for per-partition lag (Spark progress vs broker metrics).
- Authoritative mapping of legacy upstream names to query ids.
- Meaning of the file functional date and handling of file overwrites.
- Producing a file inventory for missing-file detection.

---

## Conclusion (provisional, 2026-10-23)

The model is settled at four complementary grains: raw input evidence, one flat fact per batch, one hourly row per job and source for time-window trends, and observed batch dependency edges. Monitoring reads persisted, ready-to-use statistics instead of recomputing them on each refresh; it needs means, bounds and coverage with sufficient statistics for weighted trends, not plant/source-grain facts or membership arrays. Message percentiles come from versioned histograms that merge exactly and interpolate approximately; one-day measurements support the version-1 bounds, without an accepted tolerance or cross-day evidence.

The POC is not yet a verified monitoring or lineage solution. Local compilation and focused tests narrow implementation uncertainty, but identity lifetime, replay, deployed storage, facts formation and lineage traversal are unverified. The first real freshness signal, one population well above the 2–5 min expectation, is observed but unexplained.

The stronger conclusion is about reasoning: establish grain, counted population, timestamp meaning and evidence coverage before composing ages, asserting lineage or diagnosing backlog. Stable keys make dependencies resolvable; convenient derived metrics do not supply missing population evidence.

## What changed in my understanding?

- Identity is several things with different lifetimes (unit, continuity, attempt, label). The hand-written name + date mixed two of them, and as `txnAppId` it is also a correctness risk.
- Spark's query-id lifetime is the intended basis for identity, unlike per-run `runId`; this internal local-property implementation still needs restart/reset/concurrent-query verification. Propagating it does not require changing every module's write idempotency.
- Start/end are batch facts, while input counts belong to a consuming-unit population. Repeating a batch count or summing repeated durations changes the answer. Detailed source/plant questions stay in raw drill-down.
- Stamping and recording immediate keys gives batch dependencies without an output list, not exact file/message attribution for every downstream output. Full producer means/bounds and scalar percentiles do not automatically describe a consumed subset.
- The initial two-table design was a response to a missing piece of data, not a demonstrated raw-storage need. Separate batch facts and dependencies answer different grains without demanding separate physical tables.
- An array of source/plant memberships cannot allocate batch-wide metrics. Keeping the final facts flat prevents accidental scope explosion from looking like attributed measurements.
- Convenient batch means and mergeable sums/counts are not competing designs. Build both, then use population-weighted sums for trends rather than averaging batch means.
- Histogram counts merge exactly, but interpolation assumes shape inside a bucket. An estimate below all observed values in its median bucket exposed that assumption; exact-size point masses are legitimate.
- Report absolute and relative percentile error together: a few percent can mean tens of seconds in a slow tail, while 10% can mean a fraction of a second. A bucket interval is uncertainty about location, not a confidence interval.
- Threshold shares and percentiles answer different questions. Boundary-aligned shares can be exact even when p99 interpolation is approximate; no alert policy follows merely from having 120/300 s bounds.
- Sampled distributions help choose a stable versioned contract, not prove future stability. Screening samples, exact one-day comparisons, cross-day validation and builder performance are separate evidence.
- Start age shows delay already present before processing; completion age better matches parsed-output timeliness. Daily completion quantiles need their own histogram, not an average-duration shift.
- Timestamp meaning belongs to the population. CreateTime is producer-supplied, not proof of publication or business-event time, and labeling `MIXED` after aggregation destroys the distinction. When every source shares one meaning, a per-source onboarding check is cheaper than a grain key, at the price of turning stored evidence into an assumption. A slow observed population does not justify relaxing the stated expectation.
- Joining run timelines, SCD2 jobs or billing slices can multiply correct facts. Prepare context first; repeated run usage is not additive per-batch cost.
- Working backwards from how the table is used ("what can't be recovered later?") removed most of the machinery.
- Recent processing can consume old data. Completion cadence, source timestamp age, source lateness and backlog need separate evidence and expectations.
- One atomic writer commit is not necessarily one complete reader micro-batch. Producer completion also need not precede consumer start.
- Storage annotations and encoders do not normalize incoming Spark expression types. The persisted schema and actual DataFrame types must agree.
- Backward ancestry can work without enumerating outputs, but exact per-target attribution and forward questions ("what's pending?") need additional evidence.
- Trend grain and question grain differ. Batch grain answers batch questions; time-window trends aggregate raw directly, because a non-mergeable statistic at an intermediate grain forces a return to raw anyway. *(Proposed wording; adjust.)*

## Connections

Potential connections, not evidence that the problem is already solved:

- `cases/productionizing-a-benchmark-driven-streaming-architecture.md`: control-tower tables as correctness/readiness coordination.
- `cases/databricks-cost-intelligence-loop.md`: `job_id`/`job_run_id` in telemetry enable cost per process or run; batch-level cost needs an explicit allocation rule.
- `cases/databricks-comment-metadata-ownership.md`: business owns freshness expectations; engineering owns the mechanism.
- `cases/streaming-file-parser-rework.md`: file size vs duration per batch would have surfaced large-file pressure earlier.
- `cases/repartitioning-large-delta-table.md`: retention and lifecycle operations bound long-lived assumptions; job recreation during a workspace move would split `job_id` continuity.
- `decisions/batch-telemetry-model.md`, `decisions/message-histogram-contract-v1.md`.

## Reusable lesson (provisional)

Choose the grain by the question: time windows for trends, batches for batch questions, edges for relationships, built from the finest evidence that still has what each statistic needs; retain raw detail for drill-down. Mergeable evidence is not automatically an exact percentile or a compatible population. Evaluate thresholds separately from interpolation, compare absolute and relative error, and capture completion with the same clock used in storage. Keep local statistics, ancestry, cadence and backlog separate; neither a dependency edge nor a passing local test proves more than its measured scope.
