---
type: decision
status: active
created: 2026-10-23
topics:
  - observability
  - kafka
  - event-hubs
  - freshness
---

# Message histogram contract, version 1

Case: `cases/unified-data-observability-and-lineage.md`

## Context

Monitoring needs medians/percentiles of message age and value size per batch and over windows (day, week). Raw `batch_telemetry` holds one row per topic/partition per batch, so per-message values are gone after the builder runs. Scalar partition percentiles cannot be combined into batch or daily percentiles.

## Constraints

- Databricks Runtime 16 LTS: mergeable KLL sketch SQL functions are documented only from 18.0.
- Facts are built by Core; dashboards and notebooks outside the jars may read later.
- Raw prefers explicit columns and one meaning per column.
- The raw table is in test and redeployed with the jars.

## Options

| Option | Gives | Costs |
|---|---|---|
| A. Batch median repeated on each partition row | exact-per-batch statistic, no contract | not mergeable across batches; grain-mismatch pattern |
| B. Fixed-bucket histograms per partition row | exact merge to any window; exact shares at bounds; approximate percentiles | bounds become an immutable contract; precision = bucket width |
| C. Exact percentiles on demand from Bronze | exact, no contract | Bronze retention; scan cost per evaluation (unmeasured) |
| KLL sketches in raw | mergeable approximate quantiles | not available on 16 LTS |

## Decision

Option B, under test in the POC. A and C remain fallbacks; C is the ground truth for histogram error.

**Contract (`MessageHistograms`, Core):**

- `Version = 1` identifies **both** bound sets and their semantics. Published bounds never change in place; arrays from different versions never merge; old definitions must stay resolvable.
- Buckets: `(-inf, b0)`, `[b0, b1)`, …, `[last, +inf)`. A value equal to a bound belongs to the bucket starting at it. Negative ages stay in the first bucket.
- **Age bounds, seconds (29 bounds, 30 buckets):** `0, 0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 300, 450, 600, 750, 900, 1200, 1500, 1800, 3600, 7200, 21600, 86400`.
- **Value-size bounds, bytes (83 bounds, 84 buckets):** sorted distinct union of `CEIL(POWER(2.0, i / 4.0))` for `i = 0..80` (ceiling-rounded quarter-octave) and the refinements `0, 136, 144, 224, 232, 5632, 5760`. Literal list (recomputed from the formula for this record; verify against the Core object):
  `0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 23, 27, 32, 39, 46, 54, 64, 77, 91, 108, 128, 136, 144, 153, 182, 216, 224, 232, 256, 305, 363, 431, 512, 609, 725, 862, 1024, 1218, 1449, 1723, 2048, 2436, 2897, 3445, 4096, 4871, 5632, 5760, 5793, 6889, 8192, 9742, 11586, 13778, 16384, 19484, 23171, 27555, 32768, 38968, 46341, 55109, 65536, 77936, 92682, 110218, 131072, 155872, 185364, 220436, 262144, 311744, 370728, 440872, 524288, 623488, 741456, 881744, 1048576`
- **Raw columns:** `message_age_at_start_hist`, `message_age_at_completion_hist` (30 each), `message_value_bytes_hist` (84), `message_hist_version`. Null on non-message rows.
- **Populations:** both age arrays total `message_ts_count` (valid timestamps: non-null and `timestampType <> -1`; the `-1` filter is deferred in the current local code, with a proposed facts-build fallback); the size array totals `message_value_count` (non-null values; empty value = 0 bytes). Value bytes exclude key, headers and protocol overhead.
- **Completion age** uses the same post-write end instant that Core persists as `batch_end_ts`. It is a separate histogram, not a shifted start histogram.
- **Responsibilities:** Core owns the pure constants object, identity/timing, storage normalization and persistence. Modules calculate histograms and sufficient statistics. Core does not aggregate or validate supplied histograms.
- **No bounds lookup table yet.** Core-built facts read the object; test notebooks use version-labeled literals.
- **Timestamp type is not stored** and is not a grain key. Age meaning is assumed CreateTime for all current sources (one observed day), with a proposed onboarding check per new source.

## Why

- Only histograms give correct daily/weekly percentiles and exact threshold shares on 16 LTS without rescanning Bronze.
- One-day exact-reference comparisons on production data supported retaining these bounds: size p50/p95/p99 errors mostly under 3%, worst 7.41% on a sparse tail; age errors at most about 36 s on the slowest population, with no tested percentile in an open tail.
- Age bounds 120/300/600 s make the shares relevant to the 2–5 min expectation exact.
- The 5632/5760 refinements fixed a measured interpolation failure: uniform interpolation put a median below every observed value in its bucket. They are fitted to one day's point masses; if payloads change they become useless, not wrong.
- The timestamp type's only real benefit was interpreting age meaning, a per-source property; as a grain key it cost extra rows, a more complex continuity check and an unsolved flat-facts design.

## Consequences

- Downstream checks (facts build) must verify: array totals equal their counts; array length = bounds + 1 for the version; one version per merged group.
- Percentile reads interpolate inside a bucket and must flag results in an open tail as bounds, not values. Shares at bounds are exact.
- Any bounds change requires a version bump and resolvable old definitions.
- A silent switch of a source between create and log-append time would change age meaning undetected.
- The builder keeps the raw batch persisted across data writes to compute the completion histogram after the write: memory/disk cost per batch (unmeasured).
- The largest size bound is 1,048,576 bytes, close to the largest observed values; larger values land in the open tail.

## What would make me reconsider

- Second-day or weekly comparisons show unacceptable error, or the distributions shift (version 2).
- A dashboard or non-Core consumer must interpret buckets, or the bounds change: add a Core-managed lookup table (`version`, `metric`, `bounds`) and a shared SQL entry point for interpolation.
- A non-CreateTime source appears: reintroduce timestamp-type evidence (e.g. min/max validation columns) or separate age populations.
- Builder overhead (aggregation or the persisted batch) is unacceptable.
- A runtime upgrade to 18.0+ makes KLL sketches available.
