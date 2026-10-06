# Replacing a Fixed Batch Delay with Bounded Retry State for Late-Arriving Data

## Problem

A data workflow needed to combine records from a primary source with data from another table that could arrive later.

The original implementation was a Spark batch-style job that intentionally processed approximately 24-hour-old source data. The delay was intended to give the dependent table enough time to arrive before transformations, joins, and aggregations were performed.

This created two problems:

1. The 24-hour delay reduced freshness even when the dependent data was already available.
2. The dependent table could arrive more than 24 hours late, so the delay did not guarantee complete joins. Records could still be missing from the result.

The workflow therefore paid a freshness cost without providing a reliable completeness guarantee.

## What I currently think

At the time, the fixed delay was a reasonable way to accommodate an upstream timing dependency: wait long enough for the delayed table, then process the source data.

In retrospect, the important assumption was that the dependency would become available within the chosen delay window. That assumption was not a reliable guarantee.

The better model is to process source data as it arrives and represent records that cannot yet be joined as explicit retryable state.

## Questions

- How can source records be processed promptly without losing records whose join dependency has not arrived yet?
- How long should unresolved records remain retryable?
- How can repeated retries remain efficient as the staging state changes?
- What should happen when the dependency never arrives?

## Observations

- The original workflow processed approximately 24-hour-old data.
- It performed business transformations, joins, and aggregations in the batch job.
- The dependent table could arrive later than 24 hours.
- Consequently, the original workflow could still produce missing data despite the intentional delay.
- The original batch processing was comparatively heavy and did not provide fresh output.
- The redesigned workflow uses Structured Streaming on the source table.
- Records that cannot currently be joined are written to a staging table as orphan records.
- Each subsequent batch unions the previously staged orphan records with the newly arriving source records and attempts the join again.
- The retry period is parameterized. In the described design, it can be configured to three days.
- When a staged record eventually finds its counterpart, it is emitted by the job and removed from the staging state.
- Records that remain unresolved until the configured age limit are discarded, as required by the business rule.
- The staging table is maintained and optimized for its access pattern, including Z-Ordering on the merge keys because records need to be inserted when first encountered and removed when they become joinable.
- The streaming query uses checkpointing for persisted progress and recovery state.

## Hypotheses

### Hypothesis 1

The fixed 24-hour delay is fundamentally insufficient when the dependency's arrival time is variable.

### Hypothesis 2

The main performance problem is not simply that the original workflow was batch-oriented, but that it repeatedly performed heavyweight processing while also waiting on an external data-availability assumption.

### Hypothesis 3

Making unresolved records explicit state allows freshness and completeness to be handled separately: available records can be processed immediately while unresolved records can be retried within a bounded business-defined window.

## Investigation

### Investigation 1 — Replace time-based waiting with explicit retry state

The workflow was redesigned around Structured Streaming on the source table rather than waiting for a fixed amount of time before processing.

Each micro-batch processes newly available source data. Records whose required counterpart is not yet available become orphan records in a staging table rather than being lost or postponed together with the entire source batch.

On later batches, the previously staged orphan records are combined with new source records and retried against the dependent table.

This changes the unit of waiting from the whole dataset to only the records that actually need to wait.

### Investigation 2 — Bound the retry window

The orphan-record retention period is parameterized.

For example, with a three-day setting, an unresolved record can remain in staging and participate in subsequent join attempts until it reaches three days of age.

This makes the late-arrival policy explicit rather than embedding an assumed arrival time into the batch schedule.

When the configured limit is reached without a successful join, the record is discarded in accordance with the business requirement.

### Investigation 3 — Maintain the staging state for repeated updates

The staging table is not a passive queue. It is repeatedly modified as records move from unresolved to resolved.

Records need to be added when they first appear without a matching dependency and deleted once they become joinable.

The table was therefore optimized for this workload, including Z-Ordering on the merge keys used to locate records during these operations.

### Investigation 4 — Process incrementally

The streaming design allows individual batches to be tuned for throughput and latency rather than forcing the entire workload through the heavier original batch pattern.

The available Structured Streaming controls also make the processing cadence and batch sizing explicit operational parameters.

Checkpointing provides persisted query progress and recovery state, which makes the incremental processing model more explicit than the original time-based batch schedule.

## Conclusion

The original 24-hour delay was not actually a guarantee of data completeness. It was a timing assumption about when a dependent dataset would arrive.

When that assumption failed, the system had the worst combination of properties: it processed stale data, yet could still miss records.

The redesigned approach separates freshness from late-arrival handling.

Source records are processed promptly through Structured Streaming. Records that cannot yet be joined become explicit, bounded retry state in a maintained staging table. They are retried until they successfully join or reach the configured retention limit. This allows the system to preserve freshness for records that are ready while limiting the amount of state retained for records waiting on a late dependency.

The key architectural improvement is therefore not merely "batch to streaming." It is replacing a fixed time-based completeness assumption with an explicit state-and-retry mechanism.

The evidence available here establishes the behavioral and architectural improvement, but does not include quantitative before/after measurements for runtime, cost, or exact freshness latency.

## What changed in my understanding?

Before, a fixed delay looked like a practical way to accommodate a slower upstream dependency.

I would now describe the problem differently:

> A fixed delay should not be treated as a completeness mechanism unless the dependency's arrival-time bound is actually guaranteed.

When arrival time is uncertain, the system should make unresolved work explicit and define how it is retried, maintained, and eventually expired.

I also think about freshness and completeness as separate concerns. A record waiting on an external dependency should not force otherwise-ready records to wait as well.

## Reusable lesson

When a downstream computation depends on data that may arrive late, do not make the entire pipeline wait for an assumed arrival time. Process what is ready, persist what is not, and give the unresolved state an explicit retry and expiration policy.

## Open questions

- What quantitative improvement in latency, runtime, or compute cost did the redesign achieve compared with the original workflow?
- What exact Structured Streaming checkpoint/source/sink semantics were relied upon, and what processing guarantee did they provide in this implementation?
- How was the three-day retry window selected beyond the stated business requirement?
