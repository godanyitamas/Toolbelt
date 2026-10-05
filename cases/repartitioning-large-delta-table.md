# Repartitioning a Large Delta Table During Migration

> Historical reconstruction based on memory. The work and environment details have been generalized; exact timings, file counts, and some operational details are no longer available.

## Why revisit this?

A migration required moving a very large Delta table to a new Unity Catalog workspace while also changing its partitioning and repairing historical data-quality issues. The experience is useful because my first design optimized for incremental, restartable processing with Structured Streaming, but the eventual design used explicit partition-scoped batch processing.

The main lesson is about choosing the migration unit of work from the shape of the data rather than assuming that streaming is the best mechanism.

---

## What I remember

The source Delta table was roughly 700 TB. It had four existing partition columns selected for business reasons. The reason those original columns were chosen is a separate design discussion and is intentionally out of scope here.

There was strong pressure to migrate the table and its jobs to a new Unity Catalog workspace. The migration plan involved reworking the process in the old workspace and then using a deep clone for the resulting table.

The process rework required a different physical partitioning scheme. The new table introduced `ingestion_date`, representing the day data was written, plus one functional partition column. Historical rows did not have `ingestion_date`, and some of the new partition-driving values were null or incorrect.

A related `global_records` table contained the correct values and was treated as the source of truth. Where the historical ingestion date could not be determined exactly, it had to be approximated, which introduced some reconstruction error.

I initially built a Spark Structured Streaming job that would:

- read the old table;
- join it with `global_records`;
- repair the historical data;
- write the corrected rows into a new partitioned Delta table.

The reasoning at the time was that the dataset was too large for a simple one-shot rewrite. Streaming offered incremental processing, checkpointing, restartability, and the possibility of continuing to process new records while the historical migration was running.

That approach became too slow. The source Delta table contained an extremely large number of files, and its Delta log was also large and fragmented. The join and recomputation became expensive enough that the migration progressed too slowly. During the run, a `VACUUM` operation removed underlying files that the long-running process still needed, causing the stream to fail.

In this first trial I deliberately did not enable `autoCompact` or `optimizeWrite`, because I was concerned about producing another highly fragmented target table while the migration was already struggling.

By this point the new `ingestion_date` partitioning was established, which exposed a better migration unit: one ingestion date at a time.

I switched to a simple batch process:

1. collect the distinct `ingestion_date` values from the source;
2. process one date at a time;
3. write the transformed data into the new partitioned table;
4. record completion in a separate progress table.

The progress table stored the `ingestion_date`, start time, and end time. This made execution progress explicit and also provided enough information to infer processing speed.

The batch process was designed to be restartable. If a date failed or produced corrupted output, I could remove that date's data from the new table, remove its completion record, and rerun that date. The job therefore resumed from the latest date that had not been recorded as complete.

For validation, I compared old and new data through row counts and value-to-value comparisons at a smaller scale. The transformation itself:

- inner-joined against `global_records`;
- removed duplicates;
- filled null values;
- took the ingestion date from the trusted source where available.

The source-of-truth logic and the duplicate-handling mechanism were tested and treated as correct for the migration.

---

## What I believed at the time

I initially treated the size of the table as the main reason to choose streaming. My reasoning was approximately:

> A 700 TB table cannot be rewritten as one huge operation, so I should turn the migration into an incremental stream that can checkpoint and restart.

That mental model correctly identified the need for bounded, restartable execution, but it conflated those properties with the streaming abstraction.

I also treated the ability to process continuously as a benefit because new records could accumulate while the historical repair was running.

---

## Records I checked

No contemporaneous project records were available for this retrospective beyond my recollection of the implementation and the resulting behavior.

### What the records establish

Nothing beyond the recalled implementation can be independently verified here. The technical sequence below should therefore be read as a historical reconstruction rather than a complete incident record.

---

## What I think now

The central design mistake was not choosing Structured Streaming for a large table per se. It was choosing continuous processing before identifying the natural bounded unit of work already present in the data.

Once `ingestion_date` was available, the table had a natural migration boundary:

`one ingestion_date -> one bounded batch -> one completion record`

That structure provided most of the operational properties I originally wanted from streaming:

- bounded work;
- restartability;
- explicit progress;
- failure isolation;
- deterministic retry;
- rollback at the unit-of-work level;
- partition-level validation.

The partition-scoped batch approach also reduced the amount of state each run had to reason about. Each date represented a bounded amount of source data, and the process could make progress independently of the full table.

The failed streaming approach exposed another important operational issue: a long-running computation over a mutable Delta table can become vulnerable to lifecycle operations such as file cleanup when its processing time is large enough. The exact file-retention mechanics are not fully reconstructable now, but the observed outcome was that the slow stream eventually lost data it still required.

The `global_records` join was a separate correctness mechanism. Its importance was that the migration did not simply move rows into a new physical layout; it also had to reconstruct and repair the values used by that layout. The physical repartitioning and historical data remediation therefore had to be treated as one transformation, even though the underlying concerns were distinct.

---

## What changed in my understanding?

I would now frame the problem differently.

Before:

> The table is enormous, therefore use streaming so the migration can run incrementally and restart.

Now:

> First identify the smallest deterministic unit of work that naturally maps to the target layout. Then choose the execution model that processes that unit safely and observably.

Streaming was not intrinsically wrong. It was a mismatch for this particular migration because the workload already had a useful partition-level boundary and the continuous query introduced a large, long-lived computation over a fragmented source.

I also changed my view of restartability. I had implicitly associated restartability with checkpointed streaming. The migration showed that restartability can instead be an explicit application-level property implemented with a completion ledger and deterministic partition-level retries.

---

## Reusable lesson

For large data migrations, do not equate scale with streaming. Look first for a natural bounded unit of work that aligns with the physical data layout. A deterministic batch process plus an explicit completion ledger can provide restartability, rollback, progress tracking, and observability without the complexity of a long-running stream.

---

## Limits and open questions

- The exact historical timing, throughput, file counts, and Delta-log size are no longer available.
- The precise causal relationship between the slow stream, Delta file lifecycle, and `VACUUM` cleanup is reconstructed from memory rather than contemporaneous logs.
- The amount of historical data for which `ingestion_date` had to be approximated, and the resulting error rate, is not preserved here.
- The contribution of each individual transformation to total migration cost was not isolated.
- The original rationale for the business partition columns belongs to a separate architectural discussion.
