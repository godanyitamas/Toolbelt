---
type: concept
status: draft
created: 2026-10-25
topics:
  - coordination
  - spark-structured-streaming
  - delta-lake
  - late-arriving-data
  - observability
---

# Explicit units of work

> Agent draft from the cases below (repo review, 2026-10-25). The model sentence is mine from the inbox; the rest needs my review.

## The model

Correctness comes from making each unit of work, its identity and its completion explicit, recorded state, instead of relying on timing or on outputs appearing.

## Evidence from cases

- **`cases/productionizing-a-benchmark-driven-streaming-architecture.md`:** the legacy system got synchronization implicitly from one job flow. Splitting Silver into independent streams removed that, so per-file progress tables and a completion table became an explicit fan-out/fan-in barrier. The barrier is part of the correctness contract, not only observability.
- **`cases/repartitioning-large-delta-table.md`:** a long-running stream over the whole table failed. One `ingestion_date` → one bounded batch → one completion record gave restartability, rollback per date and visible progress.
- **`cases/late-arriving-data-with-bounded-retry-state.md`:** a fixed 24-hour delay was a timing assumption, not a completeness guarantee. Records that cannot join yet became explicit retry state with an expiry.
- **`cases/unified-data-observability-and-lineage.md`:** a hand-written name + date `txnAppId` mixed unit and continuity identity, which can skip or duplicate writes. Identity now comes from Spark (`query_id`, `batch_id`), producers stamp it on write and consumers record what they read.

## How to use it

On a new design, ask:

- What is the unit of work (file, date, batch, record)?
- What identifies it, and who supplies that identity? Not a person typing a name.
- Where is completion recorded, and who reads that record?
- What happens when a unit fails, reruns or arrives late? Is the waiting state bounded?
- Which implicit guarantee am I relying on instead: timing, a single flow, ordering, "the output exists"?

## Costs and limits

- Explicit state is more machinery to build, test and operate.
- A completion barrier changes the failure model: one failed participant can hold back every unit (productionizing).
- Waiting state needs a bound and an expiry, or it grows without limit (late-arriving).
- The identity may depend on platform internals; `sql.streaming.queryId` is an internal key (observability).

## Open questions

- When is implicit coordination in one flow cheaper and safe enough? The productionizing case leaves open whether the explicit coordination was worth its complexity.
- How should stuck units be detected: recorded but never completed, or retried until expiry?
