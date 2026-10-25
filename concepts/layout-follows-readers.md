---
type: concept
status: draft
created: 2026-10-25
topics:
  - partitioning
  - spark-performance
  - delta-lake
  - observability
  - metadata-governance
---

# Physical layout follows the readers

> Agent draft from the cases below (repo review, 2026-10-25). The model sentence is mine from the inbox; the rest needs my review.

## The model

Layout and access pattern form a contract. The layout is designed for the dominant readers (you can't please everyone equally), and every other reader, including new processes and ad hoc queries, has to take the layout into account to get the performance.

## Evidence from cases

- **`cases/productionizing-a-benchmark-driven-streaming-architecture.md`:** an ingestion-oriented partitioning was consistent with the pipeline but a poor fit for consumers. For the two largest tables, business partition columns were justified by observed access patterns and experiments; smaller tables kept the original scheme. The master table's file layout also shaped downstream streaming batch sizes.
- **`cases/repartitioning-large-delta-table.md`:** the new layout added a functional column next to `ingestion_date`, for the table's readers, among them the CDC job below.
- **`cases/append-only-cdc-aggregation-pruning.md`:** the upstream partitions were already suitable, but join conditions alone did not make the downstream job benefit. An explicit predicate built from the batch's partition values did, confirmed as pruning in the Spark UI.
- **`cases/unified-data-observability-and-lineage.md`:** partitioning by job and date was rejected (tiny, skewed partitions). Liquid clustering leads with `job_name` because people and reports know jobs by name, and `hourly_facts` is persisted because monitoring reads hourly.

## Making the layout known

Readers can only honour the contract if they know it:

- checking a table's keys in the Unity Catalog UI is common practice in the team and is becoming a convention;
- a unified job stamps the partition and clustering keys of the underlying tables into view comments (`cases/databricks-comment-metadata-ownership.md`).

## How to use it

When designing a table:

- Who are the dominant readers, and how do they filter?
- Which readers will not be served well, and what will they pay?

When reading a table:

- What are its partition and clustering keys?
- Does my query state a predicate on them directly? Joins alone may not prune; check the plan or the Spark UI.

## Costs and limits

- One layout cannot serve every reader equally.
- Changing a layout is expensive: the repartitioning case was a migration.
- Readers change over time, so the dominant readers have to be re-checked.
- The best layout for writing (ingestion order) can conflict with the best layout for reading.

## Open questions

- How can readers that ignore the layout be found, for example full scans in query history?
- What signal says the dominant readers have changed enough to justify a new layout?
