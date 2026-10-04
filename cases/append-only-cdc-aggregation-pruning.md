# Reducing work in an append-only CDC aggregation job

> Historical reconstruction based on recollection of the work and Spark UI observations. Workload scale and environment details are generalized; exact performance figures are not available here.

## Why revisit this?

This job brought together several techniques that made a large, complex aggregation flow more manageable: processing only newly inserted CDC records, pruning joined tables by the batch's partition values, limiting columns, using Photon, and applying broadcast joins where appropriate.

The experience is useful because it changed how I approach large joins: I now try to make input reduction explicit and verify it in the execution plan.

---

## What I remember

I needed to build a complicated Spark job that produced aggregated results in a golden layer from newly inserted records in a CDC-enabled silver Delta table. The source was maintained with merges, but the job's contract was append-only: it needed to process inserts. There were no deletes, and updates were outside the job's concern.

The job joined this batch with several very large tables before applying business rules and aggregations. The joins and aggregations were slow and memory-heavy, and some executions ran out of memory.

The participating tables shared partition columns because I had been involved in their partitioning strategy. I used the initial batch to build a predicate containing the distinct partition-value combinations, joined with `OR`, and applied it to the other table inputs. I also selected only the columns needed from each input.

I added improvements one at a time. Partition pruning helped substantially. Photon then made a large difference. I also configured automatic and adaptive broadcast-join thresholds. In the Spark UI, the same joins took less time when executed as broadcast joins.

After these changes, the flow became faster and more predictable, stopped running out of memory, and allowed me to downsize the cluster.

---

## What I believed at the time

I expected the join conditions to dynamically filter the large input tables. I also did not expect the aggregations to be as slow as they were.

---

## Records I checked

I recall using the Spark UI to inspect the execution plan and confirm that the explicit predicate pruned partitions.

I checked the predicate itself and tested row counts on a smaller, more manageable dataset to check whether rows were being filtered out incorrectly.

In the Spark UI, I compared the joins in the previous execution graph and saw that they took less time when broadcasted.

I do not have exact runtime, scanned-data, or memory figures in this reconstruction.

---

## What the records establish

- The execution plan showed partition pruning after I added the explicit predicate.
- On the smaller test dataset, I checked the predicate and row counts for unintended filtering.
- The same joins took less time when broadcasted, according to the Spark UI comparison I remember.
- I recall that the flow stopped running out of memory and that I could reduce the cluster size after introducing the optimizations.

The available recollection does not quantify each improvement or establish a precise end-to-end contribution for every change.

---

## What I think now

For this workload, I should not assume that join conditions alone will reduce the data read from every large input enough. Building a predicate from the batch's partition-value combinations made the intended input restriction explicit, and the Spark UI showed that partitions were pruned. Selecting only the required columns further limited the data carried through the flow.

Photon accelerated this workload substantially in my experience. Broadcast joins also reduced the time for the joins where they were applicable, as seen in the Spark UI. The sequential rollout made the effects easier to observe, though I do not have quantitative measurements to isolate their individual contributions.

Processing only inserted CDC records was appropriate because the job's output contract was append-only. That contract should be made explicit when designing the incremental read.

---

## What changed in my understanding?

I moved from expecting joins to dynamically narrow all participating tables to deliberately reducing each input before the expensive join and aggregation work. I also learned to inspect the execution plan for partition pruning and join strategy, and to consider the execution engine as part of performance work.

---

## Reusable lesson

For large incremental aggregation jobs, make the input scope explicit: define which CDC changes the job must process, prune each input using relevant batch partition values, and project only needed columns. Then inspect the execution plan and runtime behavior, consider Photon and broadcast joins where appropriate, validate filtering on representative data, and right-size the cluster after the flow is stable.

---

## Limits and open questions

- What were the exact before-and-after runtime, data-scan, memory, and cluster-size changes?
- How much improvement came from partition pruning, column selection, Photon, and broadcast joins individually?
- How representative was the smaller dataset used to check row counts?
- What specific measures were used to confirm output correctness beyond row counts?
