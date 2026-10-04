# Who owns the metadata? — Databricks comments for Genie

> Historical reconstruction based on memory and available implementation context. Details are generalized; remembered explanations should not be treated as verified fact.

## Why revisit this?

The business wanted to add comments to Databricks tables, views, and columns so that Genie could use additional business context when working with the data.

I initially approached this as a technical metadata-management problem: how could comments be defined within the existing framework that creates our Delta tables?

The solution eventually became a broader design around **ownership, source of truth, reconciliation, and operational safety**.

This is worth capturing because it changed how I think about platform metadata: the team that owns the code is not necessarily the team that should own the business meaning represented by that code.

---

## What I remember

The business wanted comments on Databricks objects to provide more context for Genie.

There were two business-maintained Excel files:

- **Column-level comments:** defined by column name. The intended rule was that columns with the same name should have the same comment.
- **Table/view-level descriptions:** defined by object name and the desired comment.

My task was to design a system to apply and maintain these comments.

### Initial approach

The existing engineering framework defines target Delta tables using Scala case classes. I introduced a custom `@comment` annotation that could be applied to those case classes.

The idea was roughly:

`case class → @comment annotation → common framework → Databricks metadata`

I also planned to check and maintain the comments on the underlying table **before the Structured Streaming query was set up**.

Most of our jobs use Spark Structured Streaming. My reasoning was that changing table metadata before starting the stream would avoid changing metadata while the streaming query was already running, reducing the chance that the stream would be affected by the metadata operation.

### Architectural challenge

The architect challenged the assumption that developers should maintain business information.

His concern was that many changes to this business information were expected, while every module was also expected to use the same version of the underlying core module containing my code.

This meant that embedding frequently changing business metadata into developer-owned code would couple business changes to application-code and shared-module changes.

The key question became:

> **If developers own the code, do they therefore own the business meaning encoded in the catalog?**

The answer was no.

### Final approach

The business became the owner of the desired comments.

They maintained the two Excel files in SharePoint, which provided a convenient interface for business users.

The files were then brought into Databricks for a dedicated synchronization job. The job used the Excel contents as the **source of truth** and compared them with the current comments in the Databricks catalog.

The current state was obtained from system/catalog tables. The job computed the delta between the desired comments and the current metadata and applied the required changes for table/view and column comments.

The conceptual flow became:

`Business → SharePoint Excel → Databricks → current catalog metadata → reconciliation → Databricks comments`

The solution reached production and became the standard way we manage these comments.

### Making the change operationally safer

While designing the solution, I also considered the possible effect of metadata changes on upstream and downstream jobs.

I created notebooks that use the lineage system table to generate a report showing which jobs could be affected if the comments job executed at that point.

This provides a **dry-run** mechanism: before applying the metadata changes, the impact can be inspected and affected jobs can be paused.

I initially took charge of applying the flow because a large number of existing tables needed to be updated. The longer-term goal is for every RTP to adopt the same process so that this becomes a normal platform flow rather than a task that depends on me personally.

---

## What I believed at the time

My initial mental model was strongly influenced by the existing technical abstraction.

The framework already knew how tables were defined, so it felt natural to attach the comments to those definitions through an annotation.

I was primarily asking:

> How can the framework apply comments consistently?

I was also thinking about the lifecycle of Structured Streaming and how to avoid changing metadata while a stream was active.

I did not initially frame the problem around ownership of the business information itself.

---

## Records I checked

- The existing common framework/module that defines target Delta tables.
- The implementation of the comment-management functionality.
- The Databricks system/catalog tables used to inspect current comments.
- The business-maintained Excel files in SharePoint.
- The lineage system table used for impact analysis.
- The production deployment of the final synchronization flow.

### What the records establish

The final implementation uses business-maintained Excel files as the desired state.

The synchronization process compares the desired state with current Databricks metadata and applies the difference.

The solution reached production and became the standard flow for managing the comments.

The lineage-based notebooks are used to assess which jobs could be affected before running the metadata-changing process.

---

## What I think now

The fundamental problem was not simply:

> How do we add comments to Databricks objects?

It was:

> **Who owns the meaning of the metadata, where is the desired state maintained, and how should engineering safely reconcile that state with the platform?**

The final architecture separates two responsibilities.

**Business ownership**

The business defines and maintains the desired business meaning.

**Engineering ownership**

The engineering system owns the mechanism that reconciles that desired state with the platform.

This separation matters because business definitions can change independently of application implementation. A business change should not inherently require developers to modify a Scala annotation, change a shared core module, or make every consuming module adopt a new version.

The central synchronization job also establishes a clearer source of truth.

Conceptually:

`desired state - current state = required metadata changes`

The lineage-based dry run adds another layer of engineering discipline.

Metadata changes should not automatically be treated as isolated side effects. When a platform change can affect other workloads, it is useful to understand the blast radius before executing it.

The impact report therefore acts as an operational control around the metadata reconciliation process.

---

## What changed in my understanding?

I initially optimized around the **existing developer abstraction**: table definitions lived in code, so metadata naturally seemed to belong there too.

I now see the ownership question as more fundamental:

> **The team that owns the code does not necessarily own the business meaning represented by the system.**

That led to a different architecture:

**Before**

`Developer → annotation → shared framework → metadata`

**After**

`Business → desired metadata → centralized reconciliation → platform metadata`

I also changed how I think about metadata changes operationally.

A metadata operation may look small because it does not modify the actual data, but it can still interact with running or downstream workloads. The lineage-based dry run makes the change visible before execution and allows affected jobs to be coordinated.

Finally, I learned something about technical ownership itself.

I initially owned the implementation because the problem needed someone to drive it. After the solution reached production, however, the next step is not to remain the person who manually performs the process. The stronger outcome is to make the process a generalized team capability that every RTP can follow.

---

## Reusable lesson

When designing a system that manages information, ask not only:

> **Where can we technically store it?**

Also ask:

> **Who owns it? Who changes it? What is the source of truth?**

Separate the ownership of the information from the mechanism that applies it.

For platform-level changes, also ask about the blast radius. A useful design may need both a reconciliation mechanism and a dry-run or impact-analysis mechanism before changes are executed.

---

## Limits and open questions

- The exact wording and full reasoning of the architect's original challenge is reconstructed from memory.
- I do not have a complete record of my original mental model beyond the implementation approach I remember.
- The exact behavior of every upstream/downstream workload when metadata changes is not reconstructed here; the lineage report is an impact-assessment mechanism rather than proof that every affected job would fail.
- The long-term adoption of this flow across every RTP is still a platform-adoption task rather than a completed organizational change.
- It may be worth revisiting the business-facing source of truth if the scale or governance requirements grow beyond the current Excel-based process.
