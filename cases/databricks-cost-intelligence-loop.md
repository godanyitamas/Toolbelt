# Building a workload-aware Databricks cost intelligence loop

## Problem

After several years working on the same Databricks project, I had encountered enough expensive, inefficient, or otherwise problematic jobs to recognize recurring weaknesses in our workload estate.

The immediate problem was not that we lacked metrics. We had Databricks system tables and our own operational metrics, but with more than 100 jobs, it was not realistic to manually inspect them for cost and performance opportunities.

I wanted to make those opportunities discoverable in a way that matched how our workloads actually operate.

I initially considered producing a recurring weekly report. I changed direction because a report primarily tells people what happened, while the more valuable capability seemed to be identifying jobs and behaviors that deserve engineering attention.

I built a dashboard that combines:

- Databricks system-table information for jobs and cost;
- metadata retrieved from Databricks APIs, including job parameters and Spark configuration such as streaming trigger information;
- our own metrics Delta table, which is populated by many jobs;
- workload-specific derived metrics and signals.

The dashboard contains an overview for opportunity detection and a job-level view for investigation.

The overview currently surfaces signals such as:

- top spenders;
- week-over-week cost changes;
- underutilized clusters;
- continuously running Structured Streaming jobs using `trigger.processingTime` where the associated cluster appears not to be meaningfully utilized;
- jobs whose performance is deteriorating.

The job-level view brings together information needed to investigate an individual job, including cluster metrics, approximately 90 days of daily cost, production release dates, trigger configuration, and workload metrics such as median files per batch, batches per day, and cost per file.

The goal is not to report every available metric. It is to make potentially valuable cost/performance opportunities visible and easy to investigate.

---

## What I currently think

My current mental model is that cost optimization at this scale is partly an observability problem and partly a domain-knowledge problem.

Generic infrastructure metrics can tell me that a job is expensive or that a cluster is underutilized, but they cannot reliably tell me whether that behavior is actually problematic for a particular workload.

My previous experience with the project gave me knowledge of which jobs and behaviors were likely to contain opportunities and which apparent anomalies might be normal. I used that knowledge to decide which signals were worth exposing.

However, I also expected the system to uncover opportunities I was not already aware of. The purpose of encoding the signals into a dashboard was therefore to move from personal intuition toward a repeatable mechanism for discovering opportunities across a large job estate.

I also think the most useful metric is not necessarily an infrastructure metric. Normalized measures such as cost per file can make it easier to compare the economic efficiency of workloads.

Current hypothesis:

> A cost dashboard becomes useful when it connects project-specific workload behavior to economic signals and investigation context, rather than only presenting generic infrastructure statistics.

This is still an active case. I have evidence that the approach can identify valuable opportunities, but not yet that it has become a repeatable team practice.

---

## Questions

### Opportunity detection

- Which signals consistently identify real cost or performance opportunities?
- Which signals create noise or false positives?
- Which additional workload-specific signals are needed?
- How should opportunity thresholds be defined and maintained?

### Economic measurement

- How accurately can job cost be attributed to useful workload output?
- How should costs be normalized for different job shapes?
- How should savings be measured and attributed after an intervention?
- Which opportunities have enough potential impact to justify engineering effort?

### Investigation and causality

- What evidence is sufficient to treat a cost or performance change as an actual regression?
- How should release dates be used as investigation context without assuming causality?
- What information should be visible in the dashboard before a developer needs to leave it and investigate elsewhere?

### Operationalization

- How can a developer move from a detected opportunity to an owned investigation?
- How can cost optimization become part of the team's normal engineering flow rather than depending on one person?
- Which parts of the approach can be generalized across teams, and which must remain workload-specific?

---

## Observations

### Data and enrichment

The dashboard combines several sources because no single source contained enough information to answer the engineering questions I cared about.

Databricks system tables provide job and cost information.

Databricks API calls provide job metadata and configuration information that is not present in the system-table view I was using. I used this to expose parameters and Spark configuration, including streaming trigger information, and to connect the metadata to our own operational metrics.

Our metrics Delta table contains workload-level information written by many jobs.

I derived additional signals from these sources, including:

- cost over a historical period;
- week-over-week cost change;
- median files per batch;
- number of batches per day;
- cost per file;
- trigger configuration;
- cluster CPU and memory behavior.

For cost-per-file calculations, I took care with jobs containing multiple tasks so that process costs and file counts were attributed correctly and not double-counted.

The cost calculations are based on Databricks system-table information, including list-price information, following the calculation logic documented by Databricks for Azure. The resulting figures were also checked against actual cost reports available to us.

### Concrete opportunity: streaming job right-sizing

One Structured Streaming job used `trigger.processingTime` on a comparatively large cluster.

Based on my understanding of the workload, the incoming file volume and the underlying processing logic did not justify the cluster size.

The dashboard exposed the relevant information together:

- trigger type;
- historical cost;
- cluster metrics;
- daily workload volume;
- files processed per batch;
- batches per day;
- cost per file.

The workload was processing only a few files per batch. The combination of the workload size, the code path, and the cluster utilization strongly suggested substantial overprovisioning.

I reduced the cluster size by a large amount while retaining a healthy operating margin.

I validated the result over several days. I checked:

- driver memory;
- executor memory;
- CPU utilization;
- unexpected resource spikes;
- GC behavior;
- batch completion time.

The smaller cluster maintained healthy resource margins and batch completion time remained stable.

This was a relatively low-effort change with a large apparent economic impact.

The approximate cost moved from about $3,000/month to about $300/month.

### Concrete opportunity: parser rewrite

A separate workload was running an allocation-heavy parser on a large cluster and consuming substantial memory and CPU.

I rewrote the parser, reducing the allocation and resource pressure. The resulting job was lighter and faster and could run with substantially lower resource requirements.

The approximate cost moved from about $3,000/month to about $300/month.

The detailed technical investigation is documented separately in the parser rework case; this case is concerned with how cost/performance observability helped identify and prioritize the opportunity.

### Economic outcome

Across these two examples, the approximate recurring cost reduction was about $5,000/month.

The figures are based on system-table cost calculations and were checked against actual cost reporting. The exact sustained run-rate and attribution should be treated as an open measurement item until the before/after periods are documented more rigorously.

### Response from leadership

I showed the capability to higher-level stakeholders in the project.

The response was not only interest in the dashboard itself. I was asked to support another developer in creating a similar cost-monitoring dashboard for other teams.

That other team does not have the same control-tower metrics table and has different workload characteristics.

My main recommendation was therefore not to copy the existing dashboard mechanically. I advised the developer to work with someone who has deep knowledge of that team's jobs and processes so that the tool can expose the behaviors that are genuinely problematic for their workloads.

I also emphasized that the goal should be to build something that is useful and actually used, rather than completing a dashboard simply because it was requested.

---

## Hypotheses

### Hypothesis 1: Domain knowledge is necessary for useful cost signals

A generic cost dashboard can expose high-level spending and resource metrics, but it may miss workload-specific indicators that distinguish meaningful opportunities from normal behavior.

### Hypothesis 2: Opportunity detection is more valuable than periodic reporting

A weekly report can summarize spending changes, but a system that highlights specific candidates and provides investigation context is more directly connected to engineering action.

### Hypothesis 3: Normalized workload metrics improve prioritization

Metrics such as cost per file can reveal economic inefficiency that is difficult to understand from absolute job cost alone.

### Hypothesis 4: A reusable method should preserve workload-specific knowledge

The approach may be transferable across teams, but the actual signals and normalization metrics should be derived with people who understand the target workloads rather than copied unchanged.

---

## Investigation

### 1. Build a richer workload metadata layer

The first challenge was that the existing system-table information did not expose everything needed for the questions I wanted to ask.

I therefore combined system-table data with Databricks API-derived metadata and our own metrics Delta table.

The API-derived information allowed me to expose job parameters and Spark configuration such as the streaming trigger, and to establish the keys needed to combine the metadata with workload-level metrics.

This created a project-specific observability layer rather than relying only on generic Databricks reporting.

### 2. Encode project knowledge into signals

I selected signals based partly on recurring problems I had already encountered while working on the project.

This included identifying workloads where:

- clusters appeared substantially oversized;
- continuously running streaming jobs had unexpectedly low utilization;
- costs were increasing;
- performance was deteriorating;
- normalized workload cost appeared unusually high.

The important design decision was deciding what behavior was worth surfacing, not simply exposing more available metrics.

### 3. Add investigation context

The overview is intended to answer:

> Which jobs deserve attention?

The job-level view is intended to answer:

> Why might this job deserve attention?

Historical cost, cluster resource behavior, workload metrics, trigger configuration, and release dates are presented together so that a developer can move from detection into investigation.

### 4. Validate an infrastructure optimization

For the streaming workload, the evidence suggested that the cluster was substantially oversized.

I reduced the cluster and then monitored the workload over several days rather than assuming the change was safe.

The observed behavior remained healthy: CPU and memory retained margin, there were no unexpected resource spikes, GC behavior remained normal, and batch completion time stayed consistent.

### 5. Validate a deeper engineering opportunity

The parser workload demonstrated a different class of optimization.

The cost signal identified a large and expensive workload. The eventual solution required significant engineering work to reduce allocation and resource pressure rather than simply changing infrastructure sizing.

This is important because cost optimization is not limited to infrastructure right-sizing. It can also expose inefficient application or data-processing implementation that warrants deeper engineering work.

### 6. Start transferring the approach

The request to support another team created an early test of transferability.

Because that team has different workload characteristics and does not share the same control-tower table, I advised treating the existing dashboard as an example of the method rather than as a fixed implementation.

The current approach is to pair the engineer building the tool with someone who deeply understands that team's workloads and use that knowledge to decide which signals are worth exposing.

This transfer is still in its early stages, so adoption and effectiveness are not yet established.

---

## Conclusion

The strongest result of this work is not the dashboard itself.

It is evidence that accumulated project knowledge can be turned into a system that makes cost and performance opportunities discoverable across a workload estate that is too large to inspect manually.

The dashboard combines generic platform data with workload-specific telemetry and project knowledge. This combination allowed me to identify opportunities that would otherwise have been difficult to find systematically.

Two examples provided evidence of real economic value:

- a low-effort streaming cluster right-sizing change, reducing approximate monthly cost from $3,000 to $300;
- a deeper parser rewrite, also reducing approximate monthly cost from $3,000 to $300 while improving the workload itself.

Together these represent approximately $5,000/month of apparent recurring cost reduction, based on system-table calculations cross-checked with actual cost reports.

The work also generated an external signal of value: another team is now starting to build a similar capability, with my guidance.

However, the broader engineering problem remains open.

I have demonstrated that the method can produce useful findings and meaningful savings. I have not yet demonstrated that it can become a sustainable, team-owned engineering practice with repeatable adoption and prioritization.

---

## What changed in my understanding?

I initially thought about the problem as reporting: collect cost and performance metrics and produce a weekly view.

I now think the more valuable capability is opportunity detection.

The important step is not exposing every metric. It is identifying which combinations of workload behavior, resource usage, and cost indicate that an engineer should investigate something.

I also learned that domain knowledge is part of the observability design itself. The signals become useful because they encode knowledge about what the workloads are expected to do and what unusual behavior looks like.

Finally, I see that a successful cost tool should not merely answer “what did this job cost?” It should help answer “is this cost justified by the workload, and is there a credible engineering opportunity here?”

---

## Reusable lesson

Useful cost observability should connect:

**workload behavior → resource usage → economic impact → investigation → action → measured result**

Generic platform metrics are a foundation, not the complete solution.

The strongest signals come from combining platform data with knowledge of what the workloads are supposed to do.

---

## Open questions

- What is the exact sustained savings from the two optimizations when measured over comparable before/after periods?
- How many genuine opportunities does the dashboard surface over a representative period?
- Which signals have the highest ratio of useful findings to false positives?
- How should opportunity impact and implementation effort be combined when prioritizing work?
- What is the minimum investigation context needed for a developer to act without relying on the dashboard creator?
- How should opportunities be assigned and tracked once the dashboard identifies them?
- Can another team independently identify and validate a useful opportunity using the same approach?
- Which parts of the telemetry and signal model can be standardized across teams?
- Which parts must remain workload-specific?
- Can the dashboard become part of a recurring engineering review or another normal team workflow without becoming passive reporting?
