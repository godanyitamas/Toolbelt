# Productionizing a Benchmark-Driven Streaming Architecture

> Historical reconstruction based on memory. The system and environment details are generalized; exact measurements and some original design rationale are not available as contemporaneous records.

## Why revisit this?

I participated in a POC for replacing a legacy STDF ingestion system, then later took ownership of productionizing and operating the redesigned architecture.

The POC demonstrated a major latency improvement on its test workload, but production exposed assumptions that the benchmark had not established. The resulting work involved implementing missing business logic, sizing and tuning multiple Databricks workloads, understanding cross-layer performance effects, managing failure and synchronization behavior, and changing physical table design where production access patterns justified it.

The experience is useful because it changed my understanding of what it means to productionize a distributed data architecture: the limiting factor is not necessarily one Spark job or one cluster setting, but the interaction between workload shape, execution boundaries, storage layout, coordination, and consumer behavior.

## What I remember

The legacy STDF system parsed zipped binary files and produced multiple Silver Delta tables. The Silver tables shared a four-column partitioning scheme defined by business requirements.

The new design introduced a Medallion-style architecture. The binary files were parsed once into a large Bronze Delta table. Bronze records were organized primarily around record type, with an ingestion_date column added as a consistent temporal access key. A Silver target could generally derive its data from one or two relevant Bronze record-type partitions.

The POC then introduced a Global Silver table and multiple downstream Silver Structured Streaming queries. Each Silver stream used Global records as an entry point and used information such as file name and ingestion date to locate the corresponding records in Bronze.

Bronze ingestion was split into three streams by file size: small (0–100 MB), medium (100–200 MB), and large (200 MB–2 GB). Small files represented roughly 97% of the daily file count. The intended benefits of the split included workload-specific tuning, lower latency for small files, resource specialization, and potentially lower cost.

The POC was built by Databricks-focused engineers against an initial end-to-end baseline of roughly 30 minutes. The POC demonstrated about four minutes for the small-file workload. However, that four-minute value was not a production end-to-end metric and was based on a limited test dataset. The POC did not yet contain all of the business logic required to reproduce the legacy outputs exactly.

A particularly important part of the architecture was the use of control-tower Delta tables. The Bronze control tower contains file-level metadata such as batch information, file name, start/end information, and job information, with one row per file. This provides a file-level coordination signal because reading the Bronze data stream alone does not guarantee that all rows belonging to a binary file have arrived.

The Silver layer had an analogous control tower. Each independent Silver stream recorded its completion for a file. An endpoint table, synchro, only received one row for a file after all required Silver processes had written their control-tower records. Synchro therefore acts as the completion contract consumed by downstream Golden processing: when a file appears there, the system treats the required Silver outputs for that file as complete.

The production deployment was two Databricks jobs: one Bronze job and one Silver job with multiple tasks corresponding to the Silver streaming queries. Tasks had automatic retries, but a failure that required code or configuration changes could leave one Silver process unavailable. Other Silver processes could continue, while synchro correctly withheld the file because the full set of acknowledgements had not arrived. This could in turn halt Golden downstream processing.

The legacy system had a useful contrast. It also produced Global records first and then produced Detail tables, but the outputs were created through the same overall job flow. This meant synchronization was largely implicit, although the parser was invoked twice.

After the POC, I took ownership of productionization. There was additional work to reproduce the exact legacy business semantics and outputs.

Production cluster sizing became a major issue. The Databricks engineers had tuned against approximately one day of data, which was not sufficiently representative of production. I had to run multiple waves of cluster tuning, using runtime metrics and Spark UI observations to identify bottlenecks, choose cluster configurations, and keep enough safety margin to avoid out-of-memory failures.

Bronze and Silver required different tuning strategies. Bronze parsing is constrained by the fact that a single binary file is effectively the unit of parallelism: one file cannot simply be divided across executors. Therefore executor memory and file size have a direct relationship to processing stability, especially for the large-file class.

Silver has a different resource profile. Its expensive DataFrame transformations, joins, and other operations made memory pressure and execution time more sensitive to query shape and intermediate data.

Another complexity was that Silver streaming queries consume the Global Delta table rather than the binary files directly. Streaming batch size therefore became indirectly related to the number and size of Parquet files in Global. When Global was fragmented into many small files, batch-size tuning had to compensate for the physical layout of the Delta table.

The output side had the same kind of coupling. Poorly sized or fragmented Silver writes could create many small output files, which affected query efficiency and Delta transaction-log health. I found that optimized writes and auto compaction were important not only for keeping Parquet files reasonably compact, but also for maintaining healthy table behavior at large scale.

At least one Silver table eventually reached roughly petabyte scale. This made physical file layout and Delta-log fragmentation operationally significant rather than theoretical.

The production system eventually reached roughly 6–7 minutes from the timestamp when a file landed in the input folder to the point where its row appeared in synchro for small files. Large files generally remained around 15–17 minutes, which was acceptable to the business.

I also investigated Silver partitioning. The original partitioning used ingestion_date, which was consistent with how the pipeline created and propagated the data. However, users and downstream workloads primarily filtered on business columns. After researching partitioning strategies and running smaller experiments and statistical checks, I added an additional business-driven partitioning column to the two largest Silver tables. The smaller tables remained on the existing approach because their query behavior was acceptable with ingestion_date.

This improved partition pruning for important user queries and for downstream jobs consuming those large tables.

My ownership also included defining production deadlines, managing risks, communicating with users and use cases, and resolving issues that arose during productionization.

## What I believed at the time

I do not have a reliable contemporaneous record of my complete mental model from the beginning of the project.

My current recollection is that I initially viewed the new architecture primarily as a strong performance redesign: parse once, exploit Databricks Structured Streaming, specialize workload classes, and use parallel Silver streams to replace the duplicated parsing of the legacy system.

The POC performance result made the architecture look highly promising.

Once I became responsible for the production system, I had to reason about a much larger problem than the POC benchmark had represented.

## Records I checked

- AGENTS.md and the casebook's retrospective workflow and template.
- Existing Toolbelt cases on parser performance, large Delta-table migration, Spark join/pruning optimization, late-arriving data, and Databricks cost intelligence.
- The current case material is primarily a reconstruction from my recollection rather than from contemporaneous project records.

### What the records establish

The repository records establish the framing of this retrospective and related engineering experiences, but they do not independently verify the historical measurements or the exact original POC rationale.

The production timings, file-size distribution, partitioning decisions, and system behavior described here should therefore be treated as recalled observations unless later evidence is recovered.

## What I think now

The strongest lesson is that the POC established architectural feasibility and a promising benchmark, but it did not establish production capacity or an end-to-end SLA.

The four-minute POC result and the eventual 6–7 minute production measurement should not be compared as though they were the same metric. The production measurement included the real file-arrival-to-synchro boundary and a substantially more complete system. The large-file range also behaved differently and was governed by a different workload profile.

The new architecture made an important improvement over the legacy system by avoiding duplicated binary parsing. However, it traded some implicit guarantees for explicit distributed coordination.

In the legacy system, synchronization between Silver outputs was largely an emergent property of one execution flow producing the related tables. In the new system, Silver streams were deliberately independent. Synchro therefore became an explicit fan-out/fan-in completion barrier.

This also changed the failure model. A single Silver process could be down while other processes remained healthy, but the system could not safely release the file to Golden until all required Silver acknowledgements existed. The control tower was therefore more than an observability mechanism; it was part of the correctness and readiness contract.

The production tuning work showed that performance was not a property of the Spark transformations alone. Logical batch boundaries were coupled to physical Parquet layout in Global, and Global layout in turn influenced Silver execution behavior. Similarly, Silver output layout influenced future reads and Delta-log health.

The workload specialization in Bronze also made more sense once the real file distribution and binary parsing constraint were considered. Small, medium, and large files were materially different resource regimes, and especially large files could not be made arbitrarily more parallel by splitting the binary input.

The partitioning work changed my view further. A partitioning strategy can be technically consistent with the ingestion pipeline and still be a poor fit for the workloads consuming the resulting tables. For the two largest tables, business-driven partitioning was justified by observed access patterns and experiments, while the original ingestion-oriented approach remained adequate for smaller tables.

The system ultimately reached a stable production range of approximately 6–7 minutes for small files and 15–17 minutes for large files, with the latter accepted by the business. The exact contribution of individual optimizations to the final timings is not isolated in the available evidence.

## What changed in my understanding?

I would now explain the experience very differently.

Before, I would have described the success primarily as improving pipeline speed through Databricks and cluster tuning.

Now I see the more important lesson as productionizing a distributed architecture is an exercise in discovering and validating the system's real invariants and workload characteristics.

I learned that a benchmark is only meaningful within its measurement boundary and workload. A one-day test can establish that a design works and can be fast without being sufficient for capacity planning.

I learned that decomposing a formerly coupled pipeline creates coordination responsibilities that may have been implicit before. When execution becomes distributed, completeness, readiness, and failure semantics need to be made explicit.

I also learned that physical Delta layout is not an implementation detail that can always be tuned independently from streaming behavior. Parquet file fragmentation, write behavior, compaction, and Delta-log health can feed back into how streaming workloads behave.

Finally, I moved from thinking about partitioning primarily as an ingestion design choice to treating it as a workload-dependent access-path decision. The best partitioning depends on who and what consumes the table.

My role also changed my understanding of ownership. I was not simply optimizing someone else's Spark jobs. I was responsible for the gap between a promising POC and a production system: semantics, capacity, failure modes, operational margins, deadlines, users, and the downstream consequences of design decisions.

## Reusable lesson

A production data architecture should be evaluated against the real workload, failure model, physical storage layout, and consumer access patterns—not only the benchmark that demonstrated the original design was feasible.

## Limits and open questions

- The exact POC dataset and benchmark setup are not preserved here.
- The original rationale for every architectural decision is not independently verifiable.
- The exact contribution of individual cluster and storage optimizations to the final latency is not isolated.
- The precise failure/recovery behavior of each Silver task under every type of incident is not reconstructed.
- The final partitioning experiments and query-performance measurements should be recovered if stronger quantitative evidence is needed.
- It remains an open architectural question whether all of the explicit coordination complexity introduced by the independent Silver streams was justified by the overall production benefits.