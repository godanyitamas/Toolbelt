# Reworking a File Parser Under Memory and Runtime Pressure

> Retrospective draft based on recollection. Workload and environment details have been generalized; verify the remaining questions before treating the explanation as final.

## Problem

A Spark Structured Streaming `foreachBatch` job parsed whole compressed files and produced several Delta outputs. Stages associated with larger inputs could run for hours, with high executor CPU, heap use, and garbage collection. The driver also showed high resource use. I needed to improve the parser while preserving its business rules and outputs.

## What I currently think

The old parser built a complete AST for each file, then converted its nested structure into nested `Row` and array structures before flattening them. That model likely caused high per-file memory use and substantial object allocation.

The old pipeline also likely evaluated parsing more than once for separate outputs. In the refactor, I cached datasets reused by multiple writes, so the parser was not invoked again for those uses.

My current explanation is therefore that both whole-file materialization and repeated parsing contributed to the old job's cost. This is retrospective reasoning, not a contemporaneous written diagnosis.

## Questions

- How much of the measured improvement came from the streaming parser versus avoiding repeated parsing through caching?
- What caused the high driver CPU, heap use, and GC, separately from executor-side parsing?
- Which additional malformed-input and boundary cases remain untested?

## Observations

- Spark UI stages associated with larger input files took hours; I cross-checked file sizes against the input folder's storage view.
- The struggling executor's heap histogram showed many strings, byte arrays, objects, and tuples consistent with parser intermediates.
- The old parser constructed a complete AST, then nested `Row`/array structures that were later flattened.
- The driver also showed unusually high CPU, memory, and GC activity. The executor histogram does not explain this by itself.
- Both implementations ran against the same dataset for two weeks on a production-equivalent cluster setup. A comparison notebook checked file and row counts and value-level matches across outputs.
- I created local test files for selected business rejection rules.
- Batch-duration metrics from the audit data covered all Delta writes. The reported speedup was roughly an order of magnitude on a representative workload and much larger on a large-file stress sample. The versions ran in parallel, so shared-cluster contention may affect the exact ratio.

## Hypotheses

### Whole-file materialization amplified memory pressure

A full AST and subsequent nested row structures likely increased peak live memory for large files. The number of temporary objects also likely increased allocation and GC work. The heap histogram supports parser-related object pressure, but does not measure allocation rate or object lifetime.

### Repeated evaluation multiplied parser work

The old parser's reusable outputs were not explicitly cached, and parsing was likely evaluated again for separate writes. In the new job, reused datasets were cached and the parser was not called again for those uses. This is a strong candidate for reduced total CPU and elapsed time, but its contribution has not been isolated from the parser rewrite.

### Driver pressure has a separate or additional cause

High driver resource use was observed, but the available executor heap evidence does not establish its cause.

## Investigation

- Compared Spark UI stage duration with input file sizes.
- Inspected the struggling executor's heap histogram and related the object types to the known parser structures.
- Reworked the parser from full-AST construction toward streaming, flatter output while retaining existing business rules.
- Cached datasets reused by multiple outputs so parsing was not repeated for those uses.
- Ran both implementations in parallel over the same dataset for two weeks; compared output counts and values.
- Tested selected rejection rules with manually created local fixtures.
- Compared end-to-end batch durations, including all output writes, using existing audit metrics on a production-equivalent cluster configuration.

## Conclusion

The evidence strongly supports that the refactored job was faster end to end on the workloads measured and matched the old outputs for the observed two-week dataset and selected rejection-rule tests.

The most plausible explanation is a combination of reduced whole-file intermediate materialization and avoiding repeated parsing for reused outputs. The available comparison does not isolate the contribution of each change. The driver-side resource pressure also remains unexplained.

## What changed in my understanding?

I initially described the old parser as poorly written. A more useful current explanation is that its whole-file AST and nested-row materialization, together with likely repeated parsing, did not fit the workload's file sizes and resource profile. The replacement changed both the parsing model and reuse of parsed results.

## Reusable lesson

When large inputs cause resource pressure, inspect both per-input materialization and whether multiple downstream actions recompute expensive parsing. Validate semantic equivalence and measure end-to-end batch time, while keeping driver and executor symptoms distinct.

## Open questions

- What driver-side operation or data flow caused its CPU, heap, and GC pressure?
- Can the parser-only contribution be separated from the caching/reuse change using available stage or batch metrics?
- Which additional malformed-input, boundary, or retry cases should be tested?
