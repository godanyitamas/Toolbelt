# Inbox

- [x] Retrospective: streaming parser rework — promoted to [case draft](cases/streaming-file-parser-rework.md).
- [ ] Concept candidate: explicit progress state / unit of work. Correctness came from making units of work and their completion explicit: control tower + synchro barrier (productionizing), per-date completion ledger (repartitioning), orphan retry state (late-arriving data), batch identity as `txnAppId` (observability).
- [ ] Concept candidate: physical layout follows the readers. Business partition columns (productionizing), explicit partition pruning (append-only CDC), partition-scoped migration unit (repartitioning), liquid clustering instead of identity partitions (observability).
