# Inbox

- [x] Retrospective: streaming parser rework — promoted to [case draft](cases/streaming-file-parser-rework.md).
- [ ] Concept candidate: explicit progress state / unit of work. Correctness comes from making each unit of work, its identity and its completion explicit, recorded state, instead of relying on timing or on outputs appearing. Cases: progress tables + completion barrier (productionizing), per-date completion ledger (repartitioning), orphan retry state (late-arriving data), batch identity (`query_id`, `batch_id`) instead of a hand-written name + date `txnAppId` (observability). Ready for a concept note (repo review 2026-10-25).
- [ ] Concept candidate: physical layout follows the readers. Business partition columns (productionizing), explicit partition pruning (append-only CDC), partition-scoped migration unit (repartitioning), liquid clustering instead of identity partitions (observability).
