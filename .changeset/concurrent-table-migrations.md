---
"effect-cqrs": patch
---

Let replicas start together on one PostgreSQL database. `PersonalData.layerSql` and `ParkedEvents.layerSql` created their tables with no lock, so two processes starting at once could both run `CREATE TABLE IF NOT EXISTS` and one of them fail on a unique violation in `pg_type`. They now take an advisory lock named after the table first, as the event store already did.
