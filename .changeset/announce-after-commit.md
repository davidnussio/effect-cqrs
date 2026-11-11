---
"effect-cqrs": patch
---

Announce an append made in the caller's transaction when that transaction commits, on SQLite and PostgreSQL. It was announced as soon as it was written, so subscribers and view models of the same process looked before the commit, found nothing, and skipped the events for good: those of the transaction once it committed, or after a rollback the next ones to get the same positions. A custom `EventStore.Backend` gets the same with the new optional `afterCommit`, for which `SqlClient.afterCommit` fits.
