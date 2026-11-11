---
"effect-cqrs": patch
---

Fix what a command run in the caller's transaction leaves behind when it rolls back, and a store that stalls when a replica starts.

- **Remembered state.** `AggregateService` remembered the state a command wrote inside the caller's `sql.withTransaction` before it committed. After a rollback the state had events the store didn't, and every later command on that stream failed with `ConcurrencyConflict` until the process restarted. It now remembers and snapshots through the new `EventStore.afterCommit`, once the transaction commits.
- **Subject keys.** A personal data key created, or read, inside such a transaction stayed cached after a rollback, so later events were encrypted with a key the store never kept and could not be read after a restart. Keys read or created in a transaction are cached once it commits. Custom key stores can say so with the optional `inTransaction` and `afterCommit` of `PersonalData.Backend`.
- **PostgreSQL startup.** Every start took an `ACCESS EXCLUSIVE` lock on the events and checkpoints tables, even with nothing to migrate. Behind a long transaction, such as a dump or a report, it held up every append and read of the other replicas. A store now checks the catalog first and leaves a current schema alone.
- **Parked events.** A `retryParked` that failed while the view model was failing on another event could make that event park one attempt late, after its handler ran once more.
