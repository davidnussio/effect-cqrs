---
"effect-cqrs": patch
---

Let a process start on a PostgreSQL store that other processes are writing to. The migrations a `PostgresEventStore` layer runs on startup took their locks on the events table in an order that deadlocked with appends in progress, so the layer failed with `EventStoreError("migrate")`, nearly every time beside a busy writer. They now take the table's lock first and wait for those appends. No event was lost or damaged.
