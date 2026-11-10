---
"effect-cqrs": minor
---

Wake view models in every process, manage the schema with migrations, and write concurrent appends in groups.

- **Appends from other processes.** `PostgresEventStore` sends a notification with the new head inside every append, and a store on a `PgClient` from `@effect/sql-pg` listens to it, so view models and subscribers in every process wake up on every append. `listen: false` turns listening off, for poolers such as PgBouncer in transaction mode. A new `pollInterval` option on both stores, and on `EventStore.make`, reads the head that often instead, for processes sharing a SQLite file or as a safety net.
- **Migrations.** `SqliteEventStore.migrations(options)` and `PostgresEventStore.migrations(options)` list the schema as ordered SQL migrations, for your own migration tooling. With `migrate: false`, `layer` and `layerCheckpoints` no longer create or alter tables on startup. By default they still do, and tables created by earlier versions keep working.
- **Group commit.** Appends that arrive while another is being written go together into one transaction. On PostgreSQL that is a single statement that checks each stream's version, numbers the events and inserts them, in one round trip. On a SQLite file the appends share a commit. An append whose stream moved on still fails alone with `ConcurrencyConflict`, and positions stay gap-free in commit order. In the benchmark, 256 concurrent workers on PostgreSQL went from 1,426 to 14,460 commands/s, and on a SQLite file from 8,279 to 15,784, while a single writer is no slower. `EventStore.groupCommit` gives the same to other backends.

`EventStore.make` now needs a `Scope`, which `Layer.effect` provides, and a `Backend` can announce appends from elsewhere through an optional `changes` stream.
