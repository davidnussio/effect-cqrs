# effect-cqrs

## 0.3.0

### Minor Changes

- 5043469: Rotate the master key, manage saga triggers, and use events defined as classes.

  - **Master key rotation.** `PersonalData.rewrap()` wraps every subject key that an earlier master key wrapped with the current one, a page at a time, so the earlier key can be dropped from `previous`. Keys don't change, so no event is rewritten. `layerMasterKeyConfig` reads earlier keys from `PERSONAL_DATA_MASTER_KEY_PREVIOUS`, as `id:base64` pairs separated by commas. `Keys` has a new `rewrap`, and `PersonalData.Backend` the optional `wrappedByOthers` and `replaceWrapped` it needs. A previous master key under the id of the current one now fails, rather than wrapping new keys with the old one.
  - **Saga triggers.** `Saga.layer` provides `Saga.Sagas`, which has the `status`, `parked` and `retryParked` of `ViewModel.ViewModels` for the triggers. A trigger with `onFailure: ViewModel.park(...)` parked events that nothing could retry.
  - **Events as classes.** A `Schema.Union` of `Schema.TaggedClass` events works throughout: personal fields in them are encrypted, `EventVersions` upcasts into the class, and `Outbox` and `EventCatalog` list them, where they skipped them before.
  - **Personal fields that would be stored in plaintext.** A field transformed after `PersonalData.field`, as in `field(Schema.String, ...).pipe(Schema.decodeTo(...))`, kept its mark where it wasn't found and was written unencrypted. Building the aggregate now fails, and so does a field whose `subject` names a sibling that isn't always a string.
  - **Commands.** `If-Match: *` asks `CommandApi` for no version in particular rather than failing with `400`. A typed command or `CommandGateway.send` with an `expectedVersion` that isn't a non-negative integer, or an empty `commandId`, fails with `InvalidCommand` instead of dying. `PersonalData.forget` tries a failing store a few more times, and says to call it again when it still fails.

- 21db979: Add `CommandGateway`, one entry point for the commands of several aggregates, like Axon's `CommandGateway`. `CommandGateway.make([Todo, Account])` builds it over aggregates, or over services built with `AggregateService.make` to choose their options or share them. `send` takes a typed `{ aggregateName, aggregateId, type, payload }` command, with `commandId` and `expectedVersion`, and its errors are those of the command's handler. `dispatch` takes JSON envelopes and routes them by `aggregateName`, or by `type` when a single aggregate has a command of that name, and fails with `UnroutableCommand` when it can't tell where one goes. Interceptors, declared as `CommandGateway.Interceptor<E, R>`, run around every command to authorize it, log it or add metadata, and their errors and requirements are added to the commands' types. `gateway.services` gives each aggregate's service for reading.
- 0ed8ffd: Refuse a `commandId` that another command already ran with on the aggregate. The command's name is recorded beside its id, under the reserved metadata key `cqrs.command` (`AggregateService.commandTypeKey`), and a different command under a known id fails with `CommandIdReused` instead of returning the first command's result. Through `CommandApi`, a reused `Idempotency-Key` is answered with `422`. `CommandFailure` includes the new error.
- 9d9dec9: Commands can be retried safely. A typed command takes an optional third argument, `{ commandId, expectedVersion }`, and the `dispatch` envelope accepts both fields too.

  With a `commandId`, a command that comes again on the same aggregate returns its first result, read back from the events it wrote, instead of running again. The id is recorded in the events' metadata under `cqrs.commandId`, and each aggregate remembers its last 100 ids, set with the new `commandIdsPerStream` option. Replays rebuild them and snapshots keep them. Commands that write no events aren't remembered.

  With an `expectedVersion`, a command fails with the new `WrongExpectedVersion` error, carrying the expected and the actual version, when the aggregate has moved on since the caller read it. It isn't retried. `CommandFailure` now includes it.

  The new `Ids` module holds `Ids.IdGenerator`, the reference that makes new correlation ids, so tests can make them deterministic with `Ids.sequential()`.

- 0052202: Read `history` in pages. It takes `afterVersion` and `limit`, and lists events without decoding them unless `payload` is set, so an event that no longer decodes doesn't fail a listing that doesn't show it. The HTTP endpoint of `CommandApi` takes the same two query parameters and answers with at most `historyLimit` events, 1,000 unless `CommandApi.layer` is given another.
- 305c116: Add modules that connect aggregates to the rest of a system, described in the new integration guide.

  `CommandApi` serves an aggregate over HTTP with `effect/http-api`. `CommandApi.group` builds an `HttpApiGroup` with a `POST` endpoint per command, taking the command's payload as body, and `GET` endpoints for `history` and, given a schema of the state, `readModel`. `CommandApi.layer` implements it with an aggregate service. An invalid payload is answered with `400`, a lost race with `409`, and a handler's error with `422`: as it is when listed in `errors`, otherwise as `CommandRejected` with its tag and message. OpenAPI comes from the group.

  `EventCatalog` gives the JSON Schema of every event, in every version declared with `EventVersions`, and of every command payload, and lists them in an AsyncAPI 3.0 document. `captureSamples` keeps a few stored payloads of each event type and version, with personal fields replaced by their redacted value, `saveSamples` adds them to a JSON file, and `checkCompatibility` fails a test when one no longer decodes with the current aggregate.

  `Outbox` publishes events to brokers such as Kafka, NATS or SQS. An outbox is a view model, so it publishes at least once from its checkpoint, in batches, retrying a failed batch, and never moves the checkpoint past a message that wasn't published. Events are mapped to integration events by tag, and each message has the event's id, which stays the same, for consumers to deduplicate.

  `examples/server`, run with `pnpm example:server`, serves the todo aggregate over HTTP with OpenAPI documentation, a view model, SQLite or PostgreSQL, and OpenTelemetry export when an endpoint is configured. The integration guide also shows tenant isolation with Effect's `LayerMap`.

- 04ee024: `PersonalDataNode.layerCipher`, from `effect-cqrs/PersonalDataNode`, encrypts personal fields with `node:crypto` instead of WebCrypto. It is synchronous, which roughly halves what reading a personal field costs, and writes the same format, so data written with either cipher reads with the other. The cipher is a `PersonalData.Cipher`, WebCrypto unless another is provided, and keys are now `CipherKey`s rather than WebCrypto `CryptoKey`s.

  `PersonalData.layerSql` and `makeSqlBackend` take `migrate: false` to leave the key table to your own migrations, with its migrations from `PersonalData.migrations({ table })`, in the same shape as the stores' `migrations`.

  The key cache now uses Effect's `Cache`. Concurrent requests for a subject that isn't cached share one read of the key store and one call to the master key, and writers racing for a new subject create a single key, where each used to wrap a key of its own, a paid call with a KMS.

  Reading checks that an encrypted field belongs to the subject it would be encrypted for, so a field copied into another stream fails to decrypt instead of reading as someone else's data. Keys must be 32 bytes, and errors no longer carry a malformed master key from configuration or the text of a field that isn't JSON.

- 5b822f1: Add `PersonalData`, crypto-shredding for personal data in an append-only log. `PersonalData.field` marks a field of an event as personal. `AggregateService` encrypts it with a key that belongs to its subject: the stream's aggregate id, or a sibling property the field names. `PersonalData.forget` destroys that key. The field then reads as the replacement it declares, and the event's other fields are left as they are. Forgetting is recorded in the log, refuses new personal data for the subject and destroys sealed snapshots that hold it.

  Data keys are stored wrapped by a `MasterKey`, which can be backed by a KMS or by a key from configuration. They are kept in memory with `layerMemory`, or in a SQLite or PostgreSQL table with `layerSql`. View models erase what they stored about a forgotten subject in the new `onForget` handler. The bank demo encrypts owners' names and has a **forget** button on each account.

  Aggregates with personal fields need `PersonalData.Keys`. `makeEventDecoder` and `makeEventEncoder` take the keys as an optional argument, and `ReadError` and `CommandFailure` now include `PersonalDataError` and `SubjectForgotten`.

- 179c9c8: Wake view models in every process, manage the schema with migrations, and write concurrent appends in groups.

  - **Appends from other processes.** `PostgresEventStore` sends a notification with the new head inside every append, and a store on a `PgClient` from `@effect/sql-pg` listens to it, so view models and subscribers in every process wake up on every append. `listen: false` turns listening off, for poolers such as PgBouncer in transaction mode. A new `pollInterval` option on both stores, and on `EventStore.make`, reads the head that often instead, for processes sharing a SQLite file or as a safety net.
  - **Migrations.** `SqliteEventStore.migrations(options)` and `PostgresEventStore.migrations(options)` list the schema as ordered SQL migrations, for your own migration tooling. With `migrate: false`, `layer` and `layerCheckpoints` no longer create or alter tables on startup. By default they still do, and tables created by earlier versions keep working.
  - **Group commit.** Appends that arrive while another is being written go together into one transaction. On PostgreSQL that is a single statement that checks each stream's version, numbers the events and inserts them, in one round trip. On a SQLite file the appends share a commit. An append whose stream moved on still fails alone with `ConcurrencyConflict`, and positions stay gap-free in commit order. In the benchmark, 256 concurrent workers on PostgreSQL went from 1,426 to 14,460 commands/s, and on a SQLite file from 8,279 to 15,784, while a single writer is no slower. `EventStore.groupCommit` gives the same to other backends.

  `EventStore.make` now needs a `Scope`, which `Layer.effect` provides, and a `Backend` can announce appends from elsewhere through an optional `changes` stream.

- de59ad7: View models can park events that keep failing, commit their checkpoints with their writes, and run in one process out of several.

  `onFailure: ViewModel.park({ attempts })` gives up on an event after it failed that many times: the view model records it in the new `ParkedEvents` service and goes on with the next one. `viewModels.parked()` lists parked events with their error and attempts, `viewModels.retryParked({ viewModel })` handles them again, `status` counts them and the `cqrs_view_model_parked_events_total` metric counts every event parked. `ParkedEvents.layerSql()` keeps them in a SQLite or PostgreSQL table, created on startup unless `migrate` is `false`, with `ParkedEvents.migrations()` for its migrations. Without a policy a view model still stalls on a failing event, as before.

  `transaction: ViewModel.sqlTransaction` runs each batch of handlers in one transaction with the checkpoint it reaches, which is then saved after every batch. A failed batch leaves neither its writes nor its checkpoint, and a crash repeats nothing.

  `ViewModel.layer` takes a lease on each view model from the new `Leases` service before running it. Leases are always held by default. `Leases.layerPostgres()` holds them as PostgreSQL advisory locks, so replicas sharing a database run each view model once, and a standby takes over when the holder stops or dies. `status` shows `holdsLease`, and `caughtUp` doesn't wait for view models another process runs.

  `ViewModels` has two new methods and `Status` two new fields, so code that implements `ViewModels` itself has to add them.

### Patch Changes

- 5043469: Fix what a command run in the caller's transaction leaves behind when it rolls back, and a store that stalls when a replica starts.

  - **Remembered state.** `AggregateService` remembered the state a command wrote inside the caller's `sql.withTransaction` before it committed. After a rollback the state had events the store didn't, and every later command on that stream failed with `ConcurrencyConflict` until the process restarted. It now remembers and snapshots through the new `EventStore.afterCommit`, once the transaction commits.
  - **Subject keys.** A personal data key created, or read, inside such a transaction stayed cached after a rollback, so later events were encrypted with a key the store never kept and could not be read after a restart. Keys read or created in a transaction are cached once it commits. Custom key stores can say so with the optional `inTransaction` and `afterCommit` of `PersonalData.Backend`.
  - **PostgreSQL startup.** Every start took an `ACCESS EXCLUSIVE` lock on the events and checkpoints tables, even with nothing to migrate. Behind a long transaction, such as a dump or a report, it held up every append and read of the other replicas. A store now checks the catalog first and leaves a current schema alone.
  - **Parked events.** A `retryParked` that failed while the view model was failing on another event could make that event park one attempt late, after its handler ran once more.

- a65c05d: Announce an append made in the caller's transaction when that transaction commits, on SQLite and PostgreSQL. It was announced as soon as it was written, so subscribers and view models of the same process looked before the commit, found nothing, and skipped the events for good: those of the transaction once it committed, or after a rollback the next ones to get the same positions. A custom `EventStore.Backend` gets the same with the new optional `afterCommit`, for which `SqlClient.afterCommit` fits.
- 552579b: Run command handlers, view model handlers and outbox publishers under the caller's span and log annotations. A service built inside a span put every span of its handlers, and of the appends they made, under that span instead of under the request.
- 5ef4bfd: Let replicas start together on one PostgreSQL database. `PersonalData.layerSql` and `ParkedEvents.layerSql` created their tables with no lock, so two processes starting at once could both run `CREATE TABLE IF NOT EXISTS` and one of them fail on a unique violation in `pg_type`. They now take an advisory lock named after the table first, as the event store already did.
- 18df34f: Ship `LLMS.md`, a short guide for coding agents that use effect-cqrs: the modules, the patterns to follow, common mistakes, and how to test with `Fixture`, sagas and personal data. Point an agent at `node_modules/effect-cqrs/LLMS.md`. The documentation site now also serves `llms.txt`, `llms-full.txt` and `llms-small.txt`, each page as Markdown at its URL with `.md`, and an API reference generated from the TSDoc comments.
- ddd1edb: Fence view model checkpoints. `Checkpoints` has an optional `advance(viewModel, from, to)`, which moves a checkpoint only if it is still at `from`, implemented by the memory, SQLite and PostgreSQL stores. A view model moves its checkpoint on from where it last saved it, so while a lease changes hands and two processes run it, a view model with a `transaction` rolls a batch back rather than committing it a second time, and neither process moves the checkpoint back. Custom stores without `advance` keep the previous behaviour. A process that finds its checkpoint moved reads it again and goes on from there.
- f32646c: Make the memory store honour `afterVersion` on a read of one stream, as the SQL stores do. Running a command again with its `commandId` on the memory store returned the events of every command before it in the stream as well as its own.
- 4a9783c: Reject personal fields inside a struct whose encoded form differs from its decoded one, such as a struct piped through `Schema.encodeKeys`. The fields were looked up under their decoded keys in the encoded payload, missed, and stored in plaintext. Building the aggregate service now fails with an error that names the struct.
- 74070b7: Encrypt personal fields marked around a nullable schema, such as `PersonalData.field(Schema.NullOr(Schema.String), { redacted: null })`. They went unnoticed and were stored in plaintext. Fields marked inside the union, `Schema.NullOr(PersonalData.field(...))`, were already found.
- 5c16e92: Document that an append to the PostgreSQL store inside a transaction of your own holds the counter row until that transaction ends, so every other writer waits for it.
- 5fcca5f: Let a process start on a PostgreSQL store that other processes are writing to. The migrations a `PostgresEventStore` layer runs on startup took their locks on the events table in an order that deadlocked with appends in progress, so the layer failed with `EventStoreError("migrate")`, nearly every time beside a busy writer. They now take the table's lock first and wait for those appends. No event was lost or damaged.
- 119abee: The tutorial's saga now waits for a payment, for 15 seconds at most. After reserving the stock it asks for the payment, then races a `DurableDeferred`, which the trigger completes on `OrderPaid`, against a 15-second `DurableClock`. A paid order is confirmed. An unpaid one is rejected and its stock released, unless the payment landed just before the deadline. The shop demo has a Pay button with a countdown and shows the new steps in its Saga tab.
- 15c226f: Keep view model batches to one page of the store under load. A view model slower than the appends read its whole backlog as one batch, which with a `transaction` became one transaction and was handled again in full after a failure. It now takes the latest head of the store each time it catches up, and reads a backlog in pages. A view model that waits to retry a failing event no longer queues a notification for every append made meanwhile, and parking an event clears `Status.lastError`.
- 181d012: Don't write an append whose caller was interrupted while it waited for a group commit on SQLite or PostgreSQL. A command that timed out in that queue failed for its caller and was written anyway. An append interrupted once its group is being written may still be written, as the documentation of `groupCommit` now says.

## 0.2.0

### Minor Changes

- b388f8e: Record metrics in `AggregateService` through Effect's `Metric` module, so remembered states and snapshots can be watched in production.

  - Each series carries an `aggregate` attribute, and `AggregateService.MetricNames` lists them: where each load found its state (`memory`, `snapshot` or `replay`), how many events it read on top, how many states are in memory and how many were dropped, snapshots saved and failed, lost races, and how long commands take by `command` and `outcome`.
  - Export them with `OtlpMetrics` or `PrometheusMetrics` from `effect/observability`, or read them with `Metric.snapshot`. On the in-memory store, with no I/O to hide it, they cost about 5% of command throughput.
  - The bank demo keeps four accounts in memory, saves a snapshot every five events and shows the metrics in a new tab.

- d4406aa: Record metadata with commands: a JSON object for what the domain doesn't decide on, such as the software version, the user who sent a command or the address it came from.

  - **Recorded on every event.** `StoredEvent`, `EventMeta` and `HistoryEntry` have a new `metadata` field, `{}` when a command carried none, so projections, view models and `history` can read it.
  - **Set from the context.** `Metadata.layer({ appVersion })` provides it to the services built on top, which remember it, and `Metadata.withMetadata` adds to it where a command runs. A `dispatch` envelope can carry `metadata` too, which wins key by key.
  - **Not inherited by reactions.** Commands a view model or saga runs record their own metadata, not the one of the event they react to: the `causationId` leads back to it.

  Existing SQLite and PostgreSQL tables get a `metadata` column on startup, filled with `{}` for older events. `StoredEvent` and `EventMeta` have a new field, so code that builds them by hand needs updating.

- 734f97b: Evolve event schemas, snapshot long aggregates, and trace why events happened.

  - **Event upcasting.** `EventVersions.from(V1).next(V2, upcast)` declares the older shapes of an event in the aggregate's new `versions` option. Old events are upcast on read through a decoder composed once with `Schema.decodeTo`, so projections and view models only see the current shape. The store records each event's `schemaVersion`.
  - **Snapshots.** `AggregateService.make(aggregate, { snapshots: { every, schema, version } })` saves state through a `KeyValueStore` from `effect/persistence`, so a cold start replays only the events after the last snapshot.
  - **Causation.** Every event records a `correlationId` and a `causationId`. View model handlers make the event they handle the cause of the commands they run, `Causation.causedBy` does the same for other subscribers, and `Causation.withCausation` and the `dispatch` envelope set them from outside.
  - **Fixture.** `givenEvents` accepts events in older schema versions, made with `Aggregate.historical`, and `checkInvariants` runs random command sequences with Effect's `Arbitrary` and reports the shortest one that breaks a predicate.

  Existing SQLite and PostgreSQL tables get the new `schema_version`, `correlation_id` and `causation_id` columns on startup. `StoredEvent` has these three new fields, and `EventMeta` has the two causation ones, so code that builds them by hand needs updating.

- 9658ec3: Run sagas as durable workflows from `effect/workflow`.

  - `Saga.workflow` defines the process and `Saga.toLayer` registers its body. Commands the body runs record the event that started the saga as their cause.
  - `Saga.command` runs a command as an `Activity`: its outcome is kept, so a replayed workflow skips it. Domain errors listed in `error` are kept too, lost races and store errors are retried, and anything else fails the workflow and runs its compensations.
  - `Saga.make` is the trigger, a view model whose handlers call `Saga.start`, and `Saga.layer` runs triggers apart from the view models. The workflow's idempotency key turns a second start of the same saga into a join.

### Patch Changes

- 4ce84c1: Save snapshots in the background and remember the state a command leaves.

  - **Snapshots no longer hold up commands.** `AggregateService.make` with `snapshots` saves them in a fiber of its own and needs a `Scope`, which `Layer.effect` provides. Closing the scope waits for the saves still running. A save that fails is retried after `every` more events instead of on every command.
  - **The next command reads nothing it already knows.** After a write the service folds the events it just stored into the state it remembers, decoding them as a replay would, so the next command on that aggregate only reads what others wrote since.

- f2dedbc: Learn the framework from a tutorial and two demos that show its traces.

  - **Tutorial.** A new page builds a small shop step by step: events, aggregates and commands, a view model, a saga, metadata, event versioning and metrics. Its code runs as `pnpm tutorial` and is tested.
  - **Shop demo.** The tutorial's shop runs in the browser with devtools for every Effect span, each saga's steps and execution, the stored rows and the metrics.
  - **Traces in the bank demo.** Its new Trace tab shows each command on the bus, the aggregate's spans and the transfer saga's workflow and activities.
