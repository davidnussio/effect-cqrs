# effect-cqrs

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
