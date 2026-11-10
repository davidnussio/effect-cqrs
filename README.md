# effect-cqrs

This is a trial port of an old project,
[moleculer-cqrs](https://github.com/davidnussio/moleculer-cqrs), to
[Effect](https://effect.website) v4. The original is a CQRS and event sourcing module
for [Moleculer](https://moleculer.services), written in 2019. This repository
rebuilds the same ideas on Effect to find out how well they carry over.

It's an experiment, not a maintained library: the API may change, and
[Limitations](#limitations) lists what the port leaves out.

The documentation, with a walkthrough of the bank demo, is at
https://davidnussio.github.io/effect-cqrs/. For coding agents, the package ships
[LLMS.md](LLMS.md), a short guide to using it, and the documentation is also published as
[llms.txt](https://davidnussio.github.io/effect-cqrs/llms.txt).

Commands decide on events, events are the only source of truth, and read models
are projections you can throw away and rebuild.

## Install

```bash
pnpm add effect-cqrs effect
```

It needs Node 24 or later and Effect 4. To store events in SQLite or PostgreSQL, add
that database's `effect/sql` driver too, `@effect/sql-sqlite-node` or `@effect/sql-pg`.

## Develop

```bash
pnpm install
pnpm test
pnpm example
```

The example needs `node:sqlite` and runs TypeScript directly. `pnpm build` compiles
`src` into `dist`, which is what npm gets. [CONTRIBUTING.md](CONTRIBUTING.md) has the rest
of the setup, and [SECURITY.md](SECURITY.md) says how to report a vulnerability.

`pnpm ui` serves an interactive demo at http://localhost:5173, also published at
https://davidnussio.github.io/effect-cqrs/demo/. It runs the framework
in the browser on the in-memory store: a bank whose accounts are aggregates, three
view models, and a saga for transfers. Its devtools slow commands and projections
down, pause and step the command bus, crash a projection, scrub through the log and
replay view models up to a chosen event.

## Define an aggregate

An aggregate is a schema of events, a projection folding them into state, and
commands deciding on new events.

```ts
import { DateTime, Effect, Schema } from "effect"
import { Aggregate } from "effect-cqrs"

const TodoEvent = Schema.TaggedUnion({
  TodoCreated: { title: Schema.NonEmptyString, createdAt: Schema.DateTimeUtc },
  TodoDeleted: { deletedAt: Schema.DateTimeUtc }
})

class TodoAlreadyDeleted extends Schema.TaggedError<TodoAlreadyDeleted>()("TodoAlreadyDeleted", {}) {}

type TodoState = { readonly title: string; readonly deleted: boolean }

const Todo = Aggregate.make<TodoState>()({
  name: "todo",
  events: TodoEvent,
  initial: { title: "", deleted: false },
  projection: {
    TodoCreated: (state, { title }) => ({ ...state, title }),
    TodoDeleted: (state) => ({ ...state, deleted: true })
  }
})
  .command("createTodo", {
    payload: Schema.Struct({ title: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(_, { title }) {
      return TodoEvent.cases.TodoCreated.make({ title, createdAt: yield* DateTime.now })
    })
  })
  .command("deleteTodo", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state) {
      if (state.deleted) return yield* new TodoAlreadyDeleted()
      return TodoEvent.cases.TodoDeleted.make({ deletedAt: yield* DateTime.now })
    })
  })
```

The projection must cover every event or it doesn't compile. A handler returns one
event, several, or `[]` to record nothing. It can use services, fail with typed
errors, and may run more than once, so it should only decide. Projections and
handlers must not mutate the state, which is kept between commands.

When an event's schema changes, the stored events keep their old shape. The aggregate
declares the older shapes and how each becomes the next, and they are upcast as they
are read:

```ts
const Todo = Aggregate.make<TodoState>()({
  // ...
  versions: {
    TodoCreated: EventVersions.from(TodoCreatedV1).next(TodoEvent.cases.TodoCreated, (v1) => ({ ...v1, dueDate: null }))
  }
})
```

## Run commands

```ts
import { AggregateService, EventStore } from "effect-cqrs"

const program = Effect.gen(function*() {
  const todos = yield* AggregateService.make(Todo)

  // Typed call
  yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })

  // Untyped JSON from a transport, validated before running
  yield* todos.dispatch({ aggregateId: "todo-2", type: "createTodo", payload: { title: "Buy Eggs" } })

  yield* todos.readModel("todo-1")
  yield* todos.readModel("todo-1", { finishTime }) // state as it was before finishTime
  yield* todos.history("todo-1", { payload: true })
}).pipe(Effect.provide(EventStore.layerMemory))
```

Commands on one stream take turns inside a service, so they can't race. A handler must
not run a command on its own stream, which would wait for itself. Across processes
writers use optimistic concurrency: a command that loses a race backs off, reloads the
state and decides again, up to `maxRetries` times (5 by default), then fails with
`ConcurrencyConflict`.

The service remembers the state of recent aggregates (`stateCacheSize`, 10,000 by
default), so a command only reads the events it hasn't seen. With
`snapshots: { every, schema, version }` it also saves states through a `KeyValueStore`
from `effect/persistence`, so a restart replays only what came after the last one.

Every event records a `correlationId` and a `causationId`. A command a view model runs
gets the handled event as its cause, so a chain of reactions can be traced back to the
request that started it. `Causation.withCausation` sets the ids from outside.

Commands can also carry metadata, a JSON object for what the domain doesn't decide on,
such as the software version or the user who sent them. It is recorded on every event they
write. `Metadata.layer({ appVersion })` sets it for the whole application and
`Metadata.withMetadata` adds to it per request.

## Project into view models

A view model reads the event store from its own checkpoint, so it catches up after
downtime and can't miss an event. Delivery is at least once, so handlers should be
idempotent.

```ts
import { Checkpoints, ViewModel } from "effect-cqrs"

const TodoList = ViewModel.make({
  name: "todo-list",
  aggregates: [Todo],
  dispose: clearList, // runs before a replay
  handlers: {
    TodoCreated: ({ title }, { aggregateId }) => addToList(aggregateId, title),
    TodoDeleted: (_, { aggregateId }) => removeFromList(aggregateId)
  }
})

const program = Effect.gen(function*() {
  const todos = yield* AggregateService.make(Todo)
  const viewModels = yield* ViewModel.ViewModels

  const { position } = yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
  yield* viewModels.caughtUp(position) // read your own write

  yield* viewModels.replay({ viewModels: ["todo-list"] })
  yield* viewModels.status
}).pipe(
  Effect.provide(ViewModel.layer([TodoList]).pipe(
    Layer.provideMerge(Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory))
  ))
)
```

A failing handler stalls its view model on that event and retries with backoff, so
events are never skipped or reordered. `viewModels.status` exposes the failure.

Anything else that reacts to events can use `store.subscribe(filter)`, a stream of
past events followed by live ones.

## Test an aggregate

`Fixture` runs commands through the real service on an in-memory store, so payload
validation and event encoding are tested too. It reads like `CQRSFixture`, and the
chain runs when you yield it or hand it to `it.effect`:

```ts
import { assert, it } from "@effect/vitest"
import { Fixture } from "effect-cqrs"

it.effect("creates a todo", () =>
  Fixture.make(Todo)
    .givenEvents()
    .when("createTodo", { title: "Buy Milk" })
    .expectEvent(TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: DateTime.makeUnsafe(0) }))
    .inspectState((state) => assert.strictEqual(state.title, "Buy Milk")))

it.effect("runs commands one after another", () =>
  Fixture.make(Todo)
    .givenEvents([created])
    .when("deleteTodo", {})
    .expectEvent("TodoDeleted")
    .when("createTodo", { title: "Buy Eggs" })
    .expectEvents(["TodoCreated"]))

it.effect("rejects deleting twice", () =>
  Effect.gen(function*() {
    const error = yield* Fixture.make(Todo)
      .givenEvents([created, deleted])
      .whenFails("deleteTodo", {})

    assert.strictEqual(error._tag, "TodoAlreadyDeleted")
  }))
```

- `when` runs a command by name, typed against the aggregate. Commands that fail
  stop the chain with their typed error.
- `expectEvent` expects exactly one event and `expectEvents` an exact, ordered list,
  so `expectEvents([])` asserts that nothing was recorded. Each entry is a whole
  event, compared with the aggregate's schema, or only its tag.
- `inspectState` takes a callback that throws to fail the test, so any assertion
  library works.
- Yielding the chain gives `{ events, state, version, fixture }` for the last
  command, if you prefer plain assertions.
- `givenEvents` also takes events in an older schema version, built with
  `Aggregate.historical(Todo, "TodoCreated", 1, { ... })`, to test upcasting.
- `checkInvariants({ invariant })` runs random command sequences generated from the
  payload schemas and reports the shortest one that breaks the predicate.

`it.effect` runs on a test clock stopped at zero, so timestamps need no mocking: an
event created by `DateTime.now` has `DateTime.makeUnsafe(0)` until
`TestClock.adjust` moves time.

## Storage

`EventStore.layerMemory` keeps events in memory. For SQLite, provide any
`effect/sql` SQLite client:

```ts
import { SqliteClient } from "@effect/sql-sqlite-node"
import { SqliteEventStore } from "effect-cqrs"

const Storage = Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
  Layer.provide(SqliteClient.layer({ filename: "./data/events.sqlite" }))
)
```

Tables are created on startup, unless `migrate: false` leaves that to your own tooling
with the SQL of `migrations()`. Use durable checkpoints only with a durable view
model: an in-memory view model paired with `Checkpoints.layerMemory` rebuilds itself
on every start, which is usually what you want.

For PostgreSQL, provide an `effect/sql` PostgreSQL client:

```ts
import { PgClient } from "@effect/sql-pg"
import { PostgresEventStore } from "effect-cqrs"

const Storage = Layer.merge(PostgresEventStore.layer(), PostgresEventStore.layerCheckpoints()).pipe(
  Layer.provide(PgClient.layer({ url: Redacted.make("postgres://localhost:5432/cqrs") }))
)
```

Writers wait for each other at one step of each commit, which keeps positions in commit
order, so concurrent appends are written in groups that share a commit. Several processes
can write to one database, and each append notifies the others, so view models everywhere
wake up. After a bulk load, run `ANALYZE`, or reads for view models are slow until
autovacuum gets to the table.

To run the tests on Postgres, `docker compose up -d --wait` starts one, and setting
`POSTGRES_URL=postgres://postgres:postgres@localhost:5432/cqrs` includes it in
`pnpm test`.

Another database needs an `EventStore.Backend`: `append`, `readBatch` and `head`.
Positions must follow commit order, so a backend on a database with concurrent
writers has to guarantee that itself.

## Performance

On a SQLite file a command takes about 0.1 ms, however long its stream is and however
many events the store holds, and a million events replay in under 5 seconds. On Postgres
a command takes about 1 ms. The first use of a stream after a restart replays it, at
about 3 µs per event. See the [performance and stress report](bench/REPORT.md) for the
numbers and the remaining limits.

`pnpm bench` measures each operation and `pnpm stress` loads the framework while
checking that nothing is lost.

## Coming from moleculer-cqrs

| moleculer-cqrs | effect-cqrs |
| --- | --- |
| `CQRSEventSourcing({ aggregate })` mixin | `AggregateService.make(aggregate)` |
| `call todo.command` | `todos.commands.x(id, payload)` or `todos.dispatch(json)` |
| `call todo.read-model` | `todos.readModel(id, { finishTime })` |
| `call todo.history` | `todos.history(id, { payload, startTime, finishTime })` |
| `call todo.replay` | `viewModels.replay({ viewModels, startTime, finishTime })` |
| view model service with `events` | `ViewModel.make({ handlers, dispose })` |
| `new CQRSFixture(aggregate)` | `Fixture.make(aggregate)` |
| `.givenEvents([...])` | `.givenEvents([...])` |
| `.when(createNews, payload)` | `.when("createNews", payload)` |
| `.expectEvent(event)` | `.expectEvent(event)` or `.expectEvent("Tag")` |
| `.inspectState(cb)` | `.inspectState(cb)` |
| `.whenThrow(command).toThrow(...)` | `.whenFails(command, payload)`, which returns the error |
| `fastest-validator` | `Schema` |
| `throw` in a command | typed errors |
| `resolve-storage-*` adapters | `EventStore.Backend` |

The largest change is on the read side. moleculer-cqrs broadcasts events over the
broker and only replay recovers one that was lost; here view models pull from the
store, so `replay` is for rebuilding, not for recovery.

## Limitations

- **No broker.** New events wake subscribers in every process, through PostgreSQL
  notifications or by polling, but there is no transport equivalent to the Moleculer
  broker, so `dispatch` is the entry point to wire into your own HTTP or RPC layer.
- **SQLite, PostgreSQL and memory only.** There are no adapters for MongoDB or MySQL.
- **No code generator.** There is no equivalent of `cqrs generate`.
- **Snapshots are opt-in.** Without `snapshots`, recent state is only kept in memory, so
  after a restart each stream is replayed the first time it is used. A changed state
  needs a new snapshot `version` by hand.
- **Event upcasts are pure.** Older event shapes declared with `EventVersions` are
  upcast on read, but an upcast can't read a service or fail with its own error.
