# effect-cqrs

This is a trial port of an old project,
[moleculer-cqrs](https://github.com/davidnussio/moleculer-cqrs), to
[Effect](https://effect.website) v4. The original is a CQRS and event sourcing module
for [Moleculer](https://moleculer.services), written in 2019. This repository
rebuilds the same ideas on Effect to find out how well they carry over.

It's an experiment, not a maintained library: it isn't published, the API may change,
and [Limitations](#limitations) lists what the port leaves out.

Commands decide on events, events are the only source of truth, and read models
are projections you can throw away and rebuild.

## Getting started

```bash
pnpm install
pnpm test
pnpm example
```

Requires Node 24 or later. The example needs `node:sqlite` and runs TypeScript
directly.

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
errors, and may run more than once, so it should only decide.

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

Writers use optimistic concurrency: a command that loses a race reloads the state
and decides again, up to `maxRetries` times (5 by default), then fails with
`ConcurrencyConflict`.

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

Tables are created on startup. Use durable checkpoints only with a durable view
model: an in-memory view model paired with `Checkpoints.layerMemory` rebuilds itself
on every start, which is usually what you want.

Another database needs an `EventStore.Backend`: `append`, `readBatch` and `head`.
Positions must follow commit order, so a backend on a database with concurrent
writers has to guarantee that itself.

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

- **Single process.** New events wake subscribers in the same process only. There
  is no transport equivalent to the Moleculer broker, so `dispatch` is the entry
  point to wire into your own HTTP or RPC layer.
- **SQLite and memory only.** There are no adapters for MongoDB, MySQL or PostgreSQL.
- **No code generator.** There is no equivalent of `cqrs generate`.
- **No snapshots.** State is rebuilt from every event of a stream on each command.
- **No schema migration.** A stored event that no longer decodes fails with
  `EventDecodeError`, so evolve event schemas compatibly.
- **Not published.** Packages expose TypeScript sources and there is no build step.
