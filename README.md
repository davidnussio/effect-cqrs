# effect-cqrs

CQRS and event sourcing for [Effect](https://effect.website) v4, modelled on
[moleculer-cqrs](https://github.com/davidnussio/moleculer-cqrs).

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

`Fixture` runs commands through the real service on an in-memory store and returns
what happened.

```ts
import { assert, it } from "@effect/vitest"
import { Fixture } from "effect-cqrs"

it.effect("rejects deleting twice", () =>
  Effect.gen(function*() {
    const error = yield* Fixture.make(Todo)
      .given([created, deleted])
      .whenFails("deleteTodo", {})

    assert.strictEqual(error._tag, "TodoAlreadyDeleted")
  }))

it.effect("creates a todo", () =>
  Effect.gen(function*() {
    const { events, state, version } = yield* Fixture.make(Todo).when("createTodo", { title: "Buy Milk" })

    assert.strictEqual(version, 1)
  }))
```

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
| `CQRSFixture` | `Fixture.make(aggregate)` |
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
