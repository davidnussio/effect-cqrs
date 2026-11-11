# effect-cqrs for coding agents

effect-cqrs is CQRS and event sourcing for Effect 4. Commands decide on events, events are
the only source of truth, and read models are projections that can be thrown away and
rebuilt. This guide is for agents writing code that uses the package. The full
documentation is at https://davidnussio.github.io/effect-cqrs/, and as one text file at
https://davidnussio.github.io/effect-cqrs/llms-full.txt.

It is built on Effect 4 (`effect@^4`), whose APIs differ from Effect 3's. Read Effect's own
guide, `LLMS.md` in the Effect repository, before writing Effect code, and copy the patterns
below rather than ones remembered from Effect 3.

## Install

```bash
pnpm add effect-cqrs effect
pnpm add @effect/sql-sqlite-node # or @effect/sql-pg, to store events in a database
```

It needs Node 24 or later. Everything is exported from `effect-cqrs` as a namespace, and
each module also has its own path:

```ts
import { Aggregate, AggregateService, ViewModel } from "effect-cqrs"
import * as Fixture from "effect-cqrs/Fixture"
```

## Modules

| Module | What it is for |
| --- | --- |
| `Aggregate` | Define an aggregate: events, state, projection and commands |
| `AggregateService` | Run commands against the event store, read state and history |
| `CommandGateway` | One entry point for the commands of several aggregates, with interceptors |
| `EventVersions` | Declare older shapes of an event and upcast them on read |
| `ViewModel` | Read models that follow the log from a checkpoint, `ViewModels` to replay and wait for them |
| `Saga` | Processes that span several aggregates, as durable workflows from `effect/workflow` |
| `PersonalData` | Crypto-shredding: encrypt personal fields per subject, forget a subject |
| `EventStore` | The event store service, `layerMemory`, and the `Backend` interface for other databases |
| `SqliteEventStore`, `PostgresEventStore` | The store and checkpoints on an `effect/sql` client |
| `Checkpoints` | Where view models keep their position, `layerMemory` |
| `Causation` | `correlationId` and `causationId`, set from outside with `withCausation` |
| `Metadata` | A JSON object recorded on every event, such as the app version or the user |
| `Fixture` | Test an aggregate: given events, when a command, expect events |

## Define an aggregate

```ts
import { DateTime, Effect, Schema } from "effect"
import { Aggregate } from "effect-cqrs"

const TodoEvent = Schema.TaggedUnion({
  TodoCreated: { title: Schema.NonEmptyString, createdAt: Schema.DateTimeUtc },
  TodoDeleted: { deletedAt: Schema.DateTimeUtc }
})

class TodoAlreadyDeleted extends Schema.TaggedError<TodoAlreadyDeleted>()("TodoAlreadyDeleted", {}) {}

type TodoState = { readonly title: string; readonly deleted: boolean }

export const Todo = Aggregate.make<TodoState>()({
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

- Events are a `Schema.TaggedUnion`. Build them with `TodoEvent.cases.X.make({ ... })`,
  which fills in `_tag`. A `Schema.Union` of `Schema.TaggedClass` events works too.
- `Aggregate.make<State>()({ ... })` is curried and takes the state type explicitly, so
  literal fields such as `status: "open"` don't widen to `string`.
- The projection has one function per event tag, `(state, event, meta) => state`, and
  doesn't compile if a tag is missing.
- A handler gets `(state, payload, { aggregateId, version })` and returns an `Effect` of one
  event, an array of events, or `[]` to record nothing. Write it with `Effect.fnUntraced`,
  which infers the parameter types.
- Errors are `Schema.TaggedError` classes, failed with `return yield* new MyError(...)`.
  They reach the caller with their type.
- Handlers can use services. The aggregate's requirements become requirements of
  `AggregateService.make`.

## Run commands

```ts
import { Context, Effect, Layer } from "effect"
import { AggregateService, EventStore } from "effect-cqrs"

class Todos extends Context.Service<Todos, AggregateService.AggregateService<typeof Todo>>()("app/Todos") {
  static readonly layer = Layer.effect(Todos, AggregateService.make(Todo))
}

const program = Effect.gen(function*() {
  const todos = yield* Todos
  const { position } = yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
  // Untyped JSON from HTTP or RPC: the envelope and payload are validated first.
  yield* todos.dispatch({ aggregateId: "todo-2", type: "createTodo", payload: { title: "Buy Eggs" } })
  const state = yield* todos.readModel("todo-1")
  const events = yield* todos.history("todo-1", { payload: true })
}).pipe(Effect.provide(Todos.layer.pipe(Layer.provide(EventStore.layerMemory))))
```

- A command returns `{ aggregateName, aggregateId, events, version, position, correlationId }`.
  Pass `position` to `viewModels.caughtUp` to read your own write.
- Besides the handler's errors, a command can fail with `InvalidCommand`, `UnknownCommand`
  (`dispatch` only), `ConcurrencyConflict`, `EventStoreError` and `EventDecodeError`.
- `dispatch` is the entry point for transports. It decodes the payload with the command's
  JSON codec, so an ISO string becomes a `DateTime`.
- `AggregateService.make(aggregate, { maxRetries, stateCacheSize, snapshots })` tunes
  retries after lost races, the states kept in memory, and snapshots saved through a
  `KeyValueStore` from `effect/persistence`. With `snapshots` it also needs a `Scope`, which
  `Layer.effect` provides. Change `snapshots.version` whenever the state or the projection
  changes.
- `Causation.withCausation({ correlationId, causationId })` and
  `Metadata.withMetadata({ userId })` wrap a command to record those on its events.
  `Metadata.layer({ appVersion })` sets metadata for the whole application.
- `CommandGateway.make([Todo, Account], { interceptors })` routes commands to several
  aggregates: `gateway.send({ aggregateName, aggregateId, type, payload })` is typed, and
  `gateway.dispatch(json)` routes by `aggregateName`, or by `type` when a single aggregate has
  it, failing with `UnroutableCommand` otherwise. Declare each interceptor as
  `CommandGateway.Interceptor<E, R>` so its errors and requirements reach the commands' types.

## Change an event's schema

Stored events are never rewritten. Declare the older shapes and how each becomes the next:

```ts
import { EventVersions } from "effect-cqrs"

const TodoCreatedV1 = Schema.Struct({ title: Schema.String, createdAt: Schema.DateTimeUtc })

const Todo = Aggregate.make<TodoState>()({
  // name, events, initial, projection as before
  versions: {
    TodoCreated: EventVersions.from(TodoCreatedV1).next(TodoEvent.cases.TodoCreated, (v1) => ({ ...v1, dueDate: null }))
  }
})
```

Versions count from 1, the shape events had before the history was declared. The last
`next` must be the current schema, and each upcast returns the next version without `_tag`.
Upcasts are pure functions.

## View models

```ts
import { Checkpoints, EventStore, ViewModel } from "effect-cqrs"

const TodoList = ViewModel.make({
  name: "todo-list",
  aggregates: [Todo],
  dispose: clearList, // runs before a replay rebuilds the view model
  handlers: {
    TodoCreated: ({ title }, { aggregateId }) => addToList(aggregateId, title),
    TodoDeleted: (_, { aggregateId }) => removeFromList(aggregateId)
  }
})

const Live = Layer.mergeAll(Todos.layer, ViewModel.layer([TodoList])).pipe(
  Layer.provideMerge(Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory))
)

// const viewModels = yield* ViewModel.ViewModels
// yield* viewModels.caughtUp(position)
// yield* viewModels.replay({ viewModels: ["todo-list"] })
// yield* viewModels.status
```

- Delivery is at least once: after a crash a view model sees up to two batches of 500
  events again. Handlers must be idempotent.
- A failing handler stalls its view model on that event and retries with backoff. Events
  are never skipped. `viewModels.status` shows the failure; put a timeout on `caughtUp`
  where a stall matters.
- Checkpoints live as long as the view model: `Checkpoints.layerMemory` for one kept in
  memory, which rebuilds on every start, and `SqliteEventStore.layerCheckpoints()` or
  `PostgresEventStore.layerCheckpoints()` for one stored in that database.
- A handler that runs commands records the handled event as their cause. For a process
  that runs commands in reaction to events, write a saga instead.
- `store.subscribe(filter)` is a stream of past and live events for anything else.

## Sagas

```ts
import { WorkflowEngine } from "effect/workflow"
import { Saga } from "effect-cqrs"

const Transfer = Saga.workflow("Transfer", {
  payload: { transferId: Schema.String, from: Schema.String, to: Schema.String, amount: Schema.Int },
  idempotencyKey: ({ transferId }) => transferId
})

const TransferWorkflow = Saga.toLayer(Transfer, ({ amount, from, to, transferId }) =>
  Effect.gen(function*() {
    const bank = yield* Bank
    yield* Saga.command("credit", {
      error: AccountNotOpen,
      execute: bank.commands.receiveTransfer(to, { transferId, from, amount })
    }).pipe(
      Effect.catchTag("AccountNotOpen", () =>
        Saga.command("refund", {
          execute: bank.commands.refundTransfer(from, { transferId, to, amount, reason: "AccountNotOpen" })
        }))
    )
  }))

const Transfers = Saga.make({
  name: "transfers",
  aggregates: [Account],
  handlers: {
    TransferSent: ({ amount, to, transferId }, { aggregateId }) =>
      Saga.start(Transfer, { transferId, from: aggregateId, to, amount })
  }
})

const TransferSaga = Saga.layer([Transfers]).pipe(
  Layer.provideMerge(TransferWorkflow),
  Layer.provide(WorkflowEngine.layerMemory)
)
```

- `Saga.make` is a view model whose handlers only call `Saga.start`, which returns once the
  engine has the run.
- Every command in a workflow goes through `Saga.command`, an `Activity` whose outcome the
  engine keeps. List the domain errors the saga reacts to in `error`, so a replay takes the
  same branch.
- Undo earlier steps with the workflow's `withCompensation`, as in
  `step.pipe(Transfer.withCompensation(() => undo))`, on top-level steps of the body.
- An activity can run twice and a trigger can start a saga twice, so the commands a saga
  runs must be idempotent, for instance by recording the transfer id.
- `Saga.layer` provides `Saga.Sagas`, with `status`, `parked` and `retryParked` for the
  triggers, as `ViewModel.ViewModels` has for view models.
- Pair `WorkflowEngine.layerMemory` with `Checkpoints.layerMemory`, and a durable engine
  such as `ClusterWorkflowEngine` from `effect/cluster` with durable checkpoints.
- Wait with `DurableClock.sleep` and `DurableDeferred` from `effect/workflow`, never
  `Effect.sleep`.

## Personal data

```ts
import { PersonalData } from "effect-cqrs"

const AccountEvent = Schema.TaggedUnion({
  AccountOpened: {
    owner: PersonalData.field(Schema.String, { redacted: "[forgotten]" }),
    currency: Schema.Literals(["CHF", "EUR"])
  },
  TransferSent: {
    to: Schema.String,
    beneficiary: PersonalData.field(Schema.String, { redacted: "[forgotten]", subject: "to" }),
    amount: Schema.Number
  }
})

const Keys = PersonalData.layerSql().pipe(
  Layer.provide(PersonalData.layerMasterKeyConfig()), // PERSONAL_DATA_MASTER_KEY(_ID)
  Layer.provide(SqliteClient.layer({ filename: "./data/keys.sqlite" }))
)

yield* PersonalData.forget("alice")
```

- A field's subject is the stream's aggregate id, unless `subject` names a sibling property.
  `redacted` is what the field reads as once the subject is forgotten, and must be a valid
  value of the field.
- Personal fields can sit in the event, a nested struct, or be optional or nullable, but
  not in arrays, records or untagged unions. Mark the final schema:
  `PersonalData.field(Schema.String.pipe(Schema.decodeTo(...)), ...)`, not
  `PersonalData.field(Schema.String, ...).pipe(Schema.decodeTo(...))`, which fails.
- To rotate the master key, give it a new id, keep the old one in `previous`
  (`PERSONAL_DATA_MASTER_KEY_PREVIOUS` as `id:base64`), run `PersonalData.rewrap()`, then
  drop the old one.
- An aggregate with personal fields needs `PersonalData.Keys`. In tests use
  `PersonalData.layerMemory()` with `PersonalData.layerMasterKeyEphemeral`.
- After `forget`, commands that would write new personal data of the subject fail with
  `SubjectForgotten`.
- View models receive personal fields decrypted. One that stores them handles
  `onForget: (subject, meta) => ...` to erase them.
- Use opaque subject ids, not email addresses: the subject id is stored in plaintext.
  Don't log payloads of events with personal fields.

## Storage

```ts
import { SqliteClient } from "@effect/sql-sqlite-node"
import { SqliteEventStore } from "effect-cqrs"

const Storage = Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
  Layer.provide(SqliteClient.layer({ filename: "./data/events.sqlite" }))
)
```

`PostgresEventStore.layer()` and `layerCheckpoints()` work the same on a `PgClient`.
Tables are created on startup. `EventStore.layerMemory` keeps events in memory.

## Test with Fixture

```ts
import { assert, it } from "@effect/vitest"
import { DateTime, Effect } from "effect"
import { Fixture } from "effect-cqrs"

it.effect("creates a todo", () =>
  Fixture.make(Todo)
    .givenEvents()
    .when("createTodo", { title: "Buy Milk" })
    .expectEvent(TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: DateTime.makeUnsafe(0) }))
    .inspectState((state) => assert.strictEqual(state.title, "Buy Milk")))

it.effect("rejects deleting twice", () =>
  Effect.gen(function*() {
    const error = yield* Fixture.make(Todo).givenEvents([created, deleted]).whenFails("deleteTodo", {})
    assert.strictEqual(error._tag, "TodoAlreadyDeleted")
  }))
```

- The chain is lazy: return it from `it.effect` or `yield*` it, or it checks nothing.
- `expectEvent` takes a whole event or only its tag. `expectEvents([])` asserts that nothing
  was recorded.
- `it.effect` runs on a test clock stopped at zero, so `DateTime.now` gives
  `DateTime.makeUnsafe(0)` until `TestClock.adjust`. Sleeps and schedule delays never fire
  there: use `it.live` for tests that wait on retries.
- `givenEvents([Aggregate.historical(Todo, "TodoCreated", 1, { ... })])` seeds events in an
  older schema version to test upcasting.
- `checkInvariants({ invariant: (state) => ... })` runs random command sequences and
  reports the shortest one that breaks the predicate.
- For view models, wait with `viewModels.caughtUp(position)`, never with a sleep.

## Common mistakes

- **Effect 3 APIs.** Use `Context.Service`, not `Context.Tag` or `Effect.Service`;
  `Effect.catch`, not `Effect.catchAll`; `Effect.forkChild`, not `Effect.fork`;
  `Layer.effect`, not `Layer.scoped`; `Schema.Literals(["a", "b"])` and
  `Schema.Union([A, B])`.
- **Side effects in handlers.** A handler may run again after a lost race. It decides; it
  doesn't send emails or write elsewhere. React to the event in a view model or saga.
- **Mutating state.** States are kept between commands. Return new objects from
  projections and never change the one passed in.
- **A command on its own aggregate from its handler.** Commands on one aggregate take
  turns, so it waits for itself.
- **Non-idempotent view model or saga steps.** Delivery is at least once.
- **Durable view model, memory checkpoints** (or the other way round). A checkpoint must
  live exactly as long as what the view model stores.
- **Forgetting `dispose`.** Replay runs it before rebuilding; without it a replay adds to
  stale data.
- **Changing a stored event's schema in place.** Declare the old shape with
  `EventVersions`, or old events fail with `EventDecodeError`.
- **Reading a view model right after a command.** Wait for `caughtUp(position)` first.
- **Expecting events to reach other processes.** New events only wake subscribers in the
  same process; wire `dispatch` into your own HTTP or RPC layer.
