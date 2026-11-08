# Effect v4 patterns used in this project

Read `repos/effect/LLMS.md` first. This file records what that guide doesn't say,
learned while building this framework against Effect 4.0.6. Check any claim
against `repos/effect/packages/effect/src` before relying on it.

## Services

```ts
export class EventStore extends Context.Service<EventStore, {
  append(input: AppendInput): Effect.Effect<Stored, ConcurrencyConflict>
}>()("effect-cqrs/EventStore") {}
```

- Read a service with `yield* EventStore`, or with `EventStore.use((store) => ...)`
  outside a generator. There is no `.asEffect()` on service classes.
- `Layer.effect` accepts effects that need a `Scope` and removes it. There is no
  separate scoped constructor to reach for.
- Background work in a layer: `Effect.forkScoped` ties the fiber to the layer's
  lifetime.
- A library module exports its service plus `make`/`layer` at module level, like
  `EventJournal.ts`. A `static` member can't reference a const declared after the
  class.
- `Context.Reference<T>(key, { defaultValue })` is a service with a default, for
  ambient values such as `Causation.CurrentCausation`. Read it with `Ref.use(f)`.
- `Effect.provideContext(context)` merges, and `context` wins over what the caller
  provided. A service that captured its context at construction hides the caller's
  references, so read those outside the `provideContext`.

## Schema

```ts
const TodoEvent = Schema.TaggedUnion({
  TodoCreated: { title: Schema.NonEmptyString, createdAt: Schema.DateTimeUtc }
})
TodoEvent.cases.TodoCreated.make({ title, createdAt }) // fills in _tag
```

- `Schema.toCodecJson(schema)` derives a JSON codec for any schema, including
  `Schema.DateTimeUtc`, whose encoded side is otherwise a `DateTime`. Use it for
  anything that gets persisted or crosses a process boundary.
- `Schema.toType(schema)` validates values that are already decoded, keeping the
  checks. Use it for typed entry points. `NonEmptyString` is just `string` at the
  type level, so an unvalidated `""` compiles.
- `Schema.Json` is the type of a JSON value, and `Schema.fromJsonString(Schema.Json)`
  decodes a JSON text column.
- One-way migrations: `from.pipe(Schema.decodeTo(Schema.toType(next), { decode:
  SchemaGetter.transform(f), encode: SchemaGetter.forbidden(() => "...") }))`. Chains
  composed this way decode an old value in one pass, as `EventVersions.decoders` does.
- A `Schema.Struct` ignores keys it doesn't declare and leaves them out of its output.
- Errors are `Schema.TaggedError<X>()("X", { ... })`. Yield them directly:
  `return yield* new X({ ... })`. Inside `Effect.catchTag` wrap them with
  `Effect.fail`, because a union of two error classes is not an `Effect`.

## Generators

- `Effect.fn("name")` where a span helps, `Effect.fnUntraced` inside the library.
  Never `.pipe` them: pass combinators as extra arguments.
- `Effect.fnUntraced(function*(state, payload) { ... })` picks up parameter types
  from context, so a command handler needs no annotations.

- A fluent builder that should run on `yield*` extends `Effectable.Class` and
  implements `asEffect()`. It is then a real `Effect`, so `.pipe`, `Effect.exit` and
  returning it from `it.effect` all work, and it stays lazy, as `Fixture` does.

## Type inference

- A type parameter inferred from a sibling property doesn't give object literals
  in a callback's return a literal context: `status: "open"` widens to `string`.
  Take such a parameter explicitly with a curried `make<State>()({ ... })`, the
  same shape as `Schema.TaggedError<X>()`.
- For a record of effectful handlers, infer the record as a whole and compute `E`
  and `R` with a mapped type, as `ViewModel.make` does.
- In generic code `Effect.provideContext` leaves `Exclude<R, R>` unresolved. Cast
  once at a small helper rather than through the whole function.

## Streams and subscriptions

- `SubscriptionRef.changes` emits the current value first, then every update. That
  makes it a safe wake-up signal: subscribe, then catch up, and nothing is missed.
- `Stream.paginate(seed, f)` where `f` returns `[items, Option<nextSeed>]` is the
  shape for keyset pagination. Each page reaches `Stream.runForEachArray` as one
  non-empty array, so per-page checkpoints come for free.
- `Stream.runFoldEffect` is the effectful fold. Its initial value is a thunk.

## Scheduling

- `Effect.forEach` with `concurrency` starts each item in a fresh fiber and carries on
  while items finish without suspending. Each fiber has its own operation budget, so
  no other fiber runs until the loop ends. `node:sqlite` is synchronous, which makes
  this real. A sequential loop is one fiber, so it yields by itself.
- Yield before the work, not after it. With `concurrency: "unbounded"` every item still
  runs to its end before the loop lets go if the yield comes last.
- `Effect.yieldNow` isn't free: per command it halved the throughput of the memory
  store. Yield on a time slice instead, as `makeTimeSlice` does in `AggregateService`.
  Restart the slice after the yield, so items started in the meantime suspend too.

## SQL

- `sql.withTransaction` works as a pipe step. `sql.in("column", values)` with an
  empty array selects nothing rather than producing invalid SQL.
- Identifiers go through `sql(name)`, whole fragments through `sql.literal`.
- `SqlError` holds a typed `reason`. Match `reason._tag === "UniqueViolation"`
  exactly: `"ConstraintError"` also covers `NOT NULL` failures, and treating it as
  a concurrency conflict would retry a write that can never succeed.
- `@effect/sql-sqlite-node` uses `node:sqlite` with one connection, opens writable
  transactions with `BEGIN IMMEDIATE`, and doesn't support streaming queries, so
  read in pages.
- Alias columns in SQL (`aggregate_id AS "aggregateId"`) so a query doesn't depend
  on the name transforms configured on the client.
- `@effect/sql-pg` speaks the wire protocol itself and binds at most 32,767 parameters
  to a statement. Convert `BIGINT` with `::float8` to get a number, and qualify the
  column in `ORDER BY` so it doesn't mean the converted alias, which has no index.

## Workflows

- The module is `effect/workflow`, not `effect/unstable/workflow`. `Workflow.make(tag, {
  payload, idempotencyKey, success, error })` takes the tag first.
- A workflow body runs in the engine's fibers with the context captured by `toLayer`, so
  references such as `CurrentCausation` don't reach it from the caller. Carry them in the
  payload, as `Saga.workflow` does. Activities run in the body's fiber and see its context.
- `withCompensation` only registers on top-level steps of the body, and a compensation can
  itself be an activity.
- `Layer.provide(Layer.empty, layer)` runs a layer for its effects and hides what it
  provides, as `Saga.layer` does with `ViewModel.layer`.

## Persistence

- `KeyValueStore.toSchemaStore(store, schema)` from `effect/persistence` stores typed
  values as JSON. `KeyValueStore` has `layerMemory`, `layerFileSystem`, `layerSql` and
  `layerStorage` for `localStorage`.

## Testing

- `it.effect` runs on `TestClock`: time stands still until `TestClock.adjust`, so
  timestamps are deterministic, but `Effect.sleep`, `Effect.timeout` and schedule
  delays never fire. Use `it.live` for tests that wait on retries.
- Wait for background work with a signal, never a sleep. Here that is
  `viewModels.caughtUp(position)`.
- Silence expected error logs with
  `Effect.provideService(References.MinimumLogLevel, "None")`.
- `Effect.provide(layer)` builds a fresh layer per call. `Layer.fresh` forces a new
  instance when the same layer value is reused, as the restart tests do.
- Don't assert which of two concurrent effects wins. Assert the invariant.
- `Effect.addFinalizer` in a test body registers on the test's scope, which closes after
  any layer the body gets through `Effect.provide`. A finalizer that needs that layer,
  like dropping a table through its client, goes inside `Effect.scoped` before the
  `provide`.
- `Arbitrary` (from `effect`) does property tests without fast-check. It has no `oneOf`:
  pick a case with `Arbitrary.flatMap(Arbitrary.schema(Schema.Literals(names)), ...)`.
  `Arbitrary.checkEffect` returns a `CheckResult` with the shrunk input, and
  `formatCheckFailure` describes it.

## v3 habits that no longer compile

| v3 | v4 |
| --- | --- |
| `Context.Tag`, `Effect.Service` | `Context.Service` |
| `Effect.catchAll` | `Effect.catch` |
| `Effect.fork` | `Effect.forkChild` |
| `Layer.scoped` | `Layer.effect` |
| `Schema.Literal("a", "b")` | `Schema.Literals(["a", "b"])` |
| `Schema.Union(A, B)` | `Schema.Union([A, B])` |

See `repos/effect/MIGRATION.md` for the full list.
