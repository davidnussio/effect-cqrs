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
- Errors are `Schema.TaggedError<X>()("X", { ... })`. Yield them directly:
  `return yield* new X({ ... })`. Inside `Effect.catchTag` wrap them with
  `Effect.fail`, because a union of two error classes is not an `Effect`.

## Generators

- `Effect.fn("name")` where a span helps, `Effect.fnUntraced` inside the library.
  Never `.pipe` them: pass combinators as extra arguments.
- `Effect.fnUntraced(function*(state, payload) { ... })` picks up parameter types
  from context, so a command handler needs no annotations.

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
