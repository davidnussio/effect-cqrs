import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, DateTime, Duration, Effect, Exit, Layer, Ref, Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import { Aggregate, AggregateService, EventStore, SqliteEventStore } from "effect-cqrs"
import { Todo, TodoEvent } from "../examples/todo/aggregate.ts"

const makeTodos = AggregateService.make(Todo)

describe("AggregateService", () => {
  describe("commands", () => {
    it.effect("stores the events a command decides on", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        const store = yield* EventStore.EventStore

        const result = yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })

        assert.strictEqual(result.aggregateName, "todo")
        assert.strictEqual(result.aggregateId, "todo-1")
        assert.strictEqual(result.version, 1)
        assert.strictEqual(result.position, 1)
        const [stored] = yield* Stream.runCollect(store.read())
        assert.strictEqual(stored?.type, "TodoCreated")
        assert.strictEqual(stored?.aggregateName, "todo")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("runs handlers against the state built from earlier events", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* todos.commands.deleteTodo("todo-1", { message: "Done" })

        const error = yield* Effect.flip(todos.commands.deleteTodo("todo-1", { message: "Again" }))

        assert.strictEqual(error._tag, "TodoAlreadyDeleted")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("records nothing when a command emits no events", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        const store = yield* EventStore.EventStore
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* todos.commands.completeTodo("todo-1", {})

        const result = yield* todos.commands.completeTodo("todo-1", {})

        assert.deepStrictEqual(result.events, [])
        assert.strictEqual(result.version, 2)
        assert.strictEqual(result.position, 2)
        assert.strictEqual(yield* store.head, 2)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("validates the payload of typed calls too", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        const store = yield* EventStore.EventStore

        const error = yield* Effect.flip(todos.commands.createTodo("todo-1", { title: "" }))

        assert.strictEqual(error._tag, "InvalidCommand")
        assert.strictEqual(yield* store.head, 0)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("stores several events from one command together", () =>
      Effect.gen(function*() {
        const Events = Schema.TaggedUnion({ Tick: {}, Tock: {} })
        const Clock = Aggregate.make<number>()({
          name: "clock",
          events: Events,
          initial: 0,
          projection: { Tick: (n) => n + 1, Tock: (n) => n + 1 }
        }).command("cycle", {
          payload: Schema.Struct({}),
          handler: () => Effect.succeed([Events.cases.Tick.make({}), Events.cases.Tock.make({})])
        })
        const clocks = yield* AggregateService.make(Clock)

        const result = yield* clocks.commands.cycle("clock-1", {})

        assert.strictEqual(result.version, 2)
        assert.strictEqual(yield* clocks.readModel("clock-1"), 2)
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("handlers using services and failures", () => {
    const Events = Schema.TaggedUnion({ Incremented: { by: Schema.Int } })
    class Limited extends Schema.TaggedError<Limited>()("Limited", {}) {}

    const Counter = Aggregate.make<number>()({
      name: "counter",
      events: Events,
      initial: 0,
      projection: { Incremented: (total, { by }) => total + by }
    }).command("increment", {
      payload: Schema.Struct({ limit: Schema.Int }),
      handler: Effect.fnUntraced(function*(total, { limit }) {
        yield* Effect.yieldNow
        if (total >= limit) return yield* new Limited()
        return Events.cases.Incremented.make({ by: 1 })
      })
    })

    it.effect("retries a command that lost a race, deciding again on fresh state", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter)

        yield* Effect.all(
          Array.from({ length: 4 }, () => counters.commands.increment("counter-1", { limit: 100 })),
          { concurrency: "unbounded" }
        )

        assert.strictEqual(yield* counters.readModel("counter-1"), 4)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("retries on SQLite too", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter)

        yield* Effect.all(
          Array.from({ length: 4 }, () => counters.commands.increment("counter-1", { limit: 100 })),
          { concurrency: "unbounded" }
        )

        assert.strictEqual(yield* counters.readModel("counter-1"), 4)
      }).pipe(
        Effect.provide(SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" }))))
      ))

    it.effect("re-decides during a retry, so a losing command can be rejected", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter)

        const exits = yield* Effect.all(
          Array.from({ length: 3 }, () => Effect.exit(counters.commands.increment("counter-1", { limit: 1 }))),
          { concurrency: "unbounded" }
        )

        assert.strictEqual(exits.filter((exit) => exit._tag === "Success").length, 1)
        assert.strictEqual(yield* counters.readModel("counter-1"), 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("surfaces the conflict once retries run out", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter, { maxRetries: 0 })

        const exits = yield* Effect.all(
          [
            Effect.exit(counters.commands.increment("counter-1", { limit: 100 })),
            Effect.exit(counters.commands.increment("counter-1", { limit: 100 }))
          ],
          { concurrency: "unbounded" }
        )

        const failures = exits.flatMap((exit) => Exit.isFailure(exit) ? [Cause.squash(exit.cause)] : [])
        assert.strictEqual(failures.length, 1)
        assert.instanceOf(failures[0], EventStore.ConcurrencyConflict)
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("dispatch", () => {
    it.effect("runs a JSON envelope", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos

        const result = yield* todos.dispatch({
          aggregateId: "todo-1",
          type: "createTodo",
          payload: { title: "Buy Milk" }
        })

        assert.strictEqual(result.version, 1)
        assert.strictEqual(result.events[0]?._tag, "TodoCreated")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("rejects unknown commands, including inherited object keys", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos

        const unknown = yield* Effect.flip(todos.dispatch({ aggregateId: "a", type: "nope", payload: {} }))
        const inherited = yield* Effect.flip(todos.dispatch({ aggregateId: "a", type: "toString", payload: {} }))

        assert.strictEqual(unknown._tag, "UnknownCommand")
        assert.strictEqual(inherited._tag, "UnknownCommand")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("rejects malformed envelopes and payloads", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos

        const envelope = yield* Effect.flip(todos.dispatch({ type: "createTodo" }))
        const payload = yield* Effect.flip(
          todos.dispatch({ aggregateId: "a", type: "createTodo", payload: { title: 1 } })
        )

        assert.strictEqual(envelope._tag, "InvalidCommand")
        assert.strictEqual(payload._tag, "InvalidCommand")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("still lets command errors through", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos

        const error = yield* Effect.flip(
          todos.dispatch({ aggregateId: "a", type: "deleteTodo", payload: { message: "x" } })
        )

        assert.strictEqual(error._tag, "TodoNotFound")
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("read models", () => {
    it.effect("returns the initial state for an aggregate without events", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        assert.deepStrictEqual(yield* todos.readModel("missing"), { status: "missing" })
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("materializes the state at a point in time", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* TestClock.adjust(Duration.seconds(1))
        const before = yield* DateTime.now
        yield* TestClock.adjust(Duration.seconds(1))
        yield* todos.commands.deleteTodo("todo-1", { message: "Done" })

        assert.deepStrictEqual(yield* todos.readModel("todo-1", { finishTime: before }), {
          status: "open",
          title: "Buy Milk"
        })
        assert.deepStrictEqual(yield* todos.readModel("todo-1"), { status: "deleted", title: "Buy Milk" })
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("fails with a typed error when a stored event no longer decodes", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        const store = yield* EventStore.EventStore
        yield* store.append({
          aggregateName: "todo",
          aggregateId: "todo-1",
          expectedVersion: 0,
          events: [{ type: "TodoCreated", payload: { _tag: "TodoCreated", title: 42 } }]
        })

        const error = yield* Effect.flip(todos.readModel("todo-1"))

        assert.strictEqual(error._tag, "EventDecodeError")
        assert.strictEqual(error._tag === "EventDecodeError" && error.version, 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("history", () => {
    it.effect("lists event metadata, with payloads on request", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* todos.commands.createTodo("todo-2", { title: "Buy Eggs" })
        yield* TestClock.adjust(Duration.seconds(1))
        yield* todos.commands.deleteTodo("todo-1", { message: "Done" })

        const history = yield* todos.history("todo-1")
        const withPayload = yield* todos.history("todo-1", { payload: true })

        assert.deepStrictEqual(history.map((entry) => [entry.version, entry.type]), [
          [1, "TodoCreated"],
          [2, "TodoDeleted"]
        ])
        assert.isFalse("payload" in history[0]!)
        assert.deepStrictEqual(
          withPayload[0]?.payload,
          TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: DateTime.makeUnsafe(0) })
        )
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("restricts the time window", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* TestClock.adjust(Duration.seconds(1))
        const startTime = yield* DateTime.now
        yield* todos.commands.deleteTodo("todo-1", { message: "Done" })

        const history = yield* todos.history("todo-1", { startTime })

        assert.deepStrictEqual(history.map((entry) => entry.type), ["TodoDeleted"])
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  it.effect("provides the services handlers need when the service is built", () =>
    Effect.gen(function*() {
      const Events = Schema.TaggedUnion({ Seen: { count: Schema.Int } })
      const counter = yield* Ref.make(0)
      const Seen = Aggregate.make<number>()({
        name: "seen",
        events: Events,
        initial: 0,
        projection: { Seen: (_, { count }) => count }
      }).command("see", {
        payload: Schema.Struct({}),
        handler: () =>
          Ref.updateAndGet(counter, (n) => n + 1).pipe(
            Effect.map((count) => Events.cases.Seen.make({ count }))
          )
      })
      const seen = yield* AggregateService.make(Seen)

      yield* seen.commands.see("a", {})

      assert.strictEqual(yield* seen.readModel("a"), 1)
    }).pipe(Effect.provide(EventStore.layerMemory)))
})
