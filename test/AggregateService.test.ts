import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, DateTime, Duration, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect"
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

    const incrementTogether = (
      counters: ReadonlyArray<AggregateService.AggregateService<typeof Counter>>,
      limit = 100
    ) =>
      Effect.all(
        counters.map((service) => Effect.exit(service.commands.increment("counter-1", { limit }))),
        { concurrency: "unbounded" }
      )

    const countingConflicts = Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const conflicts = yield* Ref.make(0)
      const counting = EventStore.EventStore.of({
        ...store,
        append: (input) => store.append(input).pipe(Effect.tapError(() => Ref.update(conflicts, (n) => n + 1)))
      })
      const counters = yield* AggregateService.make(Counter).pipe(
        Effect.provideService(EventStore.EventStore, counting)
      )
      return { conflicts, counters }
    })

    for (
      const [name, storage] of [
        ["memory", EventStore.layerMemory],
        ["SQLite", SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" })))]
      ] as const
    ) {
      it.effect(`runs concurrent commands on one stream in turn on ${name}, so they can't conflict`, () =>
        Effect.gen(function*() {
          const { conflicts, counters } = yield* countingConflicts

          const exits = yield* incrementTogether(Array.from({ length: 16 }, () => counters))

          assert.isTrue(exits.every(Exit.isSuccess))
          assert.strictEqual(yield* Ref.get(conflicts), 0)
          assert.strictEqual(yield* counters.readModel("counter-1"), 16)
        }).pipe(Effect.provide(storage)))
    }

    it.effect("decides each turn on the state the previous one left", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter)

        const exits = yield* incrementTogether([counters, counters, counters], 1)

        assert.strictEqual(exits.filter(Exit.isSuccess).length, 1)
        assert.strictEqual(yield* counters.readModel("counter-1"), 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("frees the stream when a command fails", () =>
      Effect.gen(function*() {
        const counters = yield* AggregateService.make(Counter)

        const error = yield* Effect.flip(counters.commands.increment("counter-1", { limit: 0 }))
        yield* counters.commands.increment("counter-1", { limit: 1 })

        assert.strictEqual(error._tag, "Limited")
        assert.strictEqual(yield* counters.readModel("counter-1"), 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("frees the stream when a command is interrupted", () =>
      Effect.gen(function*() {
        const Stuck = Aggregate.make<number>()({
          name: "stuck",
          events: Events,
          initial: 0,
          projection: { Incremented: (total, { by }) => total + by }
        })
          .command("hang", { payload: Schema.Struct({}), handler: () => Effect.never })
          .command("increment", {
            payload: Schema.Struct({}),
            handler: () => Effect.succeed(Events.cases.Incremented.make({ by: 1 }))
          })
        const service = yield* AggregateService.make(Stuck)

        const hanging = yield* Effect.forkChild(service.commands.hang("a", {}))
        yield* Effect.yieldNow
        yield* Fiber.interrupt(hanging)
        yield* service.commands.increment("a", {})

        assert.strictEqual(yield* service.readModel("a"), 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    // Two services on one store stand in for two processes: no lock covers both.
    it.live("retries a command that lost a race to another process, on fresh state", () =>
      Effect.gen(function*() {
        const first = yield* AggregateService.make(Counter)
        const second = yield* AggregateService.make(Counter)

        const exits = yield* incrementTogether([first, second, first, second])

        assert.isTrue(exits.every(Exit.isSuccess))
        assert.strictEqual(yield* first.readModel("counter-1"), 4)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.live("re-decides during a retry, so a losing command can be rejected", () =>
      Effect.gen(function*() {
        const first = yield* AggregateService.make(Counter)
        const second = yield* AggregateService.make(Counter)

        const exits = yield* incrementTogether([first, second], 1)

        assert.strictEqual(exits.filter(Exit.isSuccess).length, 1)
        assert.strictEqual(yield* first.readModel("counter-1"), 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("surfaces the conflict once retries run out", () =>
      Effect.gen(function*() {
        const first = yield* AggregateService.make(Counter, { maxRetries: 0 })
        const second = yield* AggregateService.make(Counter, { maxRetries: 0 })

        const exits = yield* incrementTogether([first, second])

        const failures = exits.flatMap((exit) => Exit.isFailure(exit) ? [Cause.squash(exit.cause)] : [])
        assert.strictEqual(failures.length, 1)
        assert.instanceOf(failures[0], EventStore.ConcurrencyConflict)
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("state cache", () => {
    const Events = Schema.TaggedUnion({ Incremented: {} })
    const Counter = Aggregate.make<number>()({
      name: "counter",
      events: Events,
      initial: 0,
      projection: { Incremented: (total) => total + 1 }
    }).command("increment", {
      payload: Schema.Struct({}),
      handler: () => Effect.succeed(Events.cases.Incremented.make({}))
    })

    const recordingReads = (options?: { readonly stateCacheSize?: number }) =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        const reads: Array<number> = []
        const counters = yield* AggregateService.make(Counter, options).pipe(
          Effect.provideService(
            EventStore.EventStore,
            EventStore.EventStore.of({
              ...store,
              read: (filter) => {
                reads.push(filter?.afterPosition ?? 0)
                return store.read(filter)
              }
            })
          )
        )
        return { counters, reads }
      })

    it.effect("only reads what came after the state it remembers", () =>
      Effect.gen(function*() {
        const { counters, reads } = yield* recordingReads()

        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("a", {})

        // Each command remembers the state its own events left.
        assert.deepStrictEqual(reads, [0, 1, 2])
        assert.strictEqual(yield* counters.readModel("a"), 3)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("sees what another process wrote after the state it remembers", () =>
      Effect.gen(function*() {
        const first = yield* AggregateService.make(Counter)
        const second = yield* AggregateService.make(Counter)

        yield* first.commands.increment("a", {})
        yield* second.commands.increment("a", {})
        yield* second.commands.increment("a", {})

        assert.strictEqual(yield* first.readModel("a"), 3)
        assert.strictEqual((yield* first.commands.increment("a", {})).version, 4)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("forgets the least recently used state past its size", () =>
      Effect.gen(function*() {
        const { counters, reads } = yield* recordingReads({ stateCacheSize: 1 })

        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("b", {})
        yield* counters.commands.increment("a", {})

        // Remembering "b" forgets "a", which is read again from the start.
        assert.deepStrictEqual(reads, [0, 1, 0, 0])
        assert.strictEqual(yield* counters.readModel("a"), 3)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("can be turned off", () =>
      Effect.gen(function*() {
        const { counters, reads } = yield* recordingReads({ stateCacheSize: 0 })

        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("a", {})
        yield* counters.commands.increment("a", {})

        assert.deepStrictEqual(reads, [0, 0, 0])
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
