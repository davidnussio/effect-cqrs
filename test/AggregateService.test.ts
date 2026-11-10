import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, DateTime, Duration, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect"
import { Aggregate, AggregateService, EventStore, Ids, SqliteEventStore } from "effect-cqrs"
import { TestClock } from "effect/testing"
import { postgresUrl, storage } from "../bench/common.ts"
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

  it.effect("runs handlers under the caller's span, not the one the service was built in", () =>
    Effect.gen(function*() {
      const parents: Array<string | undefined> = []
      const Traced = Aggregate.make<number>()({
        name: "traced",
        events: Schema.TaggedUnion({ Ticked: {} }),
        initial: 0,
        projection: { Ticked: (total) => total + 1 }
      }).command("tick", {
        payload: Schema.Struct({}),
        handler: () =>
          Effect.map(Effect.currentParentSpan, (span) => {
            parents.push(span._tag === "Span" ? span.name : undefined)
            return { _tag: "Ticked" as const }
          })
      })
      const traced = yield* AggregateService.make(Traced).pipe(Effect.withSpan("startup"))
      yield* traced.commands.tick("a", {}).pipe(Effect.withSpan("request"))
      assert.deepStrictEqual(parents, ["traced.tick"])
    }).pipe(Effect.provide(EventStore.layerMemory)))

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

  describe("command ids", () => {
    it.effect("refuses an id that another command ran with", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" }, { commandId: "request-1" })

        const error = yield* Effect.flip(todos.commands.completeTodo("todo-1", {}, { commandId: "request-1" }))

        assert.strictEqual(error._tag, "CommandIdReused")
        assert.deepStrictEqual(error._tag === "CommandIdReused" && [error.previous, error.command], [
          "createTodo",
          "completeTodo"
        ])
        assert.strictEqual((yield* todos.history("todo-1")).length, 1)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("returns only the events of the command run again, later in its stream", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* todos.commands.completeTodo("todo-1", {}, { commandId: "complete" })

        const again = yield* todos.commands.completeTodo("todo-1", {}, { commandId: "complete" })

        assert.deepStrictEqual(again.events.map((event) => event._tag), ["TodoCompleted"])
        assert.strictEqual(again.version, 2)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    const Events = Schema.TaggedUnion({ Deposited: { amount: Schema.Number }, Fee: { amount: Schema.Number } })
    const Account = Aggregate.make<number>()({
      name: "account",
      events: Events,
      initial: 0,
      projection: {
        Deposited: (balance, { amount }) => balance + amount,
        Fee: (balance, { amount }) => balance - amount
      }
    }).command("deposit", {
      payload: Schema.Struct({ amount: Schema.Number }),
      handler: Effect.fnUntraced(function*(_, { amount }) {
        yield* Effect.yieldNow
        return [Events.cases.Deposited.make({ amount }), Events.cases.Fee.make({ amount: 1 })]
      })
    }).command("check", {
      payload: Schema.Struct({}),
      handler: () => Effect.succeed([])
    })

    it.effect("return the first result when a command comes again", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        const store = yield* EventStore.EventStore

        const first = yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c1" })
        const again = yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c1" })

        assert.deepStrictEqual(again, first)
        assert.strictEqual(again.version, 2)
        assert.strictEqual(yield* store.head, 2)
        assert.strictEqual(yield* accounts.readModel("a"), 9)
        const stored = yield* Stream.runCollect(store.read())
        assert.deepStrictEqual(stored.map((event) => event.metadata[AggregateService.commandIdKey]), ["c1", "c1"])
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("work through dispatch", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        const envelope = { aggregateId: "a", type: "deposit", payload: { amount: 10 }, commandId: "c1" }

        const first = yield* accounts.dispatch(envelope)
        const again = yield* accounts.dispatch(envelope)

        assert.deepStrictEqual(again, first)
        assert.strictEqual(yield* accounts.readModel("a"), 9)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("are rebuilt from the log by another process", () =>
      Effect.gen(function*() {
        const first = yield* AggregateService.make(Account)
        const written = yield* first.commands.deposit("a", { amount: 10 }, { commandId: "c1" })
        yield* first.commands.deposit("a", { amount: 5 })

        const second = yield* AggregateService.make(Account)
        const again = yield* second.commands.deposit("a", { amount: 10 }, { commandId: "c1" })

        assert.deepStrictEqual(again, written)
        assert.strictEqual(yield* second.readModel("a"), 13)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("belong to one aggregate", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)

        yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c1" })
        const other = yield* accounts.commands.deposit("b", { amount: 10 }, { commandId: "c1" })

        assert.strictEqual(other.aggregateId, "b")
        assert.strictEqual(yield* accounts.readModel("b"), 9)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("are remembered up to commandIdsPerStream per aggregate", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account, { commandIdsPerStream: 2 })
        for (const commandId of ["c1", "c2", "c3"]) {
          yield* accounts.commands.deposit("a", { amount: 10 }, { commandId })
        }

        yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c3" })
        yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c2" })
        assert.strictEqual(yield* accounts.readModel("a"), 27)

        // c1 has been forgotten, so it runs again.
        yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c1" })
        assert.strictEqual(yield* accounts.readModel("a"), 36)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("aren't recorded for commands that write no events", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)

        const checked = yield* accounts.commands.check("a", {}, { commandId: "c1" })
        const deposited = yield* accounts.commands.deposit("a", { amount: 10 }, { commandId: "c1" })

        assert.strictEqual(checked.version, 0)
        assert.strictEqual(deposited.version, 2)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("drop metadata given under their reserved key", () =>
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        const store = yield* EventStore.EventStore

        yield* accounts.dispatch({
          aggregateId: "a",
          type: "deposit",
          payload: { amount: 10 },
          metadata: { [AggregateService.commandIdKey]: "c1", userId: "u1" }
        })

        const stored = yield* Stream.runCollect(store.read())
        assert.deepStrictEqual(stored[0]?.metadata, { userId: "u1" })
      }).pipe(Effect.provide(EventStore.layerMemory)))

    // Two services on one store stand in for two processes retrying the same request.
    for (
      const backend of ["memory", "sqlite-memory", ...(postgresUrl === undefined ? [] : ["postgres" as const])] as const
    ) {
      it.live(`let only one of two processes run the same command on ${backend}`, () =>
        Effect.gen(function*() {
          const first = yield* AggregateService.make(Account)
          const second = yield* AggregateService.make(Account)
          const store = yield* EventStore.EventStore

          const results = yield* Effect.all(
            [first, second, first, second].map((service) =>
              service.commands.deposit("a", { amount: 10 }, { commandId: "c1" })
            ),
            { concurrency: "unbounded" }
          )

          assert.strictEqual(yield* store.head, 2)
          assert.isTrue(results.every((result) => result.version === 2 && result.events.length === 2))
        }).pipe(Effect.provide(storage(backend))))
    }
  })

  describe("expected version", () => {
    const Events = Schema.TaggedUnion({ Renamed: { name: Schema.String } })
    const Doc = Aggregate.make<string>()({
      name: "doc",
      events: Events,
      initial: "",
      projection: { Renamed: (_, { name }) => name }
    }).command("rename", {
      payload: Schema.Struct({ name: Schema.String }),
      handler: (_, { name }) => Effect.succeed(Events.cases.Renamed.make({ name }))
    })

    it.effect("lets a command through when the aggregate is still at that version", () =>
      Effect.gen(function*() {
        const docs = yield* AggregateService.make(Doc)
        yield* docs.commands.rename("d", { name: "a" }, { expectedVersion: 0 })

        const result = yield* docs.commands.rename("d", { name: "b" }, { expectedVersion: 1 })

        assert.strictEqual(result.version, 2)
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("fails without retrying when the aggregate has moved on", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        let appends = 0
        const docs = yield* AggregateService.make(Doc).pipe(
          Effect.provideService(
            EventStore.EventStore,
            EventStore.EventStore.of({
              ...store,
              append: (input) => Effect.suspend(() => (appends++, store.append(input)))
            })
          )
        )
        yield* docs.commands.rename("d", { name: "a" })
        yield* docs.commands.rename("d", { name: "b" })

        const error = yield* Effect.flip(docs.commands.rename("d", { name: "c" }, { expectedVersion: 1 }))

        assert.instanceOf(error, AggregateService.WrongExpectedVersion)
        assert.deepInclude(error, { aggregateId: "d", expectedVersion: 1, actualVersion: 2 })
        assert.strictEqual(appends, 2)
        assert.strictEqual(yield* docs.readModel("d"), "b")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("is read from the dispatch envelope", () =>
      Effect.gen(function*() {
        const docs = yield* AggregateService.make(Doc)
        yield* docs.commands.rename("d", { name: "a" })

        const stale = yield* Effect.flip(
          docs.dispatch({ aggregateId: "d", type: "rename", payload: { name: "b" }, expectedVersion: 0 })
        )
        const invalid = yield* Effect.flip(
          docs.dispatch({ aggregateId: "d", type: "rename", payload: { name: "b" }, expectedVersion: -1 })
        )

        assert.strictEqual(stale._tag, "WrongExpectedVersion")
        assert.strictEqual(invalid._tag, "InvalidCommand")
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("doesn't stop a command that comes again with its id", () =>
      Effect.gen(function*() {
        const docs = yield* AggregateService.make(Doc)
        const options = { commandId: "c1", expectedVersion: 0 }

        const first = yield* docs.commands.rename("d", { name: "a" }, options)
        const again = yield* docs.commands.rename("d", { name: "a" }, options)

        assert.deepStrictEqual(again, first)
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  it.effect("makes correlation ids with the current IdGenerator", () =>
    Effect.gen(function*() {
      const todos = yield* makeTodos

      const first = yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
      const second = yield* todos.commands.createTodo("todo-2", { title: "Buy Eggs" })

      assert.deepStrictEqual([first.correlationId, second.correlationId], ["id-1", "id-2"])
    }).pipe(
      Effect.provide(EventStore.layerMemory),
      Effect.provideService(Ids.IdGenerator, Ids.sequential())
    ))

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

    it.effect("reads a long stream in pages", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        yield* todos.commands.createTodo("todo-1", { title: "Buy Milk" })
        yield* todos.commands.completeTodo("todo-1", {})
        yield* todos.commands.deleteTodo("todo-1", { message: "Done" })

        const first = yield* todos.history("todo-1", { limit: 2 })
        const next = yield* todos.history("todo-1", { afterVersion: first.at(-1)!.version, limit: 2, payload: true })

        assert.deepStrictEqual(first.map((entry) => entry.type), ["TodoCreated", "TodoCompleted"])
        assert.deepStrictEqual(next.map((entry) => [entry.version, entry.payload?._tag]), [[3, "TodoDeleted"]])
      }).pipe(Effect.provide(EventStore.layerMemory)))

    it.effect("lists events without decoding them when the payloads aren't asked for", () =>
      Effect.gen(function*() {
        const todos = yield* makeTodos
        const store = yield* EventStore.EventStore
        yield* store.append({
          aggregateName: "todo",
          aggregateId: "todo-1",
          expectedVersion: 0,
          events: [{ type: "TodoCreated", payload: { _tag: "TodoCreated", title: "" } }]
        })

        assert.deepStrictEqual((yield* todos.history("todo-1")).map((entry) => entry.type), ["TodoCreated"])
        assert.isTrue(Exit.isFailure(yield* Effect.exit(todos.history("todo-1", { payload: true }))))
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
