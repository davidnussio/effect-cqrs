import { assert, describe, it } from "@effect/vitest"
import { Effect, Latch, Layer, References, Schema } from "effect"
import { Aggregate, AggregateService, EventStore, PersonalData } from "effect-cqrs"
import { KeyValueStore } from "effect/persistence"

const Events = Schema.TaggedUnion({ Incremented: {} })
const Counter = Aggregate.make<{ readonly total: number }>()({
  name: "counter",
  events: Events,
  initial: { total: 0 },
  projection: { Incremented: ({ total }) => ({ total: total + 1 }) }
}).command("increment", {
  payload: Schema.Struct({}),
  handler: () => Effect.succeed(Events.cases.Incremented.make({}))
})

const State = Schema.Struct({ total: Schema.Int })

/** A service as a new process would build it, recording where each replay of a stream starts. */
const start = (snapshots: Partial<AggregateService.SnapshotOptions<{ readonly total: number }>> = {}) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const reads: Array<number> = []
    const counters = yield* AggregateService.make(Counter, {
      snapshots: { every: 3, schema: State, ...snapshots }
    }).pipe(
      Effect.provideService(
        EventStore.EventStore,
        EventStore.EventStore.of({
          ...store,
          read: (filter) => {
            reads.push(filter?.afterVersion ?? 0)
            return store.read(filter)
          }
        })
      )
    )
    return { counters, reads }
  })

const increment = (counters: AggregateService.AggregateService<typeof Counter>, times: number) =>
  Effect.repeat(counters.commands.increment("c", {}), { times: times - 1 })

const Storage = Layer.merge(EventStore.layerMemory, KeyValueStore.layerMemory)

/** Runs a first process to its end, which waits for the snapshots it is still saving. */
const runFirst = (times: number) => Effect.scoped(Effect.flatMap(start(), ({ counters }) => increment(counters, times)))

describe("snapshots", () => {
  it.effect("let a new process replay only what came after the last one", () =>
    Effect.gen(function*() {
      // A snapshot is saved once three events have been folded since the last.
      yield* runFirst(5)

      const second = yield* start()
      assert.deepStrictEqual(yield* second.counters.readModel("c"), { total: 5 })
      assert.deepStrictEqual(second.reads, [3])
    }).pipe(Effect.provide(Storage)))

  it.effect("save again every so many events", () =>
    Effect.gen(function*() {
      yield* runFirst(8)

      const second = yield* start()
      yield* second.counters.readModel("c")
      assert.deepStrictEqual(second.reads, [6])
    }).pipe(Effect.provide(Storage)))

  it.effect("are ignored under another version, so a changed state is rebuilt from the log", () =>
    Effect.gen(function*() {
      yield* runFirst(5)

      const second = yield* start({ version: 2 })
      assert.deepStrictEqual(yield* second.counters.readModel("c"), { total: 5 })
      assert.deepStrictEqual(second.reads, [0])
    }).pipe(Effect.provide(Storage)))

  it.effect("fall back to the log when one doesn't decode", () =>
    Effect.gen(function*() {
      yield* runFirst(5)

      const second = yield* start({ schema: Schema.Struct({ total: Schema.String }) as any })
      assert.deepStrictEqual(yield* second.counters.readModel("c"), { total: 5 })
      assert.deepStrictEqual(second.reads, [0])
    }).pipe(
      Effect.provide(Storage),
      Effect.provideService(References.MinimumLogLevel, "None")
    ))

  it.effect("let the next command start after what the last one wrote", () =>
    Effect.gen(function*() {
      const { counters, reads } = yield* start()
      yield* increment(counters, 3)

      assert.deepStrictEqual(reads, [0, 1, 2])
      assert.deepStrictEqual(yield* counters.readModel("c"), { total: 3 })
      assert.deepStrictEqual(reads, [0, 1, 2, 3])
    }).pipe(Effect.provide(Storage)))

  it.effect("are saved without holding up commands, and finished when the service closes", () =>
    Effect.gen(function*() {
      const memory = yield* KeyValueStore.KeyValueStore
      const gate = Latch.makeUnsafe(false)
      const slow = KeyValueStore.make({
        get: memory.get,
        getUint8Array: memory.getUint8Array,
        set: (key, value) => Effect.andThen(gate.await, memory.set(key, value)),
        remove: memory.remove,
        clear: memory.clear,
        size: memory.size
      })

      yield* Effect.scoped(Effect.gen(function*() {
        const { counters } = yield* start()
        // The save started at version 3 is still waiting on the gate.
        const result = yield* increment(counters, 4)
        assert.strictEqual(result.version, 4)
        yield* gate.open
      })).pipe(Effect.provideService(KeyValueStore.KeyValueStore, slow))

      const second = yield* start()
      yield* second.counters.readModel("c")
      assert.deepStrictEqual(second.reads, [3])
    }).pipe(Effect.provide(Storage)))

  it.effect("keep the latest command ids, so a new process still knows them", () =>
    Effect.gen(function*() {
      const first = yield* Effect.scoped(Effect.flatMap(start(), ({ counters }) =>
        Effect.gen(function*() {
          const written = yield* counters.commands.increment("c", {}, { commandId: "x" })
          yield* increment(counters, 2)
          return written
        })))

      // The snapshot at version 3 holds the id, so the new process starts there and answers from it.
      const second = yield* start()
      const again = yield* second.counters.commands.increment("c", {}, { commandId: "x" })
      assert.deepStrictEqual(again, first)
      assert.deepStrictEqual(second.reads, [3, 0])
      assert.deepStrictEqual(yield* second.counters.readModel("c"), { total: 3 })
    }).pipe(Effect.provide(Storage)))

  it.effect("keep the latest command ids when they are sealed", () =>
    Effect.gen(function*() {
      const Opened = Schema.TaggedUnion({ Opened: { owner: PersonalData.field(Schema.String, { redacted: "?" }) } })
      const Account = Aggregate.make<string>()({
        name: "account",
        events: Opened,
        initial: "",
        projection: { Opened: (_, { owner }) => owner }
      }).command("open", {
        payload: Schema.Struct({ owner: Schema.String }),
        handler: (_, { owner }) => Effect.succeed(Opened.cases.Opened.make({ owner }))
      })
      const snapshots = { every: 1, schema: Schema.String }

      const first = yield* Effect.scoped(Effect.flatMap(
        AggregateService.make(Account, { snapshots }),
        (accounts) => accounts.commands.open("alice", { owner: "Alice" }, { commandId: "x" })
      ))

      const store = yield* EventStore.EventStore
      const reads: Array<number> = []
      const second = yield* AggregateService.make(Account, { snapshots }).pipe(
        Effect.provideService(
          EventStore.EventStore,
          EventStore.EventStore.of({
            ...store,
            read: (filter) => {
              reads.push(filter?.afterVersion ?? 0)
              return store.read(filter)
            }
          })
        )
      )
      const again = yield* second.commands.open("alice", { owner: "Alice" }, { commandId: "x" })
      assert.deepStrictEqual(again, first)
      assert.deepStrictEqual(reads, [1, 0])
    }).pipe(
      Effect.provide(Layer.mergeAll(
        Storage,
        PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
      ))
    ))

  it.effect("never fail a command when they can't be saved", () =>
    Effect.gen(function*() {
      const { counters } = yield* start()
      const result = yield* increment(counters, 4)

      assert.strictEqual(result.version, 4)
    }).pipe(
      Effect.provide(EventStore.layerMemory),
      Effect.provideService(
        KeyValueStore.KeyValueStore,
        KeyValueStore.make({
          get: () => Effect.succeed(undefined),
          getUint8Array: () => Effect.succeed(undefined),
          set: (key) => Effect.fail(new KeyValueStore.KeyValueStoreError({ method: "set", key, message: "full" })),
          remove: () => Effect.void,
          clear: Effect.void,
          size: Effect.succeed(0)
        })
      ),
      Effect.provideService(References.MinimumLogLevel, "None")
    ))
})
