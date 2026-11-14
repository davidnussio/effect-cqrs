import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Layer, Metric, References, Schema } from "effect"
import { Aggregate, AggregateService, EventStore } from "effect-cqrs"
import { KeyValueStore } from "effect/persistence"

const { MetricNames } = AggregateService

class TooMany extends Schema.TaggedError<TooMany>()("TooMany", {}) {}

const Events = Schema.TaggedUnion({ Incremented: {} })
const Counter = Aggregate.make<{ readonly total: number }>()({
  name: "counter",
  events: Events,
  initial: { total: 0 },
  projection: { Incremented: ({ total }) => ({ total: total + 1 }) }
}).command("increment", {
  payload: Schema.Struct({ limit: Schema.Int }),
  handler: Effect.fnUntraced(function*({ total }, { limit }) {
    // Lets a rival decide on the same version, so two services can race.
    yield* Effect.yieldNow
    if (total >= limit) return yield* new TooMany()
    return Events.cases.Incremented.make({})
  })
})

const State = Schema.Struct({ total: Schema.Int })

/** Every series of a metric whose attributes include `attributes`. */
const series = (name: string, attributes: Record<string, string> = {}) =>
  Effect.map(Metric.snapshot, (snapshots) =>
    snapshots.filter((snapshot) =>
      snapshot.id === name &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value)
    ))

/** The total of the counters, or the number of observations of the histograms, that match. */
const count = (name: string, attributes?: Record<string, string>) =>
  Effect.map(
    series(name, attributes),
    (found) => found.reduce((sum, { state }) => sum + Number((state as { readonly count: number | bigint }).count), 0)
  )

const sumOf = (name: string, attributes?: Record<string, string>) =>
  Effect.map(
    series(name, attributes),
    (found) => found.reduce((sum, { state }) => sum + (state as { readonly sum: number }).sum, 0)
  )

const gauge = (name: string) =>
  Effect.map(series(name), ([found]) => (found?.state as { readonly value: number } | undefined)?.value)

/** Gives each test registry of its own, so counts don't leak between tests. */
const isolated = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.suspend(() => Effect.provideService(effect, Metric.MetricRegistry, new Map()))

const increment = (counters: AggregateService.AggregateService<typeof Counter>, times: number, limit = 100) =>
  Effect.repeat(counters.commands.increment("c", { limit }), { times: times - 1 })

describe("metrics", () => {
  it.effect("record where each load found its state and how many events it read", () =>
    Effect.gen(function*() {
      const counters = yield* AggregateService.make(Counter)

      yield* increment(counters, 3)

      // The first command replays the empty stream, the others find its state in memory.
      assert.strictEqual(yield* count(MetricNames.stateLoads, { aggregate: "counter", source: "replay" }), 1)
      assert.strictEqual(yield* count(MetricNames.stateLoads, { source: "memory" }), 2)
      assert.strictEqual(yield* count(MetricNames.eventsReplayed), 3)
      assert.strictEqual(yield* sumOf(MetricNames.eventsReplayed), 0)
      assert.strictEqual(yield* gauge(MetricNames.stateCacheSize), 1)
    }).pipe(isolated, Effect.provide(EventStore.layerMemory)))

  it.effect("count the events another process wrote as read", () =>
    Effect.gen(function*() {
      const first = yield* AggregateService.make(Counter)
      const second = yield* AggregateService.make(Counter)
      yield* increment(first, 1)

      yield* increment(second, 4)
      yield* first.state("c")

      assert.strictEqual(yield* sumOf(MetricNames.eventsReplayed), 1 + 4)
    }).pipe(isolated, Effect.provide(EventStore.layerMemory)))

  it.effect("time commands by outcome, naming the error that refused one", () =>
    Effect.gen(function*() {
      const counters = yield* AggregateService.make(Counter)

      yield* increment(counters, 2, 2)
      yield* Effect.exit(counters.commands.increment("c", { limit: 2 }))
      yield* Effect.exit(counters.commands.increment("c", { limit: -1.5 }))

      const attributes = { aggregate: "counter", command: "increment" }
      assert.strictEqual(yield* count(MetricNames.commandDuration, { ...attributes, outcome: "success" }), 2)
      assert.strictEqual(yield* count(MetricNames.commandDuration, { ...attributes, outcome: "TooMany" }), 1)
      // An invalid payload never reaches the stream.
      assert.strictEqual(yield* count(MetricNames.commandDuration), 3)
    }).pipe(isolated, Effect.provide(EventStore.layerMemory)))

  it.effect("count appends that lost a race with another writer", () =>
    Effect.gen(function*() {
      const first = yield* AggregateService.make(Counter, { maxRetries: 0 })
      const second = yield* AggregateService.make(Counter, { maxRetries: 0 })

      const exits = yield* Effect.all(
        [first, second].map((counters) => Effect.exit(counters.commands.increment("c", { limit: 100 }))),
        { concurrency: "unbounded" }
      )

      assert.strictEqual(exits.filter(Exit.isFailure).length, 1)
      assert.strictEqual(yield* count(MetricNames.concurrencyConflicts), 1)
      assert.strictEqual(yield* count(MetricNames.commandDuration, { outcome: "ConcurrencyConflict" }), 1)
    }).pipe(isolated, Effect.provide(EventStore.layerMemory)))

  it.effect("count the states dropped from a full cache", () =>
    Effect.gen(function*() {
      const counters = yield* AggregateService.make(Counter, { stateCacheSize: 2 })

      for (const id of ["a", "b", "c", "a"]) yield* counters.commands.increment(id, { limit: 100 })

      assert.strictEqual(yield* gauge(MetricNames.stateCacheSize), 2)
      // "c" pushed out "a", whose return pushed out "b".
      assert.strictEqual(yield* count(MetricNames.stateCacheEvictions), 2)
      assert.strictEqual(yield* count(MetricNames.stateLoads, { source: "replay" }), 4)
    }).pipe(isolated, Effect.provide(EventStore.layerMemory)))

  describe("with snapshots", () => {
    const Storage = Layer.merge(EventStore.layerMemory, KeyValueStore.layerMemory)
    const withSnapshots = (schema: Schema.Codec<{ readonly total: number }, any> = State) =>
      AggregateService.make(Counter, { stateCacheSize: 0, snapshots: { every: 3, schema } })

    it.effect("count the snapshots saved, and the loads that started from one", () =>
      Effect.gen(function*() {
        yield* Effect.scoped(Effect.flatMap(withSnapshots(), (counters) => increment(counters, 7)))

        assert.strictEqual(yield* count(MetricNames.snapshotSaves, { result: "success" }), 2)
        // With no memory, every command after the third starts from a snapshot.
        assert.strictEqual(yield* count(MetricNames.stateLoads, { source: "snapshot" }), 4)
        assert.strictEqual(yield* count(MetricNames.stateLoads, { source: "replay" }), 3)
      }).pipe(isolated, Effect.scoped, Effect.provide(Storage)))

    it.effect("count the snapshots that couldn't be read", () =>
      Effect.gen(function*() {
        yield* Effect.scoped(Effect.flatMap(withSnapshots(), (counters) => increment(counters, 3)))

        const counters = yield* withSnapshots(Schema.Struct({ total: Schema.String }) as any)
        yield* counters.state("c")

        assert.strictEqual(yield* count(MetricNames.snapshotReadFailures), 1)
      }).pipe(
        isolated,
        Effect.scoped,
        Effect.provide(Storage),
        Effect.provideService(References.MinimumLogLevel, "None")
      ))

    it.effect("count the snapshots that couldn't be saved", () =>
      Effect.gen(function*() {
        yield* Effect.scoped(Effect.flatMap(withSnapshots(), (counters) => increment(counters, 3)))

        assert.strictEqual(yield* count(MetricNames.snapshotSaves, { result: "failure" }), 1)
      }).pipe(
        isolated,
        Effect.provide(EventStore.layerMemory),
        Effect.provide(
          Layer.effect(
            KeyValueStore.KeyValueStore,
            Effect.map(KeyValueStore.KeyValueStore, (store) =>
              KeyValueStore.KeyValueStore.of({
                ...store,
                set: (key) => Effect.fail(new KeyValueStore.KeyValueStoreError({ message: "full", method: "set", key }))
              }))
          ).pipe(Layer.provide(KeyValueStore.layerMemory))
        ),
        Effect.provideService(References.MinimumLogLevel, "None")
      ))
  })
})
