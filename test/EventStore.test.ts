import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it, layer } from "@effect/vitest"
import { DateTime, Duration, Effect, Fiber, Layer, Stream } from "effect"
import { SqlClient } from "effect/sql"
import { TestClock } from "effect/testing"
import { Checkpoints, EventStore, SqliteEventStore } from "effect-cqrs"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const append = (aggregateId: string, expectedVersion: number, ...types: ReadonlyArray<string>) =>
  EventStore.EventStore.use((store) =>
    store.append({
      aggregateName: "test",
      aggregateId,
      expectedVersion,
      events: types.map((type) => ({ type, payload: { type } }))
    })
  )

const read = (filter?: EventStore.EventFilter) =>
  EventStore.EventStore.use((store) => Stream.runCollect(store.read(filter)))

const adapters = {
  memory: EventStore.layerMemory,
  sqlite: SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" })))
}

for (const [name, adapter] of Object.entries(adapters)) {
  describe(`EventStore (${name})`, () => {
    it.effect("assigns stream versions and global positions", () =>
      Effect.gen(function*() {
        const first = yield* append("a", 0, "A1", "A2")
        const second = yield* append("b", 0, "B1")
        const third = yield* append("a", 2, "A3")

        assert.deepStrictEqual(first.map((event) => [event.position, event.version]), [[1, 1], [2, 2]])
        assert.deepStrictEqual(second.map((event) => [event.position, event.version]), [[3, 1]])
        assert.deepStrictEqual(third.map((event) => [event.position, event.version]), [[4, 3]])
      }).pipe(Effect.provide(adapter)))

    it.effect("round-trips stored fields", () =>
      Effect.gen(function*() {
        const [appended] = yield* append("a", 0, "A1")
        const [stored] = yield* read()

        assert.deepStrictEqual(stored, appended)
        assert.deepStrictEqual(stored?.payload, { type: "A1" })
      }).pipe(Effect.provide(adapter)))

    it.effect("rejects a stale or ahead expected version without writing", () =>
      Effect.gen(function*() {
        yield* append("a", 0, "A1", "A2")

        const stale = yield* Effect.flip(append("a", 1, "A3"))
        const ahead = yield* Effect.flip(append("a", 5, "A3"))

        assert.strictEqual(stale._tag, "ConcurrencyConflict")
        assert.strictEqual(ahead._tag, "ConcurrencyConflict")
        assert.strictEqual((yield* read()).length, 2)
      }).pipe(Effect.provide(adapter)))

    it.effect("keeps streams of different aggregates apart", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        yield* append("same-id", 0, "A1")
        const [other] = yield* store.append({
          aggregateName: "other",
          aggregateId: "same-id",
          expectedVersion: 0,
          events: [{ type: "O1", payload: null }]
        })

        assert.strictEqual(other?.version, 1)
      }).pipe(Effect.provide(adapter)))

    it.effect("filters by aggregate, type and position", () =>
      Effect.gen(function*() {
        yield* append("a", 0, "A1", "A2")
        yield* append("b", 0, "A1")

        const types = (filter: EventStore.EventFilter) =>
          Effect.map(read(filter), (events) => events.map((event) => `${event.aggregateId}:${event.type}`))

        assert.deepStrictEqual(yield* types({ aggregateIds: ["a"] }), ["a:A1", "a:A2"])
        assert.deepStrictEqual(yield* types({ eventTypes: ["A1"] }), ["a:A1", "b:A1"])
        assert.deepStrictEqual(yield* types({ aggregateNames: ["missing"] }), [])
        assert.deepStrictEqual(yield* types({ eventTypes: [] }), [])
        assert.deepStrictEqual(yield* types({ afterPosition: 2 }), ["b:A1"])
      }).pipe(Effect.provide(adapter)))

    it.effect("treats the time window as start inclusive and finish exclusive", () =>
      Effect.gen(function*() {
        const [first] = yield* append("a", 0, "A1")
        yield* TestClock.adjust(Duration.seconds(1))
        const [second] = yield* append("a", 1, "A2")
        yield* TestClock.adjust(Duration.seconds(1))
        yield* append("a", 2, "A3")

        const types = (filter: EventStore.EventFilter) =>
          Effect.map(read(filter), (events) => events.map((event) => event.type))

        assert.deepStrictEqual(yield* types({ startTime: second!.timestamp }), ["A2", "A3"])
        assert.deepStrictEqual(yield* types({ finishTime: second!.timestamp }), ["A1"])
        assert.deepStrictEqual(
          yield* types({ startTime: first!.timestamp, finishTime: DateTime.addDuration(second!.timestamp, "1 millis") }),
          ["A1", "A2"]
        )
      }).pipe(Effect.provide(adapter)))

    it.effect("reports the latest position as head", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        assert.strictEqual(yield* store.head, 0)
        yield* append("a", 0, "A1", "A2")
        assert.strictEqual(yield* store.head, 2)
      }).pipe(Effect.provide(adapter)))

    it.effect("subscribers receive history, then live events", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        yield* append("a", 0, "A1")

        const fiber = yield* store.subscribe().pipe(
          Stream.take(3),
          Stream.runCollect,
          Effect.forkChild
        )
        yield* Effect.yieldNow
        yield* append("a", 1, "A2")
        yield* append("a", 2, "A3")

        const events = yield* Fiber.join(fiber)
        assert.deepStrictEqual(events.map((event) => event.type), ["A1", "A2", "A3"])
      }).pipe(Effect.provide(adapter)))

    it.effect("subscribers only see what the filter selects, each event once", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore

        const fiber = yield* store.subscribe({ eventTypes: ["keep"] }).pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild
        )
        yield* Effect.yieldNow
        yield* append("a", 0, "skip", "keep")
        yield* append("a", 2, "skip")
        yield* append("a", 3, "keep")

        const events = yield* Fiber.join(fiber)
        assert.deepStrictEqual(events.map((event) => event.position), [2, 4])
      }).pipe(Effect.provide(adapter)))

    it.effect("reads one stream from a position, among other streams", () =>
      Effect.gen(function*() {
        yield* append("a", 0, "A1")
        yield* append("b", 0, "B1")
        yield* append("a", 1, "A2")
        yield* append("b", 1, "B2")
        yield* append("a", 2, "A3")

        const events = yield* read({ aggregateNames: ["test"], aggregateIds: ["a"], afterPosition: 1 })

        assert.deepStrictEqual(events.map((event) => event.type), ["A2", "A3"])
        assert.deepStrictEqual(yield* read({ aggregateNames: ["test"], aggregateIds: ["missing"] }), [])
      }).pipe(Effect.provide(adapter)))
  })
}

describe("EventStore pagination", () => {
  for (const [name, adapter] of Object.entries(adapters)) {
    it.effect(`pages through ${name} without losing or repeating events`, () =>
      Effect.gen(function*() {
        const total = 1203
        yield* append("a", 0, ...Array.from({ length: total }, (_, index) => `E${index}`))

        const events = yield* read()

        assert.strictEqual(events.length, total)
        assert.deepStrictEqual(events.map((event) => event.position), Array.from({ length: total }, (_, i) => i + 1))
      }).pipe(Effect.provide(adapter)))

    it.effect(`pages through one stream of ${name}, keeping to its events`, () =>
      Effect.gen(function*() {
        const total = 1203
        for (let version = 0; version < total; version++) {
          yield* append("a", version, `A${version}`)
          yield* append("b", version, `B${version}`)
        }

        const events = yield* read({ aggregateNames: ["test"], aggregateIds: ["a"] })

        assert.deepStrictEqual(events.map((event) => event.version), Array.from({ length: total }, (_, i) => i + 1))
      }).pipe(Effect.provide(adapter)))
  }

  it.effect("takes a batch too big to pass as function arguments", () =>
    Effect.gen(function*() {
      const total = 200_000

      const store = yield* EventStore.EventStore

      yield* store.append({
        aggregateName: "test",
        aggregateId: "a",
        expectedVersion: 0,
        events: Array.from({ length: total }, () => ({ type: "A", payload: null }))
      })

      assert.strictEqual(yield* store.head, total)
    }).pipe(Effect.provide(adapters.memory)))
})

describe("SqliteEventStore", () => {
  it.effect("keeps events and checkpoints across restarts", () =>
    Effect.gen(function*() {
      const filename = join(mkdtempSync(join(tmpdir(), "effect-cqrs-")), "events.sqlite")
      const database = Layer.fresh(SqliteClient.layer({ filename }))
      const storage = Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
        Layer.provide(database)
      )

      yield* Effect.gen(function*() {
        yield* append("a", 0, "A1")
        yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.set("view", 7))
      }).pipe(Effect.provide(storage))

      yield* Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        const checkpoints = yield* Checkpoints.Checkpoints

        assert.strictEqual(yield* store.head, 1)
        assert.strictEqual(yield* checkpoints.get("view"), 7)
        const [event] = yield* append("a", 1, "A2")
        assert.strictEqual(event?.position, 2)
      }).pipe(Effect.provide(storage))
    }))

  it.effect("rolls back the whole batch when an insert fails", () =>
    Effect.gen(function*() {
      const failure = yield* Effect.flip(
        EventStore.EventStore.use((store) =>
          store.append({
            aggregateName: "test",
            aggregateId: "a",
            expectedVersion: 0,
            events: [
              { type: "A1", payload: null },
              { type: null as unknown as string, payload: null }
            ]
          })
        )
      )

      assert.strictEqual(failure._tag, "EventStoreError")
      assert.strictEqual((yield* read()).length, 0)
    }).pipe(Effect.provide(adapters.sqlite)))

  it.effect("reads a stream in position order without sorting it", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient

      const plan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN SELECT * FROM cqrs_events
         WHERE position > ? AND aggregate_name IN (?) AND aggregate_id IN (?) ORDER BY position LIMIT 500`,
        [0, "test", "a"]
      )

      assert.isFalse(plan.some(({ detail }) => detail.includes("TEMP B-TREE")))
      assert.isTrue(plan.some(({ detail }) => detail.includes("cqrs_events_stream_idx")))
    }).pipe(
      Effect.provide(SqliteEventStore.layer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))))
    ))

  layer(
    SqliteEventStore.layer({ eventsTable: "custom_events" }).pipe(
      Layer.provide(SqliteClient.layer({ filename: ":memory:" }))
    )
  )("custom table", (it) => {
    it.effect("stores events in the configured table", () =>
      Effect.gen(function*() {
        yield* append("a", 0, "A1")
        assert.strictEqual((yield* read()).length, 1)
      }))
  })
})
