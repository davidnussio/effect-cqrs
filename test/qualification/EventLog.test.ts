import { assert, describe, it } from "@effect/vitest"
import { Cause, DateTime, Effect, Exit, Fiber, type Schema, Stream } from "effect"
import { EventStore } from "effect-cqrs"
import { TestClock } from "effect/testing"
import { type DurableBackend, openStorage, postgresUrl, temporaryDurable } from "../../bench/common.ts"
import { scenarios, smokeSizes } from "../../bench/stress-scenarios.ts"
import { configurations, on, outcome, range, readAll, storageOf, verifies } from "./support.ts"

const append = (aggregateId: string, expectedVersion: number, ...types: ReadonlyArray<string>) =>
  EventStore.EventStore.use((store) =>
    store.append({
      aggregateName: "record",
      aggregateId,
      expectedVersion,
      events: types.map((type) => ({ type, payload: { type } }))
    })
  )

const oneStream = (aggregateId: string) => readAll({ aggregateNames: ["record"], aggregateIds: [aggregateId] })

const upTo = (count: number) => range(count).map((index) => index + 1)

/** Values a careless store would change: escapes, Unicode, the edges of numbers, deep and large structures. */
const faithful: ReadonlyArray<Schema.Json> = [
  null,
  true,
  false,
  "",
  " leading and trailing ",
  "ünïcödé ✓ 日本語 العربية",
  "👩🏽‍⚕️ joined emoji",
  "é is not é",
  "quotes \" ' ` and \\ backslashes \\n",
  "line\nbreaks\r\n\tand tabs",
  "'; DROP TABLE cqrs_events; --",
  "%_?$1 :name {{x}}",
  "x".repeat(200_000),
  0,
  -1,
  0.1,
  -1.5e-7,
  1e21,
  123456789.12345679,
  Number.MAX_SAFE_INTEGER,
  Number.MIN_SAFE_INTEGER,
  Number.MAX_VALUE,
  Number.MIN_VALUE,
  [],
  {},
  [1, [2, [3, [4, [5, [6, [7, [8, [9, [10]]]]]]]]]],
  { "": 1, "a.b": 2, "a b": 3, "ключ": 4, constructor: 5, toString: 6 },
  { nested: { list: [null, false, 0, "", [], {}], dose: { value: 2.5, unit: "mg" } } },
  range(2000).map((index) => ({ index, label: `item-${index}` }))
]

/** Values some databases can't hold. Whatever a backend does with them, it must not store something else. */
const unusual: ReadonlyArray<string> = ["a nul \u0000 inside", "a lone surrogate \ud800 inside"]

for (const configuration of configurations) {
  const storage = storageOf(configuration)

  describe.skipIf(!configuration.available)(`event log ${on(configuration)}`, () => {
    it.effect(
      verifies("ES-001", "stores nothing of an append that loses its stream version"),
      () =>
        Effect.gen(function*() {
          yield* append("a", 0, "A1")
          const before = yield* readAll()

          const error = yield* Effect.flip(append("a", 0, "X1", "X2", "X3"))

          assert.strictEqual(error._tag, "ConcurrencyConflict")
          assert.deepStrictEqual(yield* readAll(), before)
          const [next] = yield* append("b", 0, "B1")
          assert.strictEqual(next?.position, 2)
        }).pipe(Effect.provide(storage))
    )

    it.effect.skipIf(!configuration.sql)(
      verifies("ES-001", "stores nothing of an append whose last event the database refuses"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          yield* append("a", 0, "A1")

          const exit = yield* Effect.exit(store.append({
            aggregateName: "record",
            aggregateId: "b",
            expectedVersion: 0,
            events: [
              { type: "B1", payload: null },
              { type: "B2", payload: null },
              { type: null as unknown as string, payload: null }
            ]
          }))

          assert.isTrue(Exit.isFailure(exit))
          assert.deepStrictEqual((yield* readAll()).map((event) => event.type), ["A1"])
          assert.deepStrictEqual(yield* oneStream("b"), [])
          const [next] = yield* append("b", 0, "B1")
          assert.deepStrictEqual([next?.position, next?.version], [2, 1])
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("ES-002", "numbers streams and the store without gaps, with writers at once"),
      () =>
        Effect.gen(function*() {
          const streams = range(40)
          yield* Effect.forEach(streams, (index) => append(`s-${index}`, 0, "E1", "E2", "E3"), { concurrency: 8 })
          yield* Effect.forEach(streams, (index) => append(`s-${index}`, 3, "E4", "E5"), { concurrency: 8 })

          const events = yield* readAll()

          assert.deepStrictEqual(events.map((event) => event.position), upTo(200))
          for (const index of streams) {
            const stream = events.filter((event) => event.aggregateId === `s-${index}`)
            assert.deepStrictEqual(stream.map((event) => event.version), upTo(5))
            assert.deepStrictEqual(stream.map((event) => event.type), ["E1", "E2", "E3", "E4", "E5"])
          }
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("ES-003", "stores one of the appends that race for a stream version"),
      () =>
        Effect.gen(function*() {
          yield* append("a", 0, "A1")

          const outcomes = yield* Effect.forEach(
            range(12),
            (index) =>
              Effect.map(outcome(append("a", 1, `W${index}`, `W${index}-second`)), (tag) => [index, tag] as const),
            { concurrency: "unbounded" }
          )

          const winners = outcomes.filter(([, tag]) => tag === "ok")
          assert.strictEqual(winners.length, 1)
          assert.isTrue(outcomes.every(([, tag]) => tag === "ok" || tag === "ConcurrencyConflict"))
          const [winner] = winners[0]!
          assert.deepStrictEqual((yield* oneStream("a")).map((event) => event.type), [
            "A1",
            `W${winner}`,
            `W${winner}-second`
          ])
          assert.strictEqual((yield* readAll()).length, 3)
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("ES-004", "reads every field back as it was appended"), () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        yield* TestClock.adjust("1234567 millis")
        const metadata = {
          user: "dr. Rossi ✓",
          roles: ["physician", "admin"],
          session: { id: 7, trusted: true },
          note: null
        }

        const appended = yield* store.append({
          aggregateName: "record",
          aggregateId: "fidelity",
          expectedVersion: 0,
          events: faithful.map((payload, index) => ({ type: `Value${index}`, payload, schemaVersion: index + 1 })),
          correlationId: "correlation ✓ 'quoted'",
          causationId: "record/other%20stream@12",
          metadata
        })
        const stored = yield* oneStream("fidelity")

        assert.deepStrictEqual(stored, appended)
        assert.deepStrictEqual(stored.map((event) => event.payload), faithful)
        for (const [index, event] of stored.entries()) {
          assert.deepStrictEqual(event.metadata, metadata)
          assert.deepStrictEqual(
            [event.type, event.schemaVersion, event.correlationId, event.causationId],
            [`Value${index}`, index + 1, "correlation ✓ 'quoted'", "record/other%20stream@12"]
          )
          assert.strictEqual(DateTime.toEpochMillis(event.timestamp), 1234567)
        }
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("ES-004", "refuses a value it can't hold rather than storing another"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          for (const [index, value] of unusual.entries()) {
            const aggregateId = `unusual-${index}`
            const result = yield* store.append({
              aggregateName: "record",
              aggregateId,
              expectedVersion: 0,
              events: [{ type: "Unusual", payload: { value } }],
              metadata: { value }
            }).pipe(outcome)

            const stored = yield* oneStream(aggregateId)
            if (result === "ok") {
              assert.deepStrictEqual(stored.map((event) => [event.payload, event.metadata]), [[{ value }, { value }]])
            } else {
              assert.strictEqual(result, "EventStoreError")
              assert.deepStrictEqual(stored, [])
            }
          }
          // The store still works after a refusal.
          const [next] = yield* append("after", 0, "A1")
          assert.strictEqual(next?.version, 1)
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("ES-005", "offers no way to change an event, and keeps events as the log grows"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          assert.deepStrictEqual(Object.keys(store).sort(), ["append", "changes", "head", "read", "subscribe"])

          yield* append("a", 0, "A1", "A2")
          yield* append("b", 0, "B1")
          const before = yield* readAll()

          yield* append("a", 2, "A3", "A4")
          yield* append("c", 0, "C1")
          yield* Effect.flip(append("a", 0, "A1-again"))
          yield* Effect.flip(append("b", 0, "B1-again"))

          assert.deepStrictEqual((yield* readAll()).slice(0, before.length), before)
          assert.deepStrictEqual(
            (yield* oneStream("a")).slice(0, 2),
            before.filter((event) => event.aggregateId === "a")
          )
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("ES-006", "reads every event once and in order, across pages"), () =>
      Effect.gen(function*() {
        // Pages hold 500 events: the store ends up with 1,612 and stream `a` with 806.
        const chunk = range(31).map((index) => `E${index}`)
        const versions = { a: 0, b: 0, c: 0 }
        for (const _ of range(13)) {
          for (const aggregateId of ["a", "b", "a", "c"] as const) {
            yield* append(aggregateId, versions[aggregateId], ...chunk)
            versions[aggregateId] += chunk.length
          }
        }

        const events = yield* readAll()
        assert.deepStrictEqual(events.map((event) => event.position), upTo(1612))

        const stream = yield* oneStream("a")
        assert.deepStrictEqual(stream.map((event) => event.version), upTo(806))
        assert.isTrue(stream.every((event, index) => index === 0 || event.position > stream[index - 1]!.position))
        assert.deepStrictEqual(stream, events.filter((event) => event.aggregateId === "a"))

        const rest = yield* readAll({ afterPosition: 700 })
        assert.deepStrictEqual(rest.map((event) => event.position), upTo(1612).slice(700))
        const later = yield* readAll({ aggregateNames: ["record"], aggregateIds: ["a"], afterVersion: 600 })
        assert.deepStrictEqual(later.map((event) => event.version), upTo(806).slice(600))
      }).pipe(Effect.provide(storage)), { timeout: 30_000 })

    it.effect(
      verifies("ES-007", "selects by stream, type and a time window that excludes its end"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          const types = (filter: EventStore.EventFilter) =>
            Effect.map(readAll(filter), (events) => events.map((event) => `${event.aggregateId}:${event.type}`))

          yield* append("a", 0, "T0")
          yield* TestClock.adjust("1 second")
          const start = yield* DateTime.now
          yield* append("a", 1, "T1")
          yield* append("b", 0, "T1")
          yield* TestClock.adjust("1 second")
          const finish = yield* DateTime.now
          yield* append("a", 2, "T2")
          yield* store.append({
            aggregateName: "other",
            aggregateId: "a",
            expectedVersion: 0,
            events: [{ type: "T2", payload: null }]
          })

          assert.deepStrictEqual(yield* types({ startTime: start, finishTime: finish }), ["a:T1", "b:T1"])
          assert.deepStrictEqual(yield* types({ finishTime: start }), ["a:T0"])
          assert.deepStrictEqual(yield* types({ startTime: finish }), ["a:T2", "a:T2"])
          assert.deepStrictEqual(yield* types({ aggregateNames: ["record"], startTime: finish }), ["a:T2"])
          assert.deepStrictEqual(yield* types({ aggregateIds: ["b"] }), ["b:T1"])
          assert.deepStrictEqual(yield* types({ eventTypes: ["T0", "T2"], aggregateNames: ["record"] }), [
            "a:T0",
            "a:T2"
          ])
          assert.deepStrictEqual(yield* types({ aggregateNames: ["missing"] }), [])
          assert.deepStrictEqual(yield* types({ eventTypes: [] }), [])
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("ES-008", "keeps streams apart whose names or ids look alike"), () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        const ids = [
          "a",
          "A",
          "a ",
          " a",
          "a/b",
          "a%2Fb",
          "a@1",
          "a'",
          "a\"",
          "a\\",
          "a;--",
          "a%",
          "a_",
          "é",
          "é",
          "ａ",
          "1",
          "01",
          "1.0"
        ]
        const names = ["record", "Record", "record ", "record/a"]
        const streams = names.flatMap((aggregateName) => ids.map((aggregateId) => ({ aggregateName, aggregateId })))

        for (const [index, stream] of streams.entries()) {
          const [stored] = yield* store.append({
            ...stream,
            expectedVersion: 0,
            events: [{ type: `Only${index}`, payload: { index } }]
          })
          assert.strictEqual(stored?.version, 1)
        }

        assert.strictEqual((yield* readAll()).length, streams.length)
        for (const [index, { aggregateId, aggregateName }] of streams.entries()) {
          const events = yield* readAll({ aggregateNames: [aggregateName], aggregateIds: [aggregateId] })
          assert.deepStrictEqual(
            events.map((event) => [event.aggregateName, event.aggregateId, event.type]),
            [[aggregateName, aggregateId, `Only${index}`]]
          )
        }
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("ES-009", "gives a subscriber history and then live events, each once and in order"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          yield* append("history", 0, "H1", "H2", "H3")

          const all = yield* store.subscribe().pipe(Stream.take(33), Stream.runCollect, Effect.forkChild)
          const filtered = yield* store.subscribe({ aggregateNames: ["record"], aggregateIds: ["live-0"] }).pipe(
            Stream.take(6),
            Stream.runCollect,
            Effect.forkChild
          )
          yield* Effect.yieldNow
          for (const index of range(30)) yield* append(`live-${index % 5}`, Math.floor(index / 5), `L${index}`)

          assert.deepStrictEqual((yield* Fiber.join(all)).map((event) => event.position), upTo(33))
          assert.deepStrictEqual((yield* Fiber.join(filtered)).map((event) => event.type), [
            "L0",
            "L5",
            "L10",
            "L15",
            "L20",
            "L25"
          ])
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("ES-013", "records the application's time of each append"), () =>
      Effect.gen(function*() {
        yield* TestClock.adjust("90 seconds")
        const first = yield* append("a", 0, "A1", "A2", "A3")
        yield* TestClock.adjust("1 milli")
        const second = yield* append("a", 3, "A4")
        const third = yield* append("b", 0, "B1")

        const millis = (events: ReadonlyArray<EventStore.StoredEvent>) =>
          events.map((event) => DateTime.toEpochMillis(event.timestamp))
        assert.deepStrictEqual(millis([...first, ...second, ...third]), [90_000, 90_000, 90_000, 90_001, 90_001])
        assert.deepStrictEqual(millis(yield* readAll()), [90_000, 90_000, 90_000, 90_001, 90_001])
      }).pipe(Effect.provide(storage)))
  })
}

describe("event log on any backend", () => {
  const failure = new EventStore.EventStoreError({ operation: "write", cause: new Error("the disk is full") })
  const stored = (position: number): EventStore.StoredEvent => ({
    position,
    aggregateName: "record",
    aggregateId: "a",
    version: position,
    type: "A",
    schemaVersion: 1,
    payload: null,
    timestamp: DateTime.makeUnsafe(0),
    correlationId: null,
    causationId: null,
    metadata: {}
  })

  it.effect(verifies("ES-010", "reports a backend that fails to write or read"), () =>
    Effect.gen(function*() {
      const store = yield* EventStore.make({
        append: () => Effect.fail(failure),
        readBatch: () => Effect.fail(failure),
        head: Effect.succeed(0)
      })

      const appended = append("a", 0, "A1").pipe(Effect.provideService(EventStore.EventStore, store))
      assert.strictEqual(yield* Effect.flip(appended), failure)
      assert.strictEqual(yield* Effect.flip(Stream.runCollect(store.read())), failure)

      // A subscriber that has something to read finds out too.
      const behind = yield* EventStore.make({
        append: () => Effect.fail(failure),
        readBatch: () => Effect.fail(failure),
        head: Effect.succeed(1)
      })
      assert.strictEqual(yield* Effect.flip(Stream.runCollect(behind.subscribe().pipe(Stream.take(1)))), failure)
    }))

  it.effect(
    verifies("ES-010", "fails a read that breaks off after its first page, rather than ending it"),
    () =>
      Effect.gen(function*() {
        const store = yield* EventStore.make({
          append: () => Effect.fail(failure),
          readBatch: (_, afterPosition) =>
            afterPosition === 0 ? Effect.succeed([stored(1), stored(2)]) : Effect.fail(failure),
          head: Effect.succeed(2)
        }, { batchSize: 2 })

        const seen: Array<number> = []
        const error = yield* Effect.flip(
          Stream.runForEach(store.read(), (event) => Effect.sync(() => seen.push(event.position)))
        )

        assert.strictEqual(error, failure)
        assert.deepStrictEqual(seen, [1, 2])
      })
  )

  it.effect(verifies("ES-010", "can't be built on a backend that doesn't answer"), () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(EventStore.make({
        append: () => Effect.fail(failure),
        readBatch: () => Effect.fail(failure),
        head: Effect.fail(failure)
      }))

      assert.strictEqual(error, failure)
    }))
})

const durable: ReadonlyArray<DurableBackend> = ["sqlite-file", "postgres"]

for (const backend of durable) {
  const available = backend !== "postgres" || postgresUrl !== undefined

  describe.skipIf(!available)(`durable event log ${on({ name: backend })}`, () => {
    it.live(
      verifies("ES-011", "keeps acknowledged events when the store is closed and opened again"),
      () =>
        Effect.gen(function*() {
          const { target } = yield* temporaryDurable(backend)

          const acknowledged = yield* Effect.gen(function*() {
            const first = yield* append("a", 0, "A1", "A2")
            const second = yield* append("b", 0, "B1")
            return [...first, ...second]
          }).pipe(Effect.provide(openStorage(backend, target)))

          yield* Effect.gen(function*() {
            const store = yield* EventStore.EventStore
            assert.deepStrictEqual(yield* readAll(), acknowledged)
            assert.strictEqual(yield* store.head, 3)

            const conflict = yield* Effect.flip(append("b", 0, "B1-again"))
            assert.strictEqual(conflict._tag, "ConcurrencyConflict")
            const [next] = yield* append("a", 2, "A3")
            assert.deepStrictEqual([next?.position, next?.version], [4, 3])
          }).pipe(Effect.provide(openStorage(backend, target)))

          const reopened = yield* readAll().pipe(Effect.provide(openStorage(backend, target)))
          assert.deepStrictEqual(reopened.map((event) => event.type), ["A1", "A2", "B1", "A3"])
        })
    )

    const crash = scenarios(
      { ...smokeSizes, crashes: 3, crashWriters: 3 },
      backend === "postgres" ? "postgres" : "sqlite"
    ).find(({ name }) => name === "crash")!

    it.live(
      verifies("ES-012", "loses no acknowledged command and tears none when writer processes are killed"),
      () =>
        Effect.map(crash.run, ({ metrics, violations }) => {
          assert.deepStrictEqual(violations, [])
          assert.isAbove(Number(metrics.kills), 0)
        }),
      { timeout: 60_000 }
    )

    const startsBesideWriters = Effect.gen(function*() {
      const { target } = yield* temporaryDurable(backend)
      const open = () => openStorage(backend, target)
      // Creates the tables, so the starts below find them.
      yield* Effect.void.pipe(Effect.provide(open()))
      const writer = (aggregateId: string) =>
        Effect.gen(function*() {
          for (let version = 0;; version++) {
            yield* append(aggregateId, version, "A")
            yield* Effect.yieldNow
          }
        }).pipe(Effect.provide(open()), Effect.forkChild)
      const writers = [yield* writer("w1"), yield* writer("w2")]
      yield* Effect.sleep("200 millis")

      const starts = yield* Effect.forEach(range(20), () => Effect.exit(Effect.void.pipe(Effect.provide(open()))))
      const stopped = yield* Effect.forEach(
        writers,
        (fiber) => Effect.andThen(Fiber.interrupt(fiber), Fiber.await(fiber))
      )

      assert.strictEqual(starts.filter(Exit.isFailure).length, 0, "starts that failed")
      assert.isTrue(
        stopped.every((exit) => Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)),
        "a writer failed"
      )
    })

    it.live(verifies("ES-014", "starts beside processes that are writing"), () => startsBesideWriters, {
      timeout: 60_000
    })
  })
}
