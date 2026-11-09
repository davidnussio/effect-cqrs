import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Latch, Layer, Option, Stream } from "effect"
import { SqlClient } from "effect/sql"
import { Checkpoints, EventStore } from "effect-cqrs"
import { postgresClient, postgresStorage, postgresUrl, temporaryPostgres } from "../bench/common.ts"

const append = (aggregateId: string, expectedVersion: number, ...types: ReadonlyArray<string>) =>
  EventStore.EventStore.use((store) =>
    store.append({
      aggregateName: "test",
      aggregateId,
      expectedVersion,
      events: types.map((type) => ({ type, payload: { type } }))
    })
  )

const positions = EventStore.EventStore.use((store) =>
  Effect.map(Stream.runCollect(store.read()), (events) => events.map((event) => event.position))
)

const upTo = (count: number) => Array.from({ length: count }, (_, index) => index + 1)

// Needs the database of `docker-compose.yml` and POSTGRES_URL.
describe.skipIf(postgresUrl === undefined)("PostgresEventStore", () => {
  it.live("numbers concurrent transactions with no gaps", () =>
    Effect.gen(function*() {
      yield* Effect.forEach(upTo(200), (index) => append(`stream-${index}`, 0, "A", "B"), { concurrency: 32 })

      assert.deepStrictEqual(yield* positions, upTo(400))
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))

  it.live("makes a later writer wait, so a reader can't pass an open transaction", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const store = yield* EventStore.EventStore
      const counter = sql(`${table}_events_head`)

      // Holds the counter the way an append in progress does.
      const release = yield* Latch.make()
      const open = yield* sql`UPDATE ${counter} SET position = position`.pipe(
        Effect.andThen(release.await),
        sql.withTransaction,
        Effect.forkChild
      )
      yield* Effect.sleep("50 millis")

      const waiting = yield* Effect.forkChild(append("a", 0, "A"))
      yield* Effect.sleep("100 millis")
      assert.strictEqual(yield* store.head, 0)
      assert.isTrue(Option.isNone(yield* Fiber.await(waiting).pipe(Effect.timeoutOption("10 millis"))))

      yield* release.open
      yield* Fiber.join(open)
      yield* Fiber.join(waiting)

      assert.deepStrictEqual(yield* positions, [1])
    }).pipe(Effect.provide(storage)))

  it.live("gives the position back when a transaction fails", () =>
    Effect.gen(function*() {
      yield* append("a", 0, "A")

      const failed = yield* Effect.exit(
        EventStore.EventStore.use((store) =>
          store.append({
            aggregateName: "test",
            aggregateId: "b",
            expectedVersion: 0,
            events: [{ type: "B", payload: null }, { type: null as unknown as string, payload: null }]
          })
        )
      )
      yield* append("c", 0, "C")

      assert.isTrue(Exit.isFailure(failed))
      assert.deepStrictEqual(yield* positions, [1, 2])
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))

  it.live("lets one of two writers win a stream, on separate connections", () =>
    Effect.gen(function*() {
      const exits = yield* Effect.forEach(
        upTo(16),
        () => Effect.exit(append("contested", 0, "A", "B")),
        { concurrency: "unbounded" }
      )

      const failures = exits.flatMap((exit) => Exit.isFailure(exit) ? [exit] : [])
      assert.strictEqual(failures.length, 15)
      for (const failure of failures) {
        assert.isTrue(Exit.hasFails(failure))
      }
      assert.deepStrictEqual(yield* positions, [1, 2])
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))

  it.live("adds the new columns to a table from before they existed", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const table = `old_${crypto.randomUUID().replaceAll("-", "")}`
      yield* Effect.addFinalizer(() =>
        sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`).pipe(Effect.ignore)
      )
      yield* sql.unsafe(`
        CREATE TABLE ${table}_events (
          position BIGINT PRIMARY KEY,
          aggregate_name TEXT NOT NULL,
          aggregate_id TEXT NOT NULL,
          version INTEGER NOT NULL,
          type TEXT NOT NULL,
          payload JSONB NOT NULL,
          "timestamp" BIGINT NOT NULL,
          UNIQUE (aggregate_name, aggregate_id, version)
        )
      `)
      yield* sql.unsafe(`INSERT INTO ${table}_events VALUES (1, 'test', 'a', 1, 'A1', '{}', 0)`)
      yield* sql.unsafe(`CREATE TABLE ${table}_events_head (only_row BOOLEAN PRIMARY KEY DEFAULT TRUE, position BIGINT NOT NULL)`)
      yield* sql.unsafe(`INSERT INTO ${table}_events_head (position) VALUES (1)`)

      yield* Effect.gen(function*() {
        yield* append("a", 1, "A2")
        const events = yield* EventStore.EventStore.use((store) => Stream.runCollect(store.read()))

        assert.deepStrictEqual(
          events.map((event) => [event.type, event.schemaVersion, event.correlationId, event.metadata]),
          [
            ["A1", 1, null, {}],
            ["A2", 1, null, {}]
          ]
        )
      }).pipe(Effect.provide(postgresStorage(table)))
      // The tables are dropped before the client closes.
    }).pipe(Effect.scoped, Effect.provide(Layer.fresh(postgresClient()))))

  it.live("keeps events and checkpoints across restarts, and numbers on from them", () =>
    Effect.gen(function*() {
      const reopened = Layer.fresh(postgresStorage(table).pipe(Layer.provide(Layer.fresh(postgresClient()))))

      yield* Effect.gen(function*() {
        yield* append("a", 0, "A")
        yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.set("view", 7))
      }).pipe(Effect.provide(reopened))

      yield* Effect.gen(function*() {
        assert.strictEqual(yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.get("view")), 7)
        const [event] = yield* append("a", 1, "B")
        assert.strictEqual(event?.position, 2)
      }).pipe(Effect.provide(reopened))
    }).pipe(Effect.provide(storage)))

  it.live("finds nothing new in a long stream without walking the log after it", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const store = yield* EventStore.EventStore
      const events = `${table}_events`
      const filler = Array.from({ length: 2_000 }, () => "B")
      yield* append("long", 0, ...Array.from({ length: 2_000 }, () => "A"))
      for (let stream = 0; stream < 10; stream++) yield* append(`filler-${stream}`, 0, ...filler)
      // With statistics, a stream this big is what tempts the planner to the primary key.
      yield* sql.unsafe(`ANALYZE ${events}`)

      const plan = yield* sql.unsafe<Record<string, string>>(
        `EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM ${events}
         WHERE position > 2000 AND version > 2000 AND aggregate_name IN ('test') AND aggregate_id IN ('long')
         ORDER BY version LIMIT 500`
      )

      const lines = plan.map((row) => Object.values(row)[0]!).join("\n")
      assert.notInclude(lines, "Rows Removed by Filter")
      assert.notInclude(lines, "_pkey")
      const tail = store.read({ aggregateNames: ["test"], aggregateIds: ["long"], afterPosition: 2000, afterVersion: 2000 })
      assert.strictEqual((yield* Stream.runCollect(tail)).length, 0)
    }).pipe(Effect.provide(storage)))

  it.live("takes a batch larger than one statement can carry", () =>
    Effect.gen(function*() {
      const total = 12_000

      yield* append("a", 0, ...Array.from({ length: total }, () => "A"))

      assert.deepStrictEqual(yield* positions, upTo(total))
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))
})

const table = `test_${crypto.randomUUID().replaceAll("-", "")}`

/** A store whose table name the test can see. */
const storage = Layer.suspend(() =>
  Layer.effectDiscard(
    Effect.addFinalizer(() =>
      SqlClient.SqlClient.use((sql) =>
        sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`)
      ).pipe(Effect.ignore)
    )
  ).pipe(Layer.provideMerge(postgresStorage(table)), Layer.provideMerge(Layer.fresh(postgresClient())))
)
