import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Fiber, Latch, Layer, Option, Stream } from "effect"
import { Checkpoints, EventStore, PostgresEventStore } from "effect-cqrs"
import { SqlClient } from "effect/sql"
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

  it.live("announces an append in the caller's transaction once it commits, and not if it rolls back", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const store = yield* EventStore.EventStore
      const following = yield* store.subscribe().pipe(Stream.take(3), Stream.runCollect, Effect.forkChild)
      yield* Effect.sleep("20 millis")

      // The subscriber gets to look while each transaction is still open.
      const open = Effect.sleep("20 millis")
      yield* Effect.exit(
        append("lost", 0, "Lost").pipe(
          Effect.andThen(open),
          Effect.andThen(Effect.fail("rolled back")),
          sql.withTransaction
        )
      )
      yield* Effect.sleep("20 millis")
      yield* append("a", 0, "A").pipe(Effect.andThen(open), sql.withTransaction)
      yield* append("b", 0, "B")
      yield* append("c", 0, "C")

      const seen = yield* Fiber.join(following).pipe(Effect.timeoutOption("2 seconds"))
      assert.deepStrictEqual(
        Option.map(seen, (events) => events.map((event) => event.type)),
        Option.some(["A", "B", "C"])
      )
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))

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
        sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`).pipe(
          Effect.ignore
        )
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
      yield* sql.unsafe(
        `CREATE TABLE ${table}_events_head (only_row BOOLEAN PRIMARY KEY DEFAULT TRUE, position BIGINT NOT NULL)`
      )
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
      const tail = store.read({
        aggregateNames: ["test"],
        aggregateIds: ["long"],
        afterPosition: 2000,
        afterVersion: 2000
      })
      assert.strictEqual((yield* Stream.runCollect(tail)).length, 0)
    }).pipe(Effect.provide(storage)))

  it.live("wakes a subscriber of another process through a notification", () =>
    Effect.gen(function*() {
      const [writer, reader] = yield* twoProcesses({})

      const fiber = yield* reader.subscribe().pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
      yield* writer.append({
        aggregateName: "test",
        aggregateId: "a",
        expectedVersion: 0,
        events: [{ type: "A1", payload: null }, { type: "A2", payload: null }]
      })

      const events = yield* Fiber.join(fiber).pipe(Effect.timeout("5 seconds"))
      assert.deepStrictEqual(events.map((event) => event.type), ["A1", "A2"])
    }).pipe(Effect.scoped))

  it.live("polls for appends of another process when it doesn't listen", () =>
    Effect.gen(function*() {
      const [writer, reader] = yield* twoProcesses({ listen: false, pollInterval: "20 millis" })

      const fiber = yield* reader.subscribe().pipe(Stream.take(1), Stream.runCollect, Effect.forkChild)
      yield* writer.append({
        aggregateName: "test",
        aggregateId: "a",
        expectedVersion: 0,
        events: [{ type: "A", payload: null }]
      })

      const events = yield* Fiber.join(fiber).pipe(Effect.timeout("5 seconds"))
      assert.deepStrictEqual(events.map((event) => event.type), ["A"])
    }).pipe(Effect.scoped))

  it.live("writes the rest of a group when another process took one of its streams", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const events = sql(`${table}_events`)

      // Another process has written to `s1` and not committed yet: the group's version
      // check can't see it, but its insert waits for it and then breaks the constraint.
      const inserted = yield* Latch.make()
      const release = yield* Latch.make()
      const other = yield* sql`
        INSERT INTO ${events} (position, aggregate_name, aggregate_id, version, type, payload, "timestamp")
        VALUES (1000, 'test', 's1', 1, 'X', '{}', 0)
      `.pipe(Effect.andThen(inserted.open), Effect.andThen(release.await), sql.withTransaction, Effect.forkChild)
      yield* inserted.await

      // `s0` is written alone, and `s1` and `s2` wait for it and go together.
      const writes = yield* Effect.forEach(["s0", "s1", "s2"], (id) => Effect.exit(append(id, 0, "A")), {
        concurrency: "unbounded"
      }).pipe(Effect.forkChild)
      yield* Effect.sleep("200 millis")
      yield* release.open
      yield* Fiber.join(other)
      const [s0, s1, s2] = yield* Fiber.join(writes)

      assert.isTrue(Exit.isSuccess(s0!))
      assert.deepStrictEqual(
        Exit.findErrorOption(s1!).pipe(Option.map((error) => error._tag)),
        Option.some("ConcurrencyConflict")
      )
      assert.deepStrictEqual(Exit.isSuccess(s2!) ? s2.value.map((event) => event.position) : [], [2])
    }).pipe(Effect.provide(storage)))

  it.live("creates no tables with migrate off, and runs on the ones `migrations` describes", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const name = `managed_${crypto.randomUUID().replaceAll("-", "")}`
      const options = { eventsTable: `${name}_events`, checkpointsTable: `${name}_checkpoints`, migrate: false }
      const storage = Layer.merge(PostgresEventStore.layer(options), PostgresEventStore.layerCheckpoints(options))
      yield* Effect.addFinalizer(() =>
        sql.unsafe(`DROP TABLE IF EXISTS ${name}_events, ${name}_events_head, ${name}_checkpoints`).pipe(Effect.ignore)
      )

      const failure = yield* Effect.flip(Effect.provide(Effect.void, storage))
      assert.strictEqual(failure._tag, "EventStoreError")
      const tables = yield* sql<{ readonly name: string }>`
        SELECT tablename AS name FROM pg_tables WHERE tablename LIKE ${`${name}%`}
      `
      assert.deepStrictEqual(tables, [])

      for (const migration of PostgresEventStore.migrations(options)) {
        for (const statement of migration.statements) yield* sql.unsafe(statement)
      }

      yield* Effect.gen(function*() {
        yield* append("a", 0, "A")
        yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.set("view", 1))
        assert.deepStrictEqual(yield* positions, [1])
      }).pipe(Effect.provide(storage))
    }).pipe(Effect.scoped, Effect.provide(Layer.fresh(postgresClient()))))

  it.live("starts on current tables without waiting for a long transaction", () =>
    Effect.gen(function*() {
      const name = `lock_${crypto.randomUUID().replaceAll("-", "")}`
      const sql = yield* SqlClient.SqlClient
      yield* Effect.addFinalizer(() =>
        sql.unsafe(`DROP TABLE IF EXISTS ${name}_events, ${name}_events_head, ${name}_checkpoints`).pipe(Effect.ignore)
      )

      yield* Effect.gen(function*() {
        yield* append("a", 0, "A")

        // A transaction that read the table and stays open, like a report or a dump.
        const release = yield* Latch.make()
        const open = yield* sql.unsafe(`SELECT 1 FROM ${name}_events LIMIT 1`).pipe(
          Effect.andThen(release.await),
          sql.withTransaction,
          Effect.forkChild
        )
        yield* Effect.sleep("50 millis")

        // Another process starts on the same tables, while this one goes on.
        const started = yield* Layer.build(
          Layer.fresh(postgresStorage(name).pipe(Layer.provide(Layer.fresh(postgresClient()))))
        ).pipe(Effect.timeoutOption("2 seconds"))
        assert.isTrue(Option.isSome(started))
        const appended = yield* append("b", 0, "B").pipe(Effect.timeoutOption("1 second"))
        assert.isTrue(Option.isSome(appended))
        assert.deepStrictEqual(yield* positions, [1, 2])

        yield* release.open
        yield* Fiber.join(open)
      }).pipe(Effect.provide(postgresStorage(name)))
    }).pipe(Effect.scoped, Effect.provide(Layer.fresh(postgresClient()))))

  it.live("takes a batch larger than one statement can carry", () =>
    Effect.gen(function*() {
      const total = 12_000

      yield* append("a", 0, ...Array.from({ length: total }, () => "A"))

      assert.deepStrictEqual(yield* positions, upTo(total))
    }).pipe(Effect.provide(Layer.suspend(temporaryPostgres))))
})

const table = `test_${crypto.randomUUID().replaceAll("-", "")}`

/** Two stores on the same tables, each with a client of its own, as two processes have. */
const twoProcesses = Effect.fnUntraced(function*(options: PostgresEventStore.Options) {
  const name = `shared_${crypto.randomUUID().replaceAll("-", "")}`
  const admin = yield* Layer.build(Layer.fresh(postgresClient()))
  yield* Effect.addFinalizer(() =>
    Context.get(admin, SqlClient.SqlClient).unsafe(`DROP TABLE IF EXISTS ${name}_events, ${name}_events_head`).pipe(
      Effect.ignore
    )
  )
  const open = () =>
    Layer.build(
      PostgresEventStore.layer({ ...options, eventsTable: `${name}_events` }).pipe(
        Layer.provide(Layer.fresh(postgresClient()))
      )
    ).pipe(Effect.map(Context.get(EventStore.EventStore)))
  return [yield* open(), yield* open()] as const
})

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
