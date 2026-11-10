import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect, Layer, Metric, Option, Ref, References, Schedule } from "effect"
import { AggregateService, Checkpoints, EventStore, ParkedEvents, SqliteEventStore, ViewModel } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { postgresClient, postgresUrl } from "../bench/common.ts"
import { Todo } from "../examples/todo/aggregate.ts"

const Storage = Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)

/** A view model that fails on titles in `broken`, parking them after two attempts. */
const makeFragile = Effect.gen(function*() {
  const broken = yield* Ref.make(new Set(["bad"]))
  const seen = yield* Ref.make<ReadonlyArray<string>>([])
  const Fragile = ViewModel.make({
    name: "fragile",
    aggregates: [Todo],
    dispose: Ref.set(seen, []),
    onFailure: ViewModel.park({ attempts: 2 }),
    handlers: {
      TodoCreated: ({ title }) =>
        Ref.get(broken).pipe(
          Effect.flatMap((titles) =>
            titles.has(title) ? Effect.fail(`cannot take ${title}`) : Ref.update(seen, (all) => [...all, title])
          )
        )
    }
  })
  return { broken, seen, Fragile }
})

const parkedCount = Effect.map(Metric.snapshot, (snapshots) =>
  snapshots
    .filter((snapshot) =>
      snapshot.id === ViewModel.MetricNames.parkedEvents && snapshot.attributes?.view_model === "fragile"
    )
    .reduce((sum, { state }) => sum + Number((state as { readonly count: number | bigint }).count), 0))

const quiet = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provideService(References.MinimumLogLevel, "None"),
    Effect.provideService(Metric.MetricRegistry, new Map())
  )

describe("parking", () => {
  it.live("parks an event that keeps failing and moves on", () =>
    Effect.gen(function*() {
      const { Fragile, seen } = yield* makeFragile
      const todos = yield* AggregateService.make(Todo)

      yield* Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "first" })
        const bad = yield* todos.commands.createTodo("b", { title: "bad" })
        yield* todos.commands.createTodo("c", { title: "last" })

        yield* viewModels.caughtUp().pipe(Effect.timeout("5 seconds"))

        assert.deepStrictEqual(yield* Ref.get(seen), ["first", "last"])
        const [status] = yield* viewModels.status
        assert.strictEqual(status?.parked, 1)
        assert.isTrue(Option.isNone(status!.lastError))
        const [parked] = yield* viewModels.parked("fragile")
        assert.strictEqual(parked?.position, bad.position)
        assert.strictEqual(parked?.eventId, "todo/b@1")
        assert.strictEqual(parked?.attempts, 2)
        assert.include(parked?.error, "cannot take bad")
        assert.strictEqual(yield* parkedCount, 1)
      }).pipe(Effect.provide(ViewModel.layer([Fragile])), Effect.scoped)
    }).pipe(Effect.provide(Storage), quiet))

  it.live("retries parked events, keeping those that fail again", () =>
    Effect.gen(function*() {
      const { Fragile, broken, seen } = yield* makeFragile
      const todos = yield* AggregateService.make(Todo)
      yield* Ref.set(broken, new Set(["bad", "worse"]))

      yield* Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        const bad = yield* todos.commands.createTodo("b", { title: "bad" })
        const worse = yield* todos.commands.createTodo("w", { title: "worse" })
        yield* viewModels.caughtUp().pipe(Effect.timeout("5 seconds"))
        assert.lengthOf(yield* viewModels.parked(), 2)

        yield* Ref.set(broken, new Set(["worse"]))
        const result = yield* viewModels.retryParked({ viewModel: "fragile" })

        assert.deepStrictEqual(result.succeeded, [bad.position])
        assert.deepStrictEqual(result.failed.map((event) => [event.position, event.attempts]), [[worse.position, 3]])
        assert.deepStrictEqual(yield* Ref.get(seen), ["bad"])
        assert.deepStrictEqual((yield* viewModels.parked()).map((event) => event.position), [worse.position])
        assert.strictEqual((yield* viewModels.status)[0]?.parked, 1)

        yield* Ref.set(broken, new Set())
        const again = yield* viewModels.retryParked({ viewModel: "fragile", positions: [worse.position] })
        assert.deepStrictEqual(again.succeeded, [worse.position])
        assert.lengthOf(yield* viewModels.parked(), 0)
        assert.strictEqual((yield* viewModels.status)[0]?.parked, 0)
      }).pipe(Effect.provide(ViewModel.layer([Fragile])), Effect.scoped)
    }).pipe(Effect.provide(Storage), quiet))

  it.live("skips events it parked before a restart", () =>
    Effect.gen(function*() {
      const { Fragile, seen } = yield* makeFragile
      const filename = join(mkdtempSync(join(tmpdir(), "effect-cqrs-")), "events.sqlite")
      const storage = Layer.mergeAll(
        SqliteEventStore.layer(),
        SqliteEventStore.layerCheckpoints(),
        ParkedEvents.layerSql()
      ).pipe(Layer.provide(Layer.fresh(SqliteClient.layer({ filename }))))
      const handled = Effect.gen(function*() {
        yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp().pipe(Effect.timeout("5 seconds")))
      }).pipe(Effect.provide(ViewModel.layer([Fragile]).pipe(Layer.provideMerge(storage))), Effect.scoped)

      yield* Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        yield* todos.commands.createTodo("b", { title: "bad" })
        yield* todos.commands.createTodo("c", { title: "good" })
      }).pipe(Effect.provide(storage), Effect.scoped)
      yield* handled
      yield* Ref.set(seen, [])
      // Forgets the checkpoint, so the restart reads both events again.
      yield* Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        yield* sql`DELETE FROM cqrs_checkpoints`
      }).pipe(Effect.provide(Layer.fresh(SqliteClient.layer({ filename }))), Effect.scoped)
      yield* handled

      assert.deepStrictEqual(yield* Ref.get(seen), ["good"])
      const parked = yield* ParkedEvents.ParkedEvents.use((store) => store.list()).pipe(
        Effect.provide(storage),
        Effect.scoped
      )
      assert.deepStrictEqual(parked.map((event) => [event.viewModel, event.attempts]), [["fragile", 2]])
    }).pipe(quiet))

  it.live("stalls by default, retrying the same event", () =>
    Effect.gen(function*() {
      const Stuck = ViewModel.make({
        name: "stuck",
        aggregates: [Todo],
        dispose: Effect.void,
        handlers: { TodoCreated: () => Effect.fail("broken") }
      })
      const todos = yield* AggregateService.make(Todo)

      yield* Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "first" })
        yield* viewModels.status.pipe(
          Effect.repeat({
            until: ([status]) => Option.isSome(status!.lastError),
            schedule: Schedule.spaced("5 millis")
          }),
          Effect.timeout("2 seconds")
        )
        // Long enough for more than two attempts.
        yield* Effect.sleep("400 millis")
        assert.lengthOf(yield* viewModels.parked(), 0)
        assert.strictEqual((yield* viewModels.status)[0]?.position, 0)
      }).pipe(Effect.provide(ViewModel.layer([Stuck])), Effect.scoped)
    }).pipe(Effect.provide(Storage), quiet))

  it.effect("fails for a view model that isn't registered", () =>
    Effect.gen(function*() {
      const { Fragile } = yield* makeFragile
      const error = yield* ViewModel.ViewModels.use((viewModels) => viewModels.retryParked({ viewModel: "nope" })).pipe(
        Effect.provide(ViewModel.layer([Fragile])),
        Effect.scoped,
        Effect.flip
      )
      assert.strictEqual(error._tag, "UnknownViewModel")
    }).pipe(Effect.provide(Storage)))
})

const sampleEvents = Effect.gen(function*() {
  const parkedAt = DateTime.makeUnsafe(1_700_000_000_000)
  const event = (viewModel: string, position: number): ParkedEvents.ParkedEvent => ({
    viewModel,
    position,
    eventId: `todo/${position}@1`,
    error: "Error: broken",
    attempts: 3,
    parkedAt
  })
  return [event("b", 7), event("a", 9), event("a", 4)]
})

const behavesAsAStore = (layer: Layer.Layer<ParkedEvents.ParkedEvents, unknown>) =>
  Effect.gen(function*() {
    const store = yield* ParkedEvents.ParkedEvents
    const events = yield* sampleEvents
    yield* Effect.forEach(events, store.park, { discard: true })
    // Parking again replaces the record.
    yield* store.park({ ...events[0]!, attempts: 4 })

    assert.deepStrictEqual(
      (yield* store.list()).map((event) => [event.viewModel, event.position, event.attempts]),
      [["a", 4, 3], ["a", 9, 3], ["b", 7, 4]]
    )
    assert.deepStrictEqual(yield* store.list("a"), [events[2], events[1]])

    yield* store.remove("a", 4)
    assert.deepStrictEqual((yield* store.list("a")).map((event) => event.position), [9])
  }).pipe(Effect.provide(layer))

describe("ParkedEvents", () => {
  it.effect("in memory", () => behavesAsAStore(ParkedEvents.layerMemory))

  it.effect("on SQLite", () =>
    behavesAsAStore(ParkedEvents.layerSql().pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" })))))

  it.effect("leaves the table to the application when it doesn't migrate", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const missing = yield* Effect.flip(
        ParkedEvents.makeSql({ table: "parked", migrate: false }).pipe(Effect.flatMap((store) => store.list()))
      )
      assert.strictEqual(missing._tag, "EventStoreError")

      yield* Effect.forEach(
        ParkedEvents.migrations({ table: "parked" }).flatMap(({ statements }) => statements),
        (statement) => sql.unsafe(statement),
        {
          discard: true
        }
      )
      const store = yield* ParkedEvents.makeSql({ table: "parked", migrate: false })
      assert.deepStrictEqual(yield* store.list(), [])
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))))

  // Needs the database of `docker-compose.yml` and POSTGRES_URL.
  it.live.skipIf(postgresUrl === undefined)("on PostgreSQL", () => {
    const table = `parked_${crypto.randomUUID().replaceAll("-", "")}`
    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Effect.addFinalizer(() => sql.unsafe(`DROP TABLE IF EXISTS ${table}`).pipe(Effect.ignore))
      yield* behavesAsAStore(
        ParkedEvents.layerSql({ table }).pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)))
      )
    }).pipe(Effect.scoped, Effect.provide(Layer.fresh(postgresClient())))
  })
})
