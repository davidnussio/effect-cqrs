import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Layer, Ref, References, Schedule, Scope } from "effect"
import { AggregateService, Checkpoints, EventStore, Leases, ViewModel } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { postgresClient, postgresStorage, postgresUrl } from "../bench/common.ts"
import { Todo } from "../examples/todo/aggregate.ts"

const statusOf = ViewModel.ViewModels.use((viewModels) => Effect.map(viewModels.status, ([status]) => status!))

describe("Leases", () => {
  it.effect("are always held in memory, by default", () =>
    Effect.gen(function*() {
      const Counter = ViewModel.make({
        name: "counter",
        aggregates: [Todo],
        dispose: Effect.void,
        handlers: { TodoCreated: () => Effect.void }
      })
      const status = yield* statusOf.pipe(Effect.provide(ViewModel.layer([Counter])))
      assert.isTrue(status.holdsLease)
      assert.strictEqual(status.parked, 0)
    }).pipe(Effect.provide(Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory))))

  // Needs the database of `docker-compose.yml` and POSTGRES_URL.
  describe.skipIf(postgresUrl === undefined)("on PostgreSQL", () => {
    it.live("run each view model in one process, and hand it over when that one stops", () => {
      const table = `leases_${crypto.randomUUID().replaceAll("-", "")}`
      return Effect.gen(function*() {
        const counts = { first: yield* Ref.make(0), second: yield* Ref.make(0) }
        const makeProcess = (name: keyof typeof counts) => {
          const Counter = ViewModel.make({
            name: `counter-${table}`,
            aggregates: [Todo],
            dispose: Effect.void,
            handlers: { TodoCreated: () => Ref.update(counts[name], (n) => n + 1) }
          })
          return ViewModel.layer([Counter]).pipe(
            Layer.provideMerge(Layer.merge(postgresStorage(table), Leases.layerPostgres({ interval: "50 millis" }))),
            Layer.provideMerge(Layer.fresh(postgresClient()))
          )
        }
        // Two processes, each with a client of its own.
        const start = Effect.fnUntraced(function*(name: keyof typeof counts) {
          const scope = yield* Scope.make()
          const context = yield* Layer.buildWithScope(makeProcess(name), scope)
          return {
            run: <A, E>(effect: Effect.Effect<A, E, any>) =>
              Effect.provideContext(effect, context) as Effect.Effect<A, E>,
            stop: Scope.close(scope, Exit.void)
          }
        })
        const createTodos = (ids: ReadonlyArray<string>) =>
          Effect.gen(function*() {
            const todos = yield* AggregateService.make(Todo)
            let position = 0
            for (const id of ids) position = (yield* todos.commands.createTodo(id, { title: id })).position
            return position
          })

        const first = yield* start("first")
        const second = yield* start("second")
        yield* Effect.addFinalizer(() =>
          Effect.andThen(first.stop, second.stop).pipe(
            Effect.andThen(SqlClient.SqlClient.use((sql) =>
              sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`)
            )),
            Effect.provide(Layer.fresh(postgresClient())),
            Effect.ignore
          )
        )

        assert.isTrue((yield* first.run(statusOf)).holdsLease)
        assert.isFalse((yield* second.run(statusOf)).holdsLease)

        const before = yield* first.run(createTodos(["a", "b", "c"]))
        yield* first.run(ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp(before)))
        // A standby doesn't wait for a view model it doesn't run, nor refuses to answer.
        yield* second.run(ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp(before))).pipe(
          Effect.timeout("1 second")
        )
        const refused = yield* Effect.flip(
          second.run(ViewModel.ViewModels.use((viewModels) => viewModels.replay({ viewModels: [`counter-${table}`] })))
        )
        assert.strictEqual(refused._tag, "ViewModelError")

        yield* first.stop
        yield* second.run(statusOf).pipe(
          Effect.repeat({ until: (status) => status.holdsLease, schedule: Schedule.spaced("10 millis") }),
          Effect.timeout("5 seconds")
        )
        const after = yield* second.run(createTodos(["d", "e"]))
        yield* second.run(ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp(after)))

        // The second process starts where the first one stopped.
        assert.strictEqual(yield* Ref.get(counts.first), 3)
        assert.strictEqual(yield* Ref.get(counts.second), 2)
        assert.strictEqual((yield* second.run(statusOf)).position, after)
      }).pipe(Effect.scoped, Effect.provideService(References.MinimumLogLevel, "None"))
    })

    it.live("lets a standby take over when the holder's connection dies", () => {
      const table = `leases_${crypto.randomUUID().replaceAll("-", "")}`
      return Effect.gen(function*() {
        const leases = (scope: Scope.Scope) =>
          Layer.buildWithScope(
            Leases.layerPostgres({ interval: "50 millis" }).pipe(Layer.provideMerge(Layer.fresh(postgresClient()))),
            scope
          )
        const scope = yield* Effect.scope
        const holder = yield* leases(scope)
        const standby = yield* leases(scope)
        const lease = (context: typeof holder) =>
          Leases.Leases.use((leases) => leases.acquire(table)).pipe(Effect.provideContext(context))

        const held = yield* lease(holder)
        const waiting = yield* lease(standby)
        assert.isTrue(yield* held.held)
        assert.isFalse(yield* waiting.held)

        // Ends the session that holds this lease's lock, as if its process died. Only
        // that lock: other tests running at the same time hold advisory locks too. A
        // bigint advisory lock is stored as its high and low halves.
        yield* SqlClient.SqlClient.use((sql) =>
          sql`
            WITH lock AS (SELECT hashtextextended(${`effect-cqrs/${table}`}, 0) AS key)
            SELECT pg_terminate_backend(pid) FROM pg_locks, lock
            WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND classid = ((key >> 32) & 4294967295)::oid
              AND objid = (key & 4294967295)::oid
              AND objsubid = 1
          `
        ).pipe(Effect.provideContext(standby))

        yield* waiting.held.pipe(
          Effect.repeat({ until: (isHeld) => isHeld, schedule: Schedule.spaced("10 millis") }),
          Effect.timeout("5 seconds")
        )
        yield* held.held.pipe(
          Effect.repeat({ until: (isHeld) => !isHeld, schedule: Schedule.spaced("10 millis") }),
          Effect.timeout("5 seconds")
        )
      }).pipe(Effect.scoped)
    })
  })
})
