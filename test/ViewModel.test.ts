import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import {
  DateTime,
  Duration,
  Effect,
  Fiber,
  Latch,
  Layer,
  Option,
  Ref,
  References,
  Schedule,
  Schema,
  Stream
} from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, SqliteEventStore, ViewModel } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { TestClock } from "effect/testing"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { listTodos, Todo, TodoList, TodoListStore } from "./fixtures/todo.ts"

const Storage = Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)

const TodoApp = ViewModel.layer([TodoList]).pipe(
  Layer.provideMerge(Layer.merge(Storage, TodoListStore.layer))
)

const titles = Effect.map(listTodos, (todos) => todos.map((todo) => todo.title).sort())

describe("ViewModels", () => {
  it.effect("projects new events", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const viewModels = yield* ViewModel.ViewModels

      yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      const result = yield* todos.commands.createTodo("b", { title: "Buy Eggs" })
      yield* viewModels.caughtUp(result.position)

      assert.deepStrictEqual(yield* titles, ["Buy Eggs", "Buy Milk"])
    }).pipe(Effect.provide(TodoApp)))

  it.effect("applies later events to earlier state, in order", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const viewModels = yield* ViewModel.ViewModels

      yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      yield* todos.commands.createTodo("b", { title: "Buy Eggs" })
      yield* todos.commands.completeTodo("a", {})
      yield* todos.commands.deleteTodo("b", { message: "Done" })
      yield* viewModels.caughtUp()

      assert.deepStrictEqual(yield* listTodos, [{ id: "a", title: "Buy Milk", completed: true }])
    }).pipe(Effect.provide(TodoApp)))

  it.effect("catches up on events stored before it started", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.commands.createTodo("a", { title: "Buy Milk" })

      yield* Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        yield* viewModels.caughtUp()
        assert.deepStrictEqual(yield* titles, ["Buy Milk"])
      }).pipe(Effect.provide(ViewModel.layer([TodoList])), Effect.scoped)
    }).pipe(Effect.provide(Layer.merge(Storage, TodoListStore.layer))))

  it.effect("resumes from the checkpoint instead of reprocessing", () =>
    Effect.gen(function*() {
      const seen = yield* Ref.make<ReadonlyArray<string>>([])
      const Tracker = ViewModel.make({
        name: "tracker",
        aggregates: [Todo],
        dispose: Ref.set(seen, []),
        handlers: {
          TodoCreated: ({ title }) => Ref.update(seen, (titles) => [...titles, title])
        }
      })
      const todos = yield* AggregateService.make(Todo)
      const run = ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(
        Effect.provide(ViewModel.layer([Tracker])),
        Effect.scoped
      )

      yield* todos.commands.createTodo("a", { title: "first" })
      yield* run
      yield* todos.commands.createTodo("b", { title: "second" })
      yield* run

      assert.deepStrictEqual(yield* Ref.get(seen), ["first", "second"])
    }).pipe(Effect.provide(Storage)))

  it.effect("counts skipped events as processed, so caughtUp returns", () => {
    const Partial = ViewModel.make({
      name: "partial",
      aggregates: [Todo],
      dispose: Effect.void,
      handlers: { TodoDeleted: () => Effect.void }
    })
    return Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const viewModels = yield* ViewModel.ViewModels

      const result = yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      yield* viewModels.caughtUp(result.position)

      const [status] = yield* viewModels.status
      assert.strictEqual(status?.position, result.position)
    }).pipe(Effect.provide(ViewModel.layer([Partial]).pipe(Layer.provideMerge(Storage))))
  })

  it.effect("combines events from several aggregates", () =>
    Effect.gen(function*() {
      const NoteEvent = Schema.TaggedUnion({ NoteAdded: { text: Schema.String } })
      const Note = Aggregate.make<number>()({
        name: "note",
        events: NoteEvent,
        initial: 0,
        projection: { NoteAdded: (count) => count + 1 }
      }).command("addNote", {
        payload: Schema.Struct({ text: Schema.String }),
        handler: (_, { text }) => Effect.succeed(NoteEvent.cases.NoteAdded.make({ text }))
      })
      const log = yield* Ref.make<ReadonlyArray<string>>([])
      const Activity = ViewModel.make({
        name: "activity",
        aggregates: [Todo, Note],
        dispose: Ref.set(log, []),
        handlers: {
          TodoCreated: ({ title }) => Ref.update(log, (lines) => [...lines, `todo ${title}`]),
          NoteAdded: ({ text }) => Ref.update(log, (lines) => [...lines, `note ${text}`])
        }
      })
      const todos = yield* AggregateService.make(Todo)
      const notes = yield* AggregateService.make(Note)

      yield* Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "Buy Milk" })
        yield* notes.commands.addNote("n", { text: "Remember" })
        yield* viewModels.caughtUp()
        assert.deepStrictEqual(yield* Ref.get(log), ["todo Buy Milk", "note Remember"])
      }).pipe(Effect.provide(ViewModel.layer([Activity])), Effect.scoped)
    }).pipe(Effect.provide(Storage)))

  describe("replay", () => {
    it.effect("rebuilds a view model from its events", () =>
      Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        const viewModels = yield* ViewModel.ViewModels
        const store = yield* TodoListStore
        yield* todos.commands.createTodo("a", { title: "Buy Milk" })
        yield* todos.commands.createTodo("b", { title: "Buy Eggs" })
        yield* viewModels.caughtUp()
        // A view model damaged by hand, like `todo-list.remove` in the moleculer walkthrough.
        yield* Ref.set(store, new Map())

        const result = yield* viewModels.replay({ viewModels: ["todo-list"] })

        assert.deepStrictEqual(result, { viewModels: ["todo-list"], eventCount: 2 })
        assert.deepStrictEqual(yield* titles, ["Buy Eggs", "Buy Milk"])
      }).pipe(Effect.provide(TodoApp)))

    it.effect("disposes the old data first", () =>
      Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        const viewModels = yield* ViewModel.ViewModels
        const store = yield* TodoListStore
        yield* todos.commands.createTodo("a", { title: "Buy Milk" })
        yield* viewModels.caughtUp()
        yield* Ref.set(store, new Map([["stray", { id: "stray", title: "Stray", completed: false }]]))

        yield* viewModels.replay({ viewModels: ["todo-list"] })

        assert.deepStrictEqual(yield* titles, ["Buy Milk"])
      }).pipe(Effect.provide(TodoApp)))

    it.effect("keeps following new events afterwards", () =>
      Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "Buy Milk" })
        yield* viewModels.replay({ viewModels: ["todo-list"] })

        const result = yield* todos.commands.createTodo("b", { title: "Buy Eggs" })
        yield* viewModels.caughtUp(result.position)

        assert.deepStrictEqual(yield* titles, ["Buy Eggs", "Buy Milk"])
      }).pipe(Effect.provide(TodoApp)))

    it.effect("limits the rebuild to a time window, then resumes from the present", () =>
      Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "Early" })
        yield* TestClock.adjust(Duration.seconds(1))
        const finishTime = yield* DateTime.now
        yield* todos.commands.createTodo("b", { title: "Late" })
        yield* viewModels.caughtUp()

        const result = yield* viewModels.replay({ viewModels: ["todo-list"], finishTime })
        yield* viewModels.caughtUp()

        assert.strictEqual(result.eventCount, 1)
        assert.deepStrictEqual(yield* titles, ["Early"])

        const next = yield* todos.commands.createTodo("c", { title: "New" })
        yield* viewModels.caughtUp(next.position)
        assert.deepStrictEqual(yield* titles, ["Early", "New"])
      }).pipe(Effect.provide(TodoApp)))

    it.effect("fails for a view model that isn't registered, without touching the others", () =>
      Effect.gen(function*() {
        const todos = yield* AggregateService.make(Todo)
        const viewModels = yield* ViewModel.ViewModels
        yield* todos.commands.createTodo("a", { title: "Buy Milk" })
        yield* viewModels.caughtUp()

        const error = yield* Effect.flip(viewModels.replay({ viewModels: ["todo-list", "nope"] }))

        assert.strictEqual(error._tag, "UnknownViewModel")
        assert.deepStrictEqual(yield* titles, ["Buy Milk"])
      }).pipe(Effect.provide(TodoApp)))

    it.effect("reports a failing handler", () =>
      Effect.gen(function*() {
        class Boom extends Schema.TaggedError<Boom>()("Boom", {}) {}
        const armed = yield* Ref.make(false)
        const Fragile = ViewModel.make({
          name: "fragile",
          aggregates: [Todo],
          dispose: Effect.void,
          handlers: {
            TodoCreated: () =>
              Effect.flatMap(Ref.get(armed), (isArmed) => isArmed ? Effect.fail(new Boom()) : Effect.void)
          }
        })
        const todos = yield* AggregateService.make(Todo)

        yield* Effect.gen(function*() {
          const viewModels = yield* ViewModel.ViewModels
          yield* todos.commands.createTodo("a", { title: "Buy Milk" })
          yield* viewModels.caughtUp()
          yield* Ref.set(armed, true)

          const error = yield* Effect.flip(viewModels.replay({ viewModels: ["fragile"] }))

          assert.strictEqual(error._tag, "ViewModelError")
          assert.strictEqual(error._tag === "ViewModelError" && error.position, 1)
        }).pipe(Effect.provide(ViewModel.layer([Fragile])), Effect.scoped)
      }).pipe(Effect.provide(Storage)))
  })

  describe("failing handlers", () => {
    const lastErrorOf = (viewModels: ViewModel.ViewModels["Service"]) =>
      Effect.map(viewModels.status, (statuses) => statuses[0]!.lastError)

    for (
      const [kind, failure] of [
        ["an error", Effect.fail("broken")],
        ["a defect", Effect.die("broken")]
      ] as const
    ) {
      it.live(`stalls on ${kind} and retries without skipping the event`, () =>
        Effect.gen(function*() {
          const attempts = yield* Ref.make(0)
          const processed = yield* Ref.make<ReadonlyArray<string>>([])
          const Flaky = ViewModel.make({
            name: "flaky",
            aggregates: [Todo],
            dispose: Effect.void,
            handlers: {
              TodoCreated: ({ title }) =>
                Ref.updateAndGet(attempts, (n) => n + 1).pipe(
                  Effect.flatMap((attempt) =>
                    attempt === 1 ? failure : Ref.update(processed, (titles) => [...titles, title])
                  )
                )
            }
          })
          const todos = yield* AggregateService.make(Todo)

          yield* Effect.gen(function*() {
            const viewModels = yield* ViewModel.ViewModels
            yield* todos.commands.createTodo("a", { title: "first" })
            yield* todos.commands.createTodo("b", { title: "second" })

            yield* viewModels.caughtUp()

            assert.deepStrictEqual(yield* Ref.get(processed), ["first", "second"])
            assert.strictEqual(yield* Ref.get(attempts), 3)
            yield* lastErrorOf(viewModels).pipe(
              Effect.repeat({ until: Option.isNone, schedule: Schedule.spaced("5 millis") }),
              Effect.timeout("2 seconds")
            )
          }).pipe(Effect.provide(ViewModel.layer([Flaky])), Effect.scoped)
        }).pipe(Effect.provide(Storage), Effect.provideService(References.MinimumLogLevel, "None")))
    }

    it.effect("starts again from the shortest delay after moving on", () =>
      Effect.gen(function*() {
        const failing = new Set([1, 501])
        const Flaky = ViewModel.make({
          name: "flaky",
          aggregates: [Todo],
          dispose: Effect.void,
          handlers: {
            TodoCreated: (_, { position }) => failing.delete(position) ? Effect.fail("broken") : Effect.void
          }
        })
        const todos = yield* AggregateService.make(Todo)
        for (let index = 0; index < 600; index++) {
          yield* todos.commands.createTodo(`todo-${index}`, { title: "Buy Milk" })
        }

        yield* Effect.gen(function*() {
          const viewModels = yield* ViewModel.ViewModels
          const failedAt = (position: number) =>
            lastErrorOf(viewModels).pipe(
              Effect.tap(() => Effect.yieldNow),
              Effect.repeat({ until: (error) => Option.exists(error, (error) => error.position === position) })
            )

          // Each failure waits for the same 100 millis, or `caughtUp` would never return.
          yield* failedAt(1)
          yield* TestClock.adjust("100 millis")
          yield* failedAt(501)
          yield* TestClock.adjust("100 millis")

          yield* viewModels.caughtUp()
        }).pipe(Effect.provide(ViewModel.layer([Flaky])), Effect.scoped)
      }).pipe(Effect.provide(Storage), Effect.provideService(References.MinimumLogLevel, "None")))

    it.live("exposes the failure while stalled", () =>
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

          const lastError = yield* lastErrorOf(viewModels).pipe(
            Effect.repeat({ until: Option.isSome, schedule: Schedule.spaced("5 millis") }),
            Effect.timeout("2 seconds")
          )

          assert.strictEqual(Option.getOrThrow(lastError).position, 1)
          assert.strictEqual((yield* viewModels.status)[0]?.position, 0)
          const timedOut = yield* viewModels.caughtUp().pipe(Effect.timeout("50 millis"), Effect.flip)
          assert.strictEqual(timedOut._tag, "TimeoutError")
        }).pipe(Effect.provide(ViewModel.layer([Stuck])), Effect.scoped)
      }).pipe(Effect.provide(Storage), Effect.provideService(References.MinimumLogLevel, "None")))

    it.live("parks an event on time while a retry of parked events fails beside it", () =>
      Effect.gen(function*() {
        const release = yield* Latch.make()
        const entered = yield* Latch.make()
        const calls = yield* Ref.make<ReadonlyArray<string>>([])
        const Parking = ViewModel.make({
          name: "parking",
          aggregates: [Todo],
          dispose: Effect.void,
          onFailure: ViewModel.park({ attempts: 1 }),
          handlers: {
            TodoCreated: ({ title }) =>
              Effect.gen(function*() {
                yield* Ref.update(calls, (all) => [...all, title])
                if (title === "old") return yield* Effect.fail("always")
                yield* entered.open
                yield* release.await
                return yield* Effect.fail("new")
              })
          }
        })
        // A store whose change stream takes a moment to stop, as one over a network
        // can, which gives the retry a turn between the failure and parking it.
        const SlowStop = Layer.effect(
          EventStore.EventStore,
          EventStore.EventStore.use((store) =>
            Effect.succeed(EventStore.EventStore.of({
              ...store,
              changes: store.changes.pipe(Stream.ensuring(Effect.sleep("5 millis")))
            }))
          )
        ).pipe(Layer.provide(EventStore.layerMemory))

        yield* Effect.gen(function*() {
          const todos = yield* AggregateService.make(Todo)
          const viewModels = yield* ViewModel.ViewModels
          const parkedCount = (count: number) =>
            viewModels.parked().pipe(
              Effect.repeat({ until: (all) => all.length >= count, schedule: Schedule.spaced("2 millis") }),
              Effect.timeout("2 seconds")
            )
          yield* todos.commands.createTodo("a", { title: "old" })
          yield* parkedCount(1)
          yield* todos.commands.createTodo("b", { title: "new" })
          yield* entered.await
          const retry = yield* Effect.forkChild(viewModels.retryParked({ viewModel: "parking" }))
          yield* Effect.sleep("20 millis")
          yield* release.open

          const parked = yield* parkedCount(2)
          yield* Fiber.join(retry)
          assert.deepStrictEqual(parked.find(({ position }) => position === 2)?.attempts, 1)
          assert.deepStrictEqual((yield* Ref.get(calls)).filter((title) => title === "new"), ["new"])
        }).pipe(
          Effect.provide(
            ViewModel.layer([Parking]).pipe(Layer.provideMerge(Layer.merge(SlowStop, Checkpoints.layerMemory)))
          ),
          Effect.scoped
        )
      }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))
  })

  describe("under load", () => {
    const Counter = ViewModel.make({
      name: "counter",
      aggregates: [Todo],
      dispose: Effect.void,
      handlers: { TodoCreated: () => Effect.void }
    })

    for (const concurrency of [32, "unbounded"] as const) {
      it.effect(`gets a turn during a burst of commands, with concurrency ${concurrency}`, () =>
        Effect.gen(function*() {
          const todos = yield* AggregateService.make(Todo)
          const viewModels = yield* ViewModel.ViewModels

          yield* Effect.forEach(
            Array.from({ length: 5_000 }, (_, index) => `todo-${index}`),
            (id) => todos.commands.createTodo(id, { title: "Buy Milk" }),
            { concurrency, discard: true }
          )

          const [status] = yield* viewModels.status
          assert.isAbove(status!.position, 0)
        }).pipe(Effect.provide(ViewModel.layer([Counter]).pipe(Layer.provideMerge(Storage)))))
    }

    it.effect("handles a backlog that built up while it was busy in batches of one page", () =>
      Effect.gen(function*() {
        const store = yield* EventStore.EventStore
        const blocked = yield* Latch.make(false)
        const batches: Array<number> = []
        let handled = 0
        const Slow = ViewModel.make({
          name: "slow",
          aggregates: [Todo],
          dispose: Effect.void,
          handlers: {
            TodoCreated: () => Effect.andThen(Effect.sync(() => void handled++), blocked.await)
          },
          // Counts the events of each batch, which share one checkpoint.
          transaction: (effect) =>
            Effect.suspend(() => {
              const before = handled
              return Effect.tap(effect, () => Effect.sync(() => void batches.push(handled - before)))
            })
        })
        const append = (index: number) =>
          store.append({
            aggregateName: "todo",
            aggregateId: `todo-${index}`,
            expectedVersion: 0,
            events: [{
              type: "TodoCreated",
              payload: { _tag: "TodoCreated", title: "Buy Milk", createdAt: "2026-01-01T00:00:00.000Z" }
            }]
          })

        yield* Effect.gen(function*() {
          const viewModels = yield* ViewModel.ViewModels
          yield* append(0)
          // The first event holds the view model while the backlog builds up.
          yield* Effect.yieldNow.pipe(Effect.repeat({ until: () => handled === 1 }))
          for (let index = 1; index < 3_000; index++) yield* append(index)
          yield* blocked.open
          yield* viewModels.caughtUp()
          assert.strictEqual(handled, 3_000)
          assert.isAtMost(Math.max(...batches), 500)
        }).pipe(Effect.provide(ViewModel.layer([Slow])), Effect.scoped)
      }).pipe(Effect.provide(Storage)))

    it.effect("saves checkpoints now and then, and where it got to when it stops", () =>
      Effect.gen(function*() {
        const saved: Array<number> = []
        const todos = yield* AggregateService.make(Todo)

        yield* Effect.gen(function*() {
          const viewModels = yield* ViewModel.ViewModels
          for (let index = 0; index < 1_250; index++) {
            const { position } = yield* todos.commands.createTodo(`todo-${index}`, { title: "Buy Milk" })
            yield* viewModels.caughtUp(position)
          }
          assert.deepStrictEqual(saved, [500, 1_000])
        }).pipe(
          Effect.provide(ViewModel.layer([Counter])),
          Effect.provideService(
            Checkpoints.Checkpoints,
            Checkpoints.Checkpoints.of({
              get: () => Effect.succeed(0),
              set: (_, position) => Effect.sync(() => void saved.push(position))
            })
          ),
          Effect.scoped
        )

        assert.deepStrictEqual(saved, [500, 1_000, 1_250])
      }).pipe(Effect.provide(EventStore.layerMemory)))
  })

  describe("in a transaction", () => {
    it.live("commits a batch and its checkpoint together, or neither", () =>
      Effect.gen(function*() {
        const filename = join(mkdtempSync(join(tmpdir(), "effect-cqrs-")), "events.sqlite")
        const storage = Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
          Layer.provideMerge(Layer.fresh(SqliteClient.layer({ filename })))
        )
        const broken = yield* Ref.make(true)
        const Rows = ViewModel.make({
          name: "rows",
          aggregates: [Todo],
          dispose: Effect.void,
          transaction: ViewModel.sqlTransaction,
          handlers: {
            TodoCreated: Effect.fnUntraced(function*({ title }, { aggregateId }) {
              const sql = yield* SqlClient.SqlClient
              // No `OR IGNORE`: an event handled twice fails here.
              yield* sql`INSERT INTO todo_rows (id, title) VALUES (${aggregateId}, ${title})`
              if (title === "bad" && (yield* Ref.get(broken))) return yield* Effect.fail("broken")
            })
          }
        })
        const inspect = Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          const rows = yield* sql<{ readonly id: string }>`SELECT id FROM todo_rows ORDER BY id`
          const checkpoint = yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.get("rows"))
          return { rows: rows.map(({ id }) => id), checkpoint }
        })
        const session = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          effect.pipe(Effect.provide(ViewModel.layer([Rows]).pipe(Layer.provideMerge(storage))), Effect.scoped)

        yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql`CREATE TABLE todo_rows (id TEXT PRIMARY KEY, title TEXT NOT NULL)`
          const todos = yield* AggregateService.make(Todo)
          for (const [id, title] of [["a", "one"], ["b", "two"], ["c", "bad"], ["d", "four"]]) {
            yield* todos.commands.createTodo(id!, { title: title! })
          }
        }).pipe(Effect.provide(storage), Effect.scoped)

        // The third event fails, and takes the first two and the checkpoint with it.
        yield* session(Effect.gen(function*() {
          const viewModels = yield* ViewModel.ViewModels
          yield* viewModels.status.pipe(
            Effect.repeat({
              until: ([status]) => Option.isSome(status!.lastError),
              schedule: Schedule.spaced("5 millis")
            }),
            Effect.timeout("2 seconds")
          )
          assert.deepStrictEqual(yield* inspect, { rows: [], checkpoint: 0 })
        }))
        assert.deepStrictEqual(yield* session(inspect), { rows: [], checkpoint: 0 })

        // After a restart it starts from the same place, and saves the checkpoint with the batch.
        yield* Ref.set(broken, false)
        yield* session(Effect.gen(function*() {
          yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp())
          assert.deepStrictEqual(yield* inspect, { rows: ["a", "b", "c", "d"], checkpoint: 4 })
        }))

        yield* session(Effect.gen(function*() {
          const todos = yield* AggregateService.make(Todo)
          const { position } = yield* todos.commands.createTodo("e", { title: "five" })
          yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp(position))
          assert.deepStrictEqual(yield* inspect, { rows: ["a", "b", "c", "d", "e"], checkpoint: 5 })
        }))
      }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))

    it.live("rolls a batch back when another process moved the checkpoint meanwhile", () =>
      Effect.gen(function*() {
        const Rows = ViewModel.make({
          name: "rows",
          aggregates: [Todo],
          dispose: Effect.void,
          transaction: ViewModel.sqlTransaction,
          handlers: {
            TodoCreated: Effect.fnUntraced(function*({ title }, { aggregateId }) {
              const sql = yield* SqlClient.SqlClient
              yield* sql`INSERT INTO todo_rows (id, title) VALUES (${aggregateId}, ${title})`
              // Another holder committed this batch while this one was handling it.
              yield* sql`UPDATE cqrs_checkpoints SET position = 99 WHERE view_model = 'rows'`
            })
          }
        })

        yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql`CREATE TABLE todo_rows (id TEXT PRIMARY KEY, title TEXT NOT NULL)`
          const checkpoints = yield* Checkpoints.Checkpoints
          yield* checkpoints.set("rows", 0)
          const todos = yield* AggregateService.make(Todo)
          yield* todos.commands.createTodo("a", { title: "one" })

          yield* Effect.gen(function*() {
            const viewModels = yield* ViewModel.ViewModels
            const [status] = yield* viewModels.status.pipe(
              Effect.repeat({
                until: ([status]) => Option.isSome(status!.lastError),
                schedule: Schedule.spaced("5 millis")
              }),
              Effect.timeout("2 seconds")
            )
            assert.include(String(Option.getOrThrow(status!.lastError).cause), "Another process moved")
            const rows = yield* sql<{ readonly id: string }>`SELECT id FROM todo_rows`
            assert.deepStrictEqual(rows, [])
            assert.strictEqual(yield* checkpoints.get("rows"), 0)
          }).pipe(Effect.provide(ViewModel.layer([Rows])), Effect.scoped)
        }).pipe(
          Effect.provide(
            Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
              Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))
            )
          )
        )
      }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))

    it.live("goes on from where another process moved the checkpoint to", () =>
      Effect.gen(function*() {
        const Rows = ViewModel.make({
          name: "rows",
          aggregates: [Todo],
          dispose: Effect.void,
          transaction: ViewModel.sqlTransaction,
          handlers: {
            TodoCreated: Effect.fnUntraced(function*({ title }, { aggregateId }) {
              const sql = yield* SqlClient.SqlClient
              yield* sql`INSERT OR REPLACE INTO todo_rows (id, title) VALUES (${aggregateId}, ${title})`
            })
          }
        })

        yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql`CREATE TABLE todo_rows (id TEXT PRIMARY KEY, title TEXT NOT NULL)`
          const checkpoints = yield* Checkpoints.Checkpoints
          const todos = yield* AggregateService.make(Todo)

          yield* Effect.gen(function*() {
            const viewModels = yield* ViewModel.ViewModels
            const first = yield* todos.commands.createTodo("a", { title: "one" })
            yield* viewModels.caughtUp(first.position)

            // Another holder of the lease saved a checkpoint this process doesn't know.
            yield* checkpoints.set("rows", 0)
            const second = yield* todos.commands.createTodo("b", { title: "two" })
            yield* viewModels.caughtUp(second.position).pipe(Effect.timeout("3 seconds"))

            const rows = yield* sql<{ readonly id: string }>`SELECT id FROM todo_rows ORDER BY id`
            assert.deepStrictEqual(rows.map(({ id }) => id), ["a", "b"])
            assert.strictEqual(yield* checkpoints.get("rows"), second.position)
            // The error is cleared right after the step that caught up.
            yield* viewModels.status.pipe(
              Effect.repeat({
                until: ([status]) => Option.isNone(status!.lastError),
                schedule: Schedule.spaced("5 millis")
              }),
              Effect.timeout("2 seconds")
            )
          }).pipe(Effect.provide(ViewModel.layer([Rows])), Effect.scoped)
        }).pipe(
          Effect.provide(
            Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
              Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))
            )
          )
        )
      }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))

    it.live("parks an event and commits the rest of its batch", () =>
      Effect.gen(function*() {
        const Rows = ViewModel.make({
          name: "rows",
          aggregates: [Todo],
          dispose: Effect.void,
          transaction: ViewModel.sqlTransaction,
          onFailure: ViewModel.park({ attempts: 1 }),
          handlers: {
            TodoCreated: Effect.fnUntraced(function*({ title }, { aggregateId }) {
              const sql = yield* SqlClient.SqlClient
              yield* sql`INSERT INTO todo_rows (id, title) VALUES (${aggregateId}, ${title})`
              if (title === "bad") return yield* Effect.fail("broken")
            })
          }
        })

        yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql`CREATE TABLE todo_rows (id TEXT PRIMARY KEY, title TEXT NOT NULL)`
          const todos = yield* AggregateService.make(Todo)
          for (const [id, title] of [["a", "one"], ["b", "bad"], ["c", "three"]]) {
            yield* todos.commands.createTodo(id!, { title: title! })
          }

          yield* Effect.gen(function*() {
            const viewModels = yield* ViewModel.ViewModels
            yield* viewModels.caughtUp()
            const rows = yield* sql<{ readonly id: string }>`SELECT id FROM todo_rows ORDER BY id`
            assert.deepStrictEqual(rows.map(({ id }) => id), ["a", "c"])
            assert.deepStrictEqual((yield* viewModels.parked()).map((event) => event.position), [2])
            assert.strictEqual(yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.get("rows")), 3)
          }).pipe(Effect.provide(ViewModel.layer([Rows])), Effect.scoped)
        }).pipe(
          Effect.provide(
            Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
              Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))
            )
          )
        )
      }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))
  })

  describe("on SQLite", () => {
    it.effect("persists checkpoints and rebuilds nothing on restart", () =>
      Effect.gen(function*() {
        const filename = join(mkdtempSync(join(tmpdir(), "effect-cqrs-")), "events.sqlite")
        const database = Layer.fresh(SqliteClient.layer({ filename }))
        const storage = Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
          Layer.provide(database)
        )
        const count = yield* Ref.make(0)
        const Counter = ViewModel.make({
          name: "counter",
          aggregates: [Todo],
          dispose: Ref.set(count, 0),
          handlers: { TodoCreated: () => Ref.update(count, (n) => n + 1) }
        })
        const session = (title: string, id: string) =>
          Effect.gen(function*() {
            const todos = yield* AggregateService.make(Todo)
            const viewModels = yield* ViewModel.ViewModels
            const result = yield* todos.commands.createTodo(id, { title })
            yield* viewModels.caughtUp(result.position)
          }).pipe(
            Effect.provide(ViewModel.layer([Counter]).pipe(Layer.provideMerge(storage))),
            Effect.scoped
          )

        yield* session("first", "a")
        yield* session("second", "b")

        assert.strictEqual(yield* Ref.get(count), 2)
      }))
  })

  it.effect("serves live subscribers from the same store", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const store = yield* EventStore.EventStore
      yield* todos.commands.createTodo("a", { title: "Buy Milk" })

      const first = yield* store.subscribe().pipe(Stream.take(1), Stream.runCollect)

      assert.strictEqual(first[0]?.type, "TodoCreated")
    }).pipe(Effect.provide(Storage)))
})
