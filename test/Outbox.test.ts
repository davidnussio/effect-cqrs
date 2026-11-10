import { assert, describe, it } from "@effect/vitest"
import { Deferred, Effect, Layer, Ref, References } from "effect"
import { AggregateService, Checkpoints, EventStore, Outbox } from "effect-cqrs"
import { Todo } from "./fixtures/todo.ts"

/** Collects what is published, in calls, failing the first `failures` calls. */
const makePublisher = (failures = 0) =>
  Effect.gen(function*() {
    const batches = yield* Ref.make<ReadonlyArray<ReadonlyArray<Outbox.Message>>>([])
    const attempts = yield* Ref.make(0)
    const publish = (messages: ReadonlyArray<Outbox.Message>) =>
      Ref.updateAndGet(attempts, (n) => n + 1).pipe(
        Effect.flatMap((attempt) =>
          attempt <= failures
            ? Effect.fail(new Error("broker unavailable"))
            : Ref.update(batches, (all) => [...all, messages])
        )
      )
    const messages = Effect.map(Ref.get(batches), (all) => all.flat())
    return { publish, batches, attempts, messages }
  })

const createTodos = (count: number) =>
  Effect.gen(function*() {
    const todos = yield* AggregateService.make(Todo)
    for (let index = 1; index <= count; index++) {
      yield* todos.commands.createTodo(`t${index}`, { title: `Todo ${index}` })
    }
    return todos
  })

const Storage = Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)

describe("Outbox", () => {
  it.effect("publishes mapped events in batches, with stable ids", () =>
    Effect.gen(function*() {
      const publisher = yield* makePublisher()
      const outbox = Outbox.make({
        name: "todo-outbox",
        aggregates: [Todo],
        batchSize: 2,
        messages: {
          TodoCreated: ({ title }, { aggregateId }) => ({
            type: "todo.created.v1",
            payload: { id: aggregateId, title }
          }),
          // Deleted todos are announced, completed ones are not.
          TodoDeleted: (_, { aggregateId }) => ({ type: "todo.deleted.v1", payload: { id: aggregateId } })
        },
        publish: publisher.publish
      })
      yield* Effect.gen(function*() {
        const todos = yield* createTodos(4)
        yield* todos.commands.completeTodo("t1", {})
        yield* todos.commands.deleteTodo("t2", { message: "done" })
        yield* Outbox.Outboxes.use((outboxes) => outboxes.published())
      }).pipe(Effect.provide(Outbox.layer([outbox])), Effect.scoped)

      const batches = yield* Ref.get(publisher.batches)
      assert.deepStrictEqual(batches.map((batch) => batch.length), [2, 2, 1])
      const messages = batches.flat()
      assert.deepStrictEqual(messages.map(({ type }) => type), [
        "todo.created.v1",
        "todo.created.v1",
        "todo.created.v1",
        "todo.created.v1",
        "todo.deleted.v1"
      ])
      assert.deepStrictEqual(messages.map(({ id }) => id), [
        "todo/t1@1",
        "todo/t2@1",
        "todo/t3@1",
        "todo/t4@1",
        "todo/t2@2"
      ])
      assert.deepStrictEqual(messages[0]?.payload, { id: "t1", title: "Todo 1" })
    }).pipe(Effect.provide(Storage)))

  it.effect("publishes every event in its JSON form without mappings", () =>
    Effect.gen(function*() {
      const publisher = yield* makePublisher()
      const outbox = Outbox.make({ name: "all", aggregates: [Todo], publish: publisher.publish })
      yield* Effect.gen(function*() {
        const todos = yield* createTodos(1)
        yield* todos.commands.completeTodo("t1", {})
        yield* Outbox.Outboxes.use((outboxes) => outboxes.published())
      }).pipe(Effect.provide(Outbox.layer([outbox])), Effect.scoped)

      const messages = yield* publisher.messages
      assert.deepStrictEqual(messages.map(({ type }) => type), ["todo.TodoCreated", "todo.TodoCompleted"])
      assert.deepStrictEqual(messages[0]?.payload, {
        _tag: "TodoCreated",
        title: "Todo 1",
        createdAt: "1970-01-01T00:00:00.000Z"
      })
    }).pipe(Effect.provide(Storage)))

  it.live("retries a batch that failed to publish", () =>
    Effect.gen(function*() {
      const publisher = yield* makePublisher(2)
      const done = yield* Deferred.make<void>()
      const outbox = Outbox.make({
        name: "retrying",
        aggregates: [Todo],
        publish: (messages) => publisher.publish(messages).pipe(Effect.andThen(Deferred.succeed(done, void 0)))
      })
      yield* Effect.gen(function*() {
        yield* createTodos(3)
        yield* Deferred.await(done)
      }).pipe(Effect.provide(Outbox.layer([outbox])), Effect.scoped)

      assert.strictEqual(yield* Ref.get(publisher.attempts), 3)
      assert.deepStrictEqual((yield* publisher.messages).map(({ id }) => id), ["todo/t1@1", "todo/t2@1", "todo/t3@1"])
    }).pipe(Effect.provide(Storage), Effect.provideService(References.MinimumLogLevel, "None")))

  it.effect("resumes from its checkpoint after a restart", () =>
    Effect.gen(function*() {
      const publisher = yield* makePublisher()
      const outbox = Outbox.make({ name: "resuming", aggregates: [Todo], publish: publisher.publish })
      const run = (aggregateId: string) =>
        Effect.gen(function*() {
          const todos = yield* AggregateService.make(Todo)
          yield* todos.commands.createTodo(aggregateId, { title: aggregateId })
          yield* Outbox.Outboxes.use((outboxes) => outboxes.published())
        }).pipe(Effect.provide(Layer.fresh(Outbox.layer([outbox]))), Effect.scoped)

      yield* run("t1")
      yield* run("later")

      assert.deepStrictEqual((yield* publisher.messages).map(({ id }) => id), ["todo/t1@1", "todo/later@1"])
    }).pipe(Effect.provide(Storage)))

  it.effect("publishes again, with the same ids, what a crash left unconfirmed", () =>
    Effect.gen(function*() {
      const publisher = yield* makePublisher()
      const outbox = Outbox.make({ name: "crashing", aggregates: [Todo], publish: publisher.publish })
      // A process that dies before saving its checkpoint.
      const Forgetful = Layer.succeed(Checkpoints.Checkpoints)({ get: () => Effect.succeed(0), set: () => Effect.void })
      const run = Outbox.Outboxes.use((outboxes) => outboxes.published()).pipe(
        Effect.provide(Layer.fresh(Outbox.layer([outbox])).pipe(Layer.provide(Forgetful))),
        Effect.scoped
      )
      yield* createTodos(2)
      yield* run
      yield* run

      assert.deepStrictEqual((yield* publisher.messages).map(({ id }) => id), [
        "todo/t1@1",
        "todo/t2@1",
        "todo/t1@1",
        "todo/t2@1"
      ])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("never saves a checkpoint past a message that wasn't published", () =>
    Effect.gen(function*() {
      const outbox = Outbox.make({
        name: "failing",
        aggregates: [Todo],
        publish: () => Effect.fail("broker unavailable")
      })
      yield* Effect.gen(function*() {
        yield* createTodos(2)
        const error = yield* Effect.flip(Outbox.Outboxes.use((outboxes) => outboxes.published()))
        assert.strictEqual(error._tag, "OutboxError")
      }).pipe(Effect.provide(Outbox.layer([outbox])), Effect.scoped)

      assert.strictEqual(yield* Checkpoints.Checkpoints.use((checkpoints) => checkpoints.get("failing")), 0)
    }).pipe(Effect.provide(Storage), Effect.provideService(References.MinimumLogLevel, "None")))
})
