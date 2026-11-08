import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import { Aggregate, AggregateService, Causation, Checkpoints, EventStore, ViewModel } from "effect-cqrs"
import { Todo } from "../examples/todo/aggregate.ts"

const AuditEvent = Schema.TaggedUnion({ Noted: { todo: Schema.String } })
const Audit = Aggregate.make<number>()({
  name: "audit",
  events: AuditEvent,
  initial: 0,
  projection: { Noted: (count) => count + 1 }
}).command("note", {
  payload: Schema.Struct({ todo: Schema.String }),
  handler: (_, { todo }) => Effect.succeed(AuditEvent.cases.Noted.make({ todo }))
})

const readAll = EventStore.EventStore.use((store) => Stream.runCollect(store.read()))

const ids = (event: EventStore.StoredEvent) => [event.correlationId, event.causationId]

describe("Causation", () => {
  it.effect("starts a new correlation for a command without a cause", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const first = yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      const second = yield* todos.commands.createTodo("b", { title: "Buy Eggs" })

      const [a, b] = yield* readAll
      assert.deepStrictEqual(ids(a!), [first.correlationId, first.correlationId])
      assert.deepStrictEqual(ids(b!), [second.correlationId, second.correlationId])
      assert.notStrictEqual(first.correlationId, second.correlationId)
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("records the causation the caller provides", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const result = yield* todos.commands.createTodo("a", { title: "Buy Milk" }).pipe(
        Causation.withCausation({ correlationId: "request-1", causationId: "request-1" })
      )

      const [event] = yield* readAll
      assert.strictEqual(result.correlationId, "request-1")
      assert.deepStrictEqual(ids(event!), ["request-1", "request-1"])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("takes ids from a dispatched envelope", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.dispatch({
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" },
        correlationId: "session-7",
        causationId: "click-3"
      })

      const [event] = yield* readAll
      assert.deepStrictEqual(ids(event!), ["session-7", "click-3"])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("makes the handled event the cause of what a view model does", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const audit = yield* AggregateService.make(Audit)
      const Auditor = ViewModel.make({
        name: "auditor",
        aggregates: [Todo],
        handlers: {
          TodoCreated: (event, meta) => audit.commands.note(meta.aggregateId, { todo: event.title })
        },
        dispose: Effect.void
      })

      const created = yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(
        Effect.provide(ViewModel.layer([Auditor]).pipe(Layer.provide(Checkpoints.layerMemory)))
      )

      const [cause, effect] = yield* readAll
      assert.strictEqual(effect?.aggregateName, "audit")
      assert.deepStrictEqual(ids(effect!), [created.correlationId, EventStore.eventId(cause!)])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("lists the ids in the history", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.commands.createTodo("a", { title: "Buy Milk" }).pipe(
        Causation.withCausation({ correlationId: "c", causationId: "d" })
      )

      const [entry] = yield* todos.history("a")
      assert.deepStrictEqual([entry?.correlationId, entry?.causationId], ["c", "d"])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it("names events by stream and version, whatever the id contains", () => {
    const at = (aggregateId: string) => EventStore.eventId({ aggregateName: "a", aggregateId, version: 1 })

    assert.strictEqual(at("x"), "a/x@1")
    assert.notStrictEqual(at("x/y"), EventStore.eventId({ aggregateName: "a/x", aggregateId: "y", version: 1 }))
  })
})
