import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, Metadata, ViewModel } from "effect-cqrs"
import { Todo } from "./fixtures/todo.ts"

const NoteEvent = Schema.TaggedUnion({ Noted: { text: Schema.String } })
const Notes = Aggregate.make<ReadonlyArray<Metadata.Metadata>>()({
  name: "notes",
  events: NoteEvent,
  initial: [],
  projection: { Noted: (seen, _, meta) => [...seen, meta.metadata] }
}).command("note", {
  payload: Schema.Struct({ text: Schema.String }),
  handler: (_, { text }) => Effect.succeed(NoteEvent.cases.Noted.make({ text }))
})

const readAll = EventStore.EventStore.use((store) => Stream.runCollect(store.read()))

const recorded = readAll.pipe(Effect.map((events) => events.map((event) => event.metadata)))

describe("Metadata", () => {
  it.effect("records an empty object when there is none", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.commands.createTodo("a", { title: "Buy Milk" })

      assert.deepStrictEqual(yield* recorded, [{}])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("records the metadata of the caller, key by key across nested scopes", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.commands.createTodo("a", { title: "Buy Milk" }).pipe(
        Metadata.withMetadata({ user: "alice", client: "web" }),
        Metadata.withMetadata({ client: "cli", appVersion: "1.4.0" })
      )

      assert.deepStrictEqual(yield* recorded, [{ appVersion: "1.4.0", client: "web", user: "alice" }])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("records the metadata the service was built with, under the caller's", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo).pipe(
        Effect.provide(Metadata.layer({ appVersion: "1.4.0", region: "eu" }))
      )
      yield* todos.commands.createTodo("a", { title: "Buy Milk" })
      yield* todos.commands.createTodo("b", { title: "Buy Eggs" }).pipe(
        Metadata.withMetadata({ region: "us", user: "alice" })
      )

      assert.deepStrictEqual(yield* recorded, [
        { appVersion: "1.4.0", region: "eu" },
        { appVersion: "1.4.0", region: "us", user: "alice" }
      ])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("takes metadata from a dispatched envelope, over the current one", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      yield* todos.dispatch({
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" },
        metadata: { user: "bob", ip: "10.0.0.1" }
      }).pipe(Metadata.withMetadata({ user: "alice", appVersion: "1.4.0" }))
      const invalid = yield* Effect.flip(
        todos.dispatch({ aggregateId: "b", type: "createTodo", payload: { title: "Buy Eggs" }, metadata: [1] })
      )

      assert.deepStrictEqual(yield* recorded, [{ appVersion: "1.4.0", ip: "10.0.0.1", user: "bob" }])
      assert.strictEqual(invalid._tag, "InvalidCommand")
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("passes the metadata to projections and lists it in the history", () =>
    Effect.gen(function*() {
      const notes = yield* AggregateService.make(Notes)
      yield* notes.commands.note("a", { text: "first" }).pipe(Metadata.withMetadata({ appVersion: "1" }))
      yield* notes.commands.note("a", { text: "second" }).pipe(Metadata.withMetadata({ appVersion: "2" }))

      assert.deepStrictEqual(yield* notes.readModel("a"), [{ appVersion: "1" }, { appVersion: "2" }])
      const history = yield* notes.history("a")
      assert.deepStrictEqual(history.map((entry) => entry.metadata), [{ appVersion: "1" }, { appVersion: "2" }])
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("doesn't pass the metadata of an event on to what a view model does", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const notes = yield* AggregateService.make(Notes)
      const seen: Array<Metadata.Metadata> = []
      const Auditor = ViewModel.make({
        name: "auditor",
        aggregates: [Todo],
        handlers: {
          TodoCreated: (event, meta) =>
            Effect.sync(() => seen.push(meta.metadata)).pipe(
              Effect.andThen(notes.commands.note(meta.aggregateId, { text: event.title }))
            )
        },
        dispose: Effect.void
      })

      yield* todos.commands.createTodo("a", { title: "Buy Milk" }).pipe(Metadata.withMetadata({ user: "alice" }))
      yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(
        Effect.provide(ViewModel.layer([Auditor]).pipe(Layer.provide(Checkpoints.layerMemory)))
      )

      assert.deepStrictEqual(seen, [{ appVersion: "1.4.0", user: "alice" }])
      assert.deepStrictEqual(yield* recorded, [{ appVersion: "1.4.0", user: "alice" }, { appVersion: "1.4.0" }])
    }).pipe(Effect.provide(Layer.merge(EventStore.layerMemory, Metadata.layer({ appVersion: "1.4.0" })))))
})
