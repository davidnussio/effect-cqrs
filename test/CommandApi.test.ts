import { assert, describe, it, layer } from "@effect/vitest"
import { type Duration, Effect, Fiber, Layer, References, Schema } from "effect"
import { AggregateService, Checkpoints, CommandApi, EventStore, ViewModel } from "effect-cqrs"
import { HttpRouter, HttpServer } from "effect/http"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup, HttpApiTest, OpenApi } from "effect/http-api"
import { Todo, TodoAlreadyCreated, TodoAlreadyDeleted } from "./fixtures/todo.ts"

const TodoState = Schema.Union([
  Schema.Struct({ status: Schema.Literal("missing") }),
  Schema.Struct({ status: Schema.Literals(["open", "completed", "deleted"]), title: Schema.String })
])

const TodoApi = CommandApi.group(Todo, { state: TodoState, errors: [TodoAlreadyCreated, TodoAlreadyDeleted] })

class Api extends HttpApi.make("api").add(TodoApi) {}

const Handlers = CommandApi.layer(Api, "todo", AggregateService.make(Todo), { historyLimit: 2 })

const App = Layer.mergeAll(Handlers.pipe(Layer.provide(EventStore.layerMemory)), HttpServer.layerServices)

const client = HttpApiTest.groups(Api, ["todo"])

layer(App)("CommandApi", (it) => {
  it.effect("runs commands and reads them back", () =>
    Effect.gen(function*() {
      const { todo } = yield* client
      const created = yield* todo.createTodo({ params: { aggregateId: "a" }, payload: { title: "Buy Milk" } })
      assert.strictEqual(created.version, 1)
      assert.strictEqual(created.events[0]?._tag, "TodoCreated")

      yield* todo.completeTodo({ params: { aggregateId: "a" }, payload: {} })
      assert.deepStrictEqual(yield* todo.state({ params: { aggregateId: "a" }, query: {} }), {
        status: "completed",
        title: "Buy Milk"
      })

      const history = yield* todo.history({ params: { aggregateId: "a" }, query: { payload: true } })
      assert.deepStrictEqual(history.map((entry) => entry.type), ["TodoCreated", "TodoCompleted"])
      assert.strictEqual(history[0]?.payload?._tag, "TodoCreated")
      const withoutPayload = yield* todo.history({ params: { aggregateId: "a" }, query: {} })
      assert.isUndefined(withoutPayload[0]?.payload)
    }))

  it.effect("lists history in pages no longer than the limit", () =>
    Effect.gen(function*() {
      const { todo } = yield* client
      const params = { aggregateId: "paged" }
      yield* todo.createTodo({ params, payload: { title: "Buy Milk" } })
      yield* todo.completeTodo({ params, payload: {} })
      yield* todo.deleteTodo({ params, payload: { message: "Done" } })

      const first = yield* todo.history({ params, query: { limit: 10 } })
      assert.deepStrictEqual(first.map((entry) => entry.version), [1, 2])
      const next = yield* todo.history({ params, query: { afterVersion: 2 } })
      assert.deepStrictEqual(next.map((entry) => entry.version), [3])
      const one = yield* todo.history({ params, query: { limit: 1 } })
      assert.deepStrictEqual(one.map((entry) => entry.version), [1])
    }))

  it.effect("answers a declared domain error as it is", () =>
    Effect.gen(function*() {
      const { todo } = yield* client
      yield* todo.createTodo({ params: { aggregateId: "b" }, payload: { title: "Buy Eggs" } })
      const error = yield* Effect.flip(todo.createTodo({ params: { aggregateId: "b" }, payload: { title: "Again" } }))
      assert.strictEqual(error._tag, "TodoAlreadyCreated")
      assert.strictEqual(error._tag === "TodoAlreadyCreated" && error.title, "Buy Eggs")
    }))

  it.effect("rejects other domain errors with their tag", () =>
    Effect.gen(function*() {
      const { todo } = yield* client
      const error = yield* Effect.flip(todo.completeTodo({ params: { aggregateId: "missing" }, payload: {} }))
      assert.strictEqual(error._tag, "CommandRejected")
      assert.strictEqual(error._tag === "CommandRejected" && error.error, "TodoNotFound")
    }))

  it.effect("describes every endpoint in OpenAPI", () =>
    Effect.sync(() => {
      const spec = OpenApi.fromApi(Api)
      assert.deepStrictEqual(Object.keys(spec.paths).sort(), [
        "/todo/{aggregateId}",
        "/todo/{aggregateId}/completeTodo",
        "/todo/{aggregateId}/createTodo",
        "/todo/{aggregateId}/deleteTodo",
        "/todo/{aggregateId}/history"
      ])
      const responses = spec.paths["/todo/{aggregateId}/createTodo"]?.post?.responses ?? {}
      assert.includeMembers(Object.keys(responses), ["200", "400", "409", "422"])
    }))
})

const webHandler = (store: Layer.Layer<EventStore.EventStore, EventStore.EventStoreError>) =>
  HttpRouter.toWebHandler(
    HttpApiBuilder.layer(Api).pipe(
      Layer.provide(Handlers.pipe(Layer.provide(store))),
      Layer.provide(HttpServer.layerServices)
    ),
    { disableLogger: true }
  )

const post = (
  handler: (request: Request) => Promise<Response>,
  path: string,
  body?: string,
  headers?: Record<string, string>
) =>
  Effect.promise(async () => {
    const response = await handler(
      new Request(`http://localhost${path}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        ...(body === undefined ? {} : { body })
      })
    )
    return { status: response.status, body: await response.text() }
  })

describe("CommandApi.group", () => {
  it("refuses a command with the name of an endpoint", () => {
    const withState = { ...Todo, commands: { ...Todo.commands, state: Todo.commands.createTodo } }
    assert.throws(() => CommandApi.group(withState), /command named state/)
    const withReadModel = { ...Todo, commands: { ...Todo.commands, readModel: Todo.commands.createTodo } }
    assert.doesNotThrow(() => CommandApi.group(withReadModel))
  })
})

describe("CommandApi over HTTP", () => {
  it.effect("answers with status codes", () =>
    Effect.gen(function*() {
      const { dispose, handler } = webHandler(EventStore.layerMemory)
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const created = yield* post(handler, "/todo/a/createTodo", JSON.stringify({ title: "Buy Milk" }))
      assert.strictEqual(created.status, 200)

      const notJson = yield* post(handler, "/todo/a/createTodo", "{")
      assert.strictEqual(notJson.status, 400)
      assert.strictEqual(JSON.parse(notJson.body)._tag, "InvalidCommand")

      const invalid = yield* post(handler, "/todo/b/createTodo", JSON.stringify({ title: "" }))
      assert.strictEqual(invalid.status, 400)
      assert.strictEqual(JSON.parse(invalid.body)._tag, "InvalidCommand")

      // No body is an empty payload.
      const completed = yield* post(handler, "/todo/a/completeTodo")
      assert.strictEqual(completed.status, 200)

      const again = yield* post(handler, "/todo/a/createTodo", JSON.stringify({ title: "Again" }))
      assert.strictEqual(again.status, 422)
      assert.strictEqual(JSON.parse(again.body)._tag, "TodoAlreadyCreated")
    }))

  it.effect("passes Idempotency-Key and If-Match to the command", () =>
    Effect.gen(function*() {
      const { dispose, handler } = webHandler(EventStore.layerMemory)
      yield* Effect.addFinalizer(() => Effect.promise(dispose))
      const create = JSON.stringify({ title: "Buy Milk" })

      const first = yield* post(handler, "/todo/a/createTodo", create, { "idempotency-key": "c-1" })
      const retried = yield* post(handler, "/todo/a/createTodo", create, { "idempotency-key": "c-1" })
      assert.strictEqual(first.status, 200)
      assert.strictEqual(retried.status, 200)
      assert.strictEqual(JSON.parse(retried.body).position, JSON.parse(first.body).position)

      const stale = yield* post(handler, "/todo/a/completeTodo", undefined, { "if-match": "\"0\"" })
      assert.strictEqual(stale.status, 409)
      assert.strictEqual(JSON.parse(stale.body)._tag, "WrongExpectedVersion")

      const current = yield* post(handler, "/todo/a/completeTodo", undefined, { "if-match": "1" })
      assert.strictEqual(current.status, 200)

      const malformed = yield* post(handler, "/todo/a/deleteTodo", JSON.stringify({ message: "x" }), {
        "if-match": "abc"
      })
      assert.strictEqual(malformed.status, 400)

      const any = yield* post(handler, "/todo/a/deleteTodo", JSON.stringify({ message: "x" }), { "if-match": "*" })
      assert.strictEqual(any.status, 200)
    }))

  it.live("answers a lost race with 409", () =>
    Effect.gen(function*() {
      const Racing = Layer.effect(
        EventStore.EventStore,
        Effect.map(EventStore.makeMemory, (store) =>
          EventStore.EventStore.of({
            ...store,
            append: (input) =>
              Effect.fail(
                new EventStore.ConcurrencyConflict({
                  aggregateName: input.aggregateName,
                  aggregateId: input.aggregateId,
                  expectedVersion: input.expectedVersion
                })
              )
          }))
      )
      const { dispose, handler } = webHandler(Racing)
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const response = yield* post(handler, "/todo/a/createTodo", JSON.stringify({ title: "Buy Milk" }))
      assert.strictEqual(response.status, 409)
      assert.strictEqual(JSON.parse(response.body)._tag, "ConcurrencyConflict")
    }))

  it.effect("answers a store failure with 500", () =>
    Effect.gen(function*() {
      const Broken = Layer.effect(
        EventStore.EventStore,
        Effect.map(EventStore.makeMemory, (store) =>
          EventStore.EventStore.of({
            ...store,
            append: () => Effect.fail(new EventStore.EventStoreError({ operation: "append", cause: "down" }))
          }))
      )
      const { dispose, handler } = webHandler(Broken)
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const response = yield* post(handler, "/todo/a/createTodo", JSON.stringify({ title: "Buy Milk" }))
      assert.strictEqual(response.status, 500)
    }).pipe(Effect.provideService(References.MinimumLogLevel, "None")))
})

// A view model that only moves on when the test opens the gate, so the test controls
// whether it has caught up.
const makeGate = () => {
  let open = () => {}
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  return { open: () => open(), wait: Effect.promise(() => opened) }
}

const readAfterApp = (gate: ReturnType<typeof makeGate>, timeout: Duration.Input) => {
  const titles: Array<string> = []
  const Titles = ViewModel.make({
    name: "titles",
    aggregates: [Todo],
    dispose: Effect.sync(() => titles.splice(0)),
    handlers: {
      TodoCreated: ({ title }) => Effect.andThen(gate.wait, Effect.sync(() => titles.push(title)))
    }
  })
  const Queries = HttpApiGroup.make("queries")
    .add(HttpApiEndpoint.get("titles", "/titles", { success: Schema.Array(Schema.String) }))
    .middleware(CommandApi.ReadAfter)
  class ReadApi extends HttpApi.make("read").add(TodoApi).add(Queries) {}

  const store = Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)
  const app = HttpApiBuilder.layer(ReadApi).pipe(
    Layer.provide(CommandApi.layer(ReadApi, "todo", AggregateService.make(Todo))),
    Layer.provide(
      HttpApiBuilder.group(
        ReadApi,
        "queries",
        (handlers) => handlers.handle("titles", () => Effect.sync(() => [...titles]))
      )
    ),
    Layer.provide(CommandApi.layerReadAfter({ timeout })),
    Layer.provide(ViewModel.layer([Titles])),
    Layer.provide(store),
    Layer.provide(HttpServer.layerServices)
  )
  return HttpRouter.toWebHandler(app, { disableLogger: true })
}

const get = (handler: (request: Request) => Promise<Response>, path: string, headers?: Record<string, string>) =>
  Effect.promise(async () => {
    const response = await handler(new Request(`http://localhost${path}`, headers === undefined ? {} : { headers }))
    return { status: response.status, body: await response.text() }
  })

describe("CommandApi.ReadAfter", () => {
  it.live("waits for the view models to reach the position a command answered with", () =>
    Effect.gen(function*() {
      const gate = makeGate()
      const { dispose, handler } = readAfterApp(gate, "5 seconds")
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const created = yield* Effect.promise(() =>
        handler(
          new Request("http://localhost/todo/a/createTodo", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ title: "Buy Milk" })
          })
        )
      )
      const position = created.headers.get("event-position")
      const body = yield* Effect.promise(() => created.json() as Promise<{ position: number }>)
      assert.strictEqual(position, String(body.position))

      // Without the header the read runs at once, before the view model has the title.
      assert.deepStrictEqual(JSON.parse((yield* get(handler, "/titles")).body), [])

      const waiting = yield* Effect.forkChild(get(handler, "/titles", { "read-after": position! }))
      yield* Effect.sleep("20 millis")
      assert.isUndefined(waiting.pollUnsafe())

      gate.open()
      const read = yield* Fiber.join(waiting)
      assert.strictEqual(read.status, 200)
      assert.deepStrictEqual(JSON.parse(read.body), ["Buy Milk"])
    }))

  it.live("answers 503 when the view models don't catch up in time", () =>
    Effect.gen(function*() {
      const { dispose, handler } = readAfterApp(makeGate(), "20 millis")
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const response = yield* get(handler, "/titles", { "read-after": "1000" })
      assert.strictEqual(response.status, 503)
      assert.strictEqual(JSON.parse(response.body)._tag, "NotCaughtUp")
      assert.strictEqual(JSON.parse(response.body).position, 1000)
    }))

  it.live("answers 400 to a header that isn't a position", () =>
    Effect.gen(function*() {
      const { dispose, handler } = readAfterApp(makeGate(), "20 millis")
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      for (const value of ["abc", "-1", "1.5"]) {
        const response = yield* get(handler, "/titles", { "read-after": value })
        assert.strictEqual(response.status, 400, value)
      }
    }))
})
