/**
 * The moleculer-cqrs README walkthrough, as a program. Events live in an
 * in-memory SQLite database, so each run starts from nothing.
 */
import { NodeRuntime } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Context, Effect, Layer } from "effect"
import { AggregateService, Checkpoints, SqliteEventStore, ViewModel } from "effect-cqrs"
import { Todo } from "./aggregate.ts"
import { listTodos, TodoList, TodoListStore } from "./view-model.ts"

class Todos extends Context.Service<Todos, AggregateService.AggregateService<typeof Todo>>()("example/Todos") {
  static readonly layer = Layer.effect(Todos, AggregateService.make(Todo))
}

const program = Effect.gen(function*() {
  const todos = yield* Todos
  const viewModels = yield* ViewModel.ViewModels

  yield* todos.commands.createTodo("uuid-todo-1", { title: "Buy Milk" })
  yield* todos.commands.createTodo("uuid-todo-2", { title: "Buy Eggs" })
  // Transports hand over untyped JSON, which `dispatch` validates.
  const last = yield* todos.dispatch({
    aggregateId: "uuid-todo-3",
    type: "createTodo",
    payload: { title: "Buy a new Google Pixel" }
  })

  yield* viewModels.caughtUp(last.position)
  yield* Effect.log("view model", yield* listTodos)
  yield* Effect.log("read model", yield* todos.readModel("uuid-todo-2"))

  const deleted = yield* todos.commands.deleteTodo("uuid-todo-2", { message: "Already bought" })
  yield* viewModels.caughtUp(deleted.position)
  yield* Effect.log("read model after delete", yield* todos.readModel("uuid-todo-2"))
  yield* Effect.log("view model after delete", yield* listTodos)

  const rejected = yield* Effect.flip(todos.commands.deleteTodo("uuid-todo-2", { message: "Again" }))
  yield* Effect.log("rejected command", rejected._tag)

  const replayed = yield* viewModels.replay({ viewModels: ["todo-list"] })
  yield* Effect.log("replayed", replayed)
  yield* Effect.log("view model after replay", yield* listTodos)

  yield* Effect.log("history", yield* todos.history("uuid-todo-2", { payload: true }))
})

const Database = SqliteClient.layer({ filename: ":memory:" })

const Storage = Layer.mergeAll(SqliteEventStore.layer(), Checkpoints.layerMemory).pipe(
  Layer.provide(Database)
)

const Main = Layer.mergeAll(Todos.layer, ViewModel.layer([TodoList])).pipe(
  Layer.provideMerge(Layer.merge(Storage, TodoListStore.layer))
)

program.pipe(Effect.provide(Main), NodeRuntime.runMain)
