/**
 * The HTTP API of the todo server. It holds no server code, so a client can
 * import it to call the server with `HttpApiClient`.
 */
import { Schema } from "effect"
import { CommandApi } from "effect-cqrs"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/http-api"
import { Todo, TodoAlreadyCreated, TodoAlreadyDeleted, TodoNotFound } from "../todo/aggregate.ts"

export const TodoState = Schema.Union([
  Schema.Struct({ status: Schema.Literal("missing") }),
  Schema.Struct({ status: Schema.Literals(["open", "completed", "deleted"]), title: Schema.String })
])

export const TodoItem = Schema.Struct({ id: Schema.String, title: Schema.String, completed: Schema.Boolean })

/** `POST /todo/:id/createTodo` and the others, `GET /todo/:id` and `GET /todo/:id/history`. */
export const TodoCommands = CommandApi.group(Todo, {
  state: TodoState,
  errors: [TodoAlreadyCreated, TodoNotFound, TodoAlreadyDeleted]
})

/**
 * The view model. A request with a `Read-After` header, the `Event-Position` a command
 * answered with, waits until the list has caught up with that command.
 */
export const TodoQueries = HttpApiGroup.make("todoList")
  .add(
    HttpApiEndpoint.get("list", "/todos", { success: Schema.Array(TodoItem) })
      .annotate(OpenApi.Summary, "Lists the todos that are not deleted")
  )
  .middleware(CommandApi.ReadAfter)

export class Api extends HttpApi.make("todo-api")
  .add(TodoCommands)
  .add(TodoQueries)
  .annotateMerge(OpenApi.annotations({ title: "Todo API", version: "1.0.0" }))
{}
