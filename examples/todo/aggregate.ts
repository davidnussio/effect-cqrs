import { DateTime, Effect, Schema } from "effect"
import { Aggregate } from "effect-cqrs"

export const TodoEvent = Schema.TaggedUnion({
  TodoCreated: { title: Schema.NonEmptyString, createdAt: Schema.DateTimeUtc },
  TodoCompleted: { completedAt: Schema.DateTimeUtc },
  TodoDeleted: { message: Schema.String, deletedAt: Schema.DateTimeUtc }
})
export type TodoEvent = typeof TodoEvent.Type

export class TodoAlreadyCreated extends Schema.TaggedError<TodoAlreadyCreated>()("TodoAlreadyCreated", {
  title: Schema.String
}) {}

export class TodoNotFound extends Schema.TaggedError<TodoNotFound>()("TodoNotFound", {
  aggregateId: Schema.String
}) {}

export class TodoAlreadyDeleted extends Schema.TaggedError<TodoAlreadyDeleted>()("TodoAlreadyDeleted", {
  aggregateId: Schema.String
}) {}

export type TodoState =
  | { readonly status: "missing" }
  | { readonly status: "open" | "completed" | "deleted"; readonly title: string }

export const Todo = Aggregate.make<TodoState>()({
  name: "todo",
  events: TodoEvent,
  initial: { status: "missing" },
  projection: {
    TodoCreated: (_, { title }) => ({ status: "open", title }),
    TodoCompleted: (state) => state.status === "missing" ? state : { ...state, status: "completed" },
    TodoDeleted: (state) => state.status === "missing" ? state : { ...state, status: "deleted" }
  }
})
  .command("createTodo", {
    payload: Schema.Struct({ title: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { title }) {
      if (state.status !== "missing") {
        return yield* new TodoAlreadyCreated({ title: state.title })
      }
      return TodoEvent.cases.TodoCreated.make({ title, createdAt: yield* DateTime.now })
    })
  })
  .command("completeTodo", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
      if (state.status === "missing") {
        return yield* new TodoNotFound({ aggregateId })
      }
      if (state.status === "deleted") {
        return yield* new TodoAlreadyDeleted({ aggregateId })
      }
      // Completing twice is harmless, so it records nothing.
      if (state.status === "completed") {
        return []
      }
      return TodoEvent.cases.TodoCompleted.make({ completedAt: yield* DateTime.now })
    })
  })
  .command("deleteTodo", {
    payload: Schema.Struct({ message: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { message }, { aggregateId }) {
      if (state.status === "missing") {
        return yield* new TodoNotFound({ aggregateId })
      }
      if (state.status === "deleted") {
        return yield* new TodoAlreadyDeleted({ aggregateId })
      }
      return TodoEvent.cases.TodoDeleted.make({ message, deletedAt: yield* DateTime.now })
    })
  })
