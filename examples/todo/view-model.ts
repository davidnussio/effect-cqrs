import { Context, Effect, Layer, Ref } from "effect"
import { ViewModel } from "effect-cqrs"
import { Todo } from "./aggregate.ts"

export interface TodoItem {
  readonly id: string
  readonly title: string
  readonly completed: boolean
}

export class TodoListStore extends Context.Service<TodoListStore, Ref.Ref<ReadonlyMap<string, TodoItem>>>()(
  "example/TodoListStore"
) {
  static readonly layer = Layer.effect(TodoListStore, Ref.make<ReadonlyMap<string, TodoItem>>(new Map()))
}

const modify = (f: (items: Map<string, TodoItem>) => void) =>
  TodoListStore.use((store) =>
    Ref.update(store, (items) => {
      const next = new Map(items)
      f(next)
      return next
    })
  )

export const listTodos = Effect.gen(function*() {
  const store = yield* TodoListStore
  return Array.from((yield* Ref.get(store)).values())
})

/** Deleted todos leave the list, and every handler is idempotent. */
export const TodoList = ViewModel.make({
  name: "todo-list",
  aggregates: [Todo],
  dispose: modify((items) => items.clear()),
  handlers: {
    TodoCreated: ({ title }, { aggregateId }) =>
      modify((items) => items.set(aggregateId, { id: aggregateId, title, completed: false })),
    TodoCompleted: (_, { aggregateId }) =>
      modify((items) => {
        const item = items.get(aggregateId)
        if (item !== undefined) items.set(aggregateId, { ...item, completed: true })
      }),
    TodoDeleted: (_, { aggregateId }) => modify((items) => items.delete(aggregateId))
  }
})
