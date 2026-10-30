import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect, Exit } from "effect"
import { Fixture } from "effect-cqrs"
import { Todo, TodoEvent } from "../examples/todo/aggregate.ts"

const at = DateTime.makeUnsafe(0)
const created = TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: at })
const deleted = TodoEvent.cases.TodoDeleted.make({ message: "Done", deletedAt: at })

describe("Fixture", () => {
  it.effect("reports the events and state a command produces", () =>
    Effect.gen(function*() {
      const outcome = yield* Fixture.make(Todo).when("createTodo", { title: "Buy Milk" })

      assert.deepStrictEqual(outcome.events, [created])
      assert.deepStrictEqual(outcome.state, { status: "open", title: "Buy Milk" })
      assert.strictEqual(outcome.version, 1)
    }))

  it.effect("runs commands on top of the given events", () =>
    Effect.gen(function*() {
      const outcome = yield* Fixture.make(Todo).given([created]).when("deleteTodo", { message: "Done" })

      assert.deepStrictEqual(outcome.events, [deleted])
      assert.deepStrictEqual(outcome.state, { status: "deleted", title: "Buy Milk" })
      assert.strictEqual(outcome.version, 2)
    }))

  it.effect("exposes the state of the given events", () =>
    Effect.gen(function*() {
      const state = yield* Fixture.make(Todo).given([created, deleted]).state

      assert.deepStrictEqual(state, { status: "deleted", title: "Buy Milk" })
    }))

  it.effect("returns the error of a rejected command", () =>
    Effect.gen(function*() {
      const error = yield* Fixture.make(Todo).given([created, deleted]).whenFails("deleteTodo", { message: "Again" })

      assert.strictEqual(error._tag, "TodoAlreadyDeleted")
    }))

  it.effect("rejects invalid payloads like the real service", () =>
    Effect.gen(function*() {
      const error = yield* Fixture.make(Todo).whenFails("createTodo", { title: "" })

      assert.strictEqual(error._tag, "InvalidCommand")
    }))

  it.effect("dies when an expected failure succeeds, so the test can't pass by accident", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(Fixture.make(Todo).whenFails("createTodo", { title: "Buy Milk" }))

      assert.isTrue(Exit.hasDies(exit))
    }))

  it.effect("continues from an outcome", () =>
    Effect.gen(function*() {
      const first = yield* Fixture.make(Todo).when("createTodo", { title: "Buy Milk" })
      const second = yield* first.fixture.when("completeTodo", {})
      const third = yield* second.fixture.when("deleteTodo", { message: "Done" })

      assert.strictEqual(third.version, 3)
      assert.deepStrictEqual(third.state, { status: "deleted", title: "Buy Milk" })
    }))

  it.effect("leaves the original fixture untouched", () =>
    Effect.gen(function*() {
      const fixture = Fixture.make(Todo).given([created])

      yield* fixture.when("deleteTodo", { message: "Done" })

      assert.deepStrictEqual(yield* fixture.state, { status: "open", title: "Buy Milk" })
    }))
})
