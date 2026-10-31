import { assert, describe, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Aggregate, type AggregateService, Fixture, ViewModel } from "effect-cqrs"
import { Todo, type TodoAlreadyCreated } from "../examples/todo/aggregate.ts"

type Equals<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false

const Events = Schema.TaggedUnion({ Tick: {}, Tock: {} })

// These are checked by `tsc`. A `@ts-expect-error` that stops erroring fails `pnpm check`.
describe("types", () => {
  it("requires a projection for every event", () => {
    Aggregate.make<number>()({
      name: "clock",
      events: Events,
      initial: 0,
      // @ts-expect-error Tock is missing
      projection: { Tick: (n) => n + 1 }
    })
  })

  it("checks projections against the state", () => {
    Aggregate.make<number>()({
      name: "clock",
      events: Events,
      initial: 0,
      projection: {
        // @ts-expect-error the state is a number
        Tick: () => "one",
        Tock: (n) => n + 1
      }
    })
  })

  it("requires handlers to return events of the aggregate", () => {
    Aggregate.make<number>()({
      name: "clock",
      events: Events,
      initial: 0,
      projection: { Tick: (n) => n + 1, Tock: (n) => n + 1 }
    }).command("wrong", {
      payload: Schema.Struct({}),
      // @ts-expect-error not an event
      handler: () => Effect.succeed({ _tag: "Other" })
    })
  })

  it("derives command types from the aggregate", () => {
    const names: Equals<Aggregate.CommandName<typeof Todo>, "createTodo" | "completeTodo" | "deleteTodo"> = true
    const payload: Equals<Aggregate.Payload<typeof Todo, "createTodo">, { readonly title: string }> = true
    const error: Equals<Aggregate.CommandError<typeof Todo, "createTodo">, TodoAlreadyCreated> = true
    const context: Equals<Aggregate.Context<typeof Todo>, never> = true

    assert.isTrue(names && payload && error && context)
  })

  it("only accepts view model handlers for events of its aggregates", () => {
    ViewModel.make({
      name: "wrong",
      aggregates: [Todo],
      dispose: Effect.void,
      handlers: {
        // @ts-expect-error not an event of Todo
        Tick: () => Effect.void
      }
    })
  })

  it("types the fixture chain from the aggregate", () => {
    const fixture = Fixture.make(Todo)

    // @ts-expect-error not a command of Todo
    fixture.when("nope", {})
    // @ts-expect-error the title is a string
    fixture.when("createTodo", { title: 1 })
    // @ts-expect-error not an event of Todo
    fixture.givenEvents([{ _tag: "Other" }])
    // @ts-expect-error not a tag of Todo
    fixture.when("completeTodo", {}).expectEvent("Other")

    const scenario = fixture.when("createTodo", { title: "Buy Milk" }).expectEvent("TodoCreated")
    const error: Equals<Effect.Error<typeof scenario>, TodoAlreadyCreated | AggregateService.CommandFailure> = true

    assert.isTrue(error)
  })
})
