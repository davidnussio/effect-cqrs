import { assert, describe, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Aggregate, type AggregateService, type EventStore, EventVersions, Fixture, ViewModel } from "effect-cqrs"
import { Todo, type TodoAlreadyCreated, type TodoAlreadyDeleted, type TodoNotFound } from "./fixtures/todo.ts"

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

  it("types the errors and options of commands", () => {
    type Todos = AggregateService.AggregateService<typeof Todo>
    type CreateError = Effect.Error<ReturnType<Todos["commands"]["createTodo"]>>
    type DispatchError = Effect.Error<ReturnType<Todos["dispatch"]>>

    const create: Equals<CreateError, TodoAlreadyCreated | AggregateService.CommandFailure> = true
    const dispatch: Equals<
      DispatchError,
      | TodoAlreadyCreated
      | TodoNotFound
      | TodoAlreadyDeleted
      | AggregateService.CommandFailure
      | AggregateService.UnknownCommand
    > = true
    const wrongVersion: AggregateService.WrongExpectedVersion extends CreateError ? true : false = true
    const conflict: EventStore.ConcurrencyConflict extends CreateError ? true : false = true

    void ((todos: Todos) => {
      todos.commands.createTodo("todo-1", { title: "Buy Milk" })
      todos.commands.createTodo("todo-1", { title: "Buy Milk" }, { commandId: "c1", expectedVersion: 0 })
      // @ts-expect-error the expected version is a number
      todos.commands.createTodo("todo-1", { title: "Buy Milk" }, { expectedVersion: "0" })
    })

    assert.isTrue(create && dispatch && wrongVersion && conflict)
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

    // Only its type is checked.
    const _scenario = fixture.when("createTodo", { title: "Buy Milk" }).expectEvent("TodoCreated")
    const error: Equals<
      Effect.Error<typeof _scenario>,
      TodoAlreadyCreated | AggregateService.CommandFailure
    > = true

    assert.isTrue(error)
  })
  it("requires a history to end in the event's current shape", () => {
    const Named = Schema.TaggedUnion({ Named: { first: Schema.String, last: Schema.String } })
    const V1 = Schema.Struct({ name: Schema.String })
    const make = <V extends Aggregate.Versions<typeof Named.Type>>(versions: V) =>
      Aggregate.make<string>()({
        name: "named",
        events: Named,
        initial: "",
        projection: { Named: (_, { first }) => first },
        versions
      })

    const named = make({
      Named: EventVersions.from(V1).next(Named.cases.Named, ({ name }) => ({ first: name, last: "" }))
    })
    make({
      // @ts-expect-error the history stops before the current shape
      Named: EventVersions.from(V1)
    })
    EventVersions.from(V1).next(
      Named.cases.Named,
      // @ts-expect-error the upcast has to produce the next version
      ({ name }) => ({ first: name })
    )

    Aggregate.historical(named, "Named", 1, { name: "Ada" })
    // Only type checked: these would throw when they run.
    void (() => {
      // @ts-expect-error version 1 had a name, not a first name
      Aggregate.historical(named, "Named", 1, { first: "Ada" })
      // @ts-expect-error there is no version 3
      Aggregate.historical(named, "Named", 3, { first: "Ada", last: "Lovelace" })
    })
  })
})
