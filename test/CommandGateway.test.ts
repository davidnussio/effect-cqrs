import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Layer, Schema, Stream } from "effect"
import { Aggregate, AggregateService, CommandGateway, EventStore, Metadata, PersonalData } from "effect-cqrs"
import { Account } from "./fixtures/account.ts"
import { Todo, TodoAlreadyCreated } from "./fixtures/todo.ts"

// Shares the `deposit` command with `Account`, so `deposit` alone doesn't say where it goes.
const LedgerEvent = Schema.TaggedUnion({ Deposited: { amount: Schema.Number } })
const Ledger = Aggregate.make<number>()({
  name: "ledger",
  events: LedgerEvent,
  initial: 0,
  projection: { Deposited: (total, { amount }) => total + amount }
}).command("deposit", {
  payload: Schema.Struct({ amount: Schema.Number }),
  handler: (_, { amount }) => Effect.succeed(LedgerEvent.cases.Deposited.make({ amount }))
})

class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", { type: Schema.String }) {}

class CurrentUser extends Context.Service<CurrentUser, { readonly id: string }>()("test/CurrentUser") {}

// The account's owner is personal data, so its service needs keys.
const Store = Layer.mergeAll(
  EventStore.layerMemory,
  PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
)

const stored = EventStore.EventStore.use((store) => Stream.runCollect(store.read()))

describe("CommandGateway", () => {
  it.effect("sends typed commands to the aggregate they name", () =>
    Effect.gen(function*() {
      const gateway = yield* CommandGateway.make([Todo, Account])
      const created = yield* gateway.send({
        aggregateName: "todo",
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" }
      })
      assert.strictEqual(created.aggregateName, "todo")
      assert.strictEqual(created.events[0]?._tag, "TodoCreated")

      const opened = yield* gateway.send({
        aggregateName: "account",
        aggregateId: "acc",
        type: "openAccount",
        payload: { owner: "Alice", color: "blue" }
      })
      assert.strictEqual(opened.aggregateName, "account")

      assert.deepStrictEqual(yield* gateway.services.todo.readModel("a"), { status: "open", title: "Buy Milk" })
    }).pipe(Effect.provide(Store)))

  it.effect("passes options and the handler's errors through", () =>
    Effect.gen(function*() {
      const gateway = yield* CommandGateway.make([Todo])
      const command = {
        aggregateName: "todo",
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" },
        commandId: "c1"
      } as const
      const first = yield* gateway.send(command)
      const again = yield* gateway.send(command)
      assert.strictEqual(again.version, first.version)

      const error = yield* Effect.flip(gateway.send({ ...command, commandId: "c2" }))
      assert.instanceOf(error, TodoAlreadyCreated)

      const stale = yield* Effect.flip(
        gateway.send({ aggregateName: "todo", aggregateId: "a", type: "completeTodo", payload: {}, expectedVersion: 0 })
      )
      assert.strictEqual(stale._tag, "WrongExpectedVersion")
    }).pipe(Effect.provide(Store)))

  it.effect("routes envelopes by aggregate name, or by type when a single aggregate has it", () =>
    Effect.gen(function*() {
      const gateway = yield* CommandGateway.make([Todo, Account, Ledger])
      const named = yield* gateway.dispatch({
        aggregateName: "todo",
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" }
      })
      assert.strictEqual(named.aggregateName, "todo")

      const byType = yield* gateway.dispatch({ aggregateId: "a", type: "completeTodo", payload: {} })
      assert.strictEqual(byType.aggregateName, "todo")
      assert.strictEqual(byType.version, 2)

      const ledger = yield* gateway.dispatch({
        aggregateName: "ledger",
        aggregateId: "l",
        type: "deposit",
        payload: { amount: 5 }
      })
      assert.strictEqual(ledger.aggregateName, "ledger")
    }).pipe(Effect.provide(Store)))

  it.effect("refuses envelopes it can't route", () =>
    Effect.gen(function*() {
      const gateway = yield* CommandGateway.make([Todo, Account, Ledger])

      const ambiguous = yield* Effect.flip(gateway.dispatch({ aggregateId: "x", type: "deposit", payload: {} }))
      assert.instanceOf(ambiguous, CommandGateway.UnroutableCommand)
      assert.deepStrictEqual((ambiguous as CommandGateway.UnroutableCommand).candidates, ["account", "ledger"])

      const none = yield* Effect.flip(gateway.dispatch({ aggregateId: "x", type: "fly", payload: {} }))
      assert.instanceOf(none, CommandGateway.UnroutableCommand)
      assert.deepStrictEqual((none as CommandGateway.UnroutableCommand).candidates, [])

      const missing = yield* Effect.flip(
        gateway.dispatch({ aggregateName: "order", aggregateId: "x", type: "createTodo", payload: {} })
      )
      assert.instanceOf(missing, CommandGateway.UnroutableCommand)
      assert.strictEqual((missing as CommandGateway.UnroutableCommand).aggregateName, "order")

      const unknown = yield* Effect.flip(
        gateway.dispatch({ aggregateName: "todo", aggregateId: "x", type: "fly", payload: {} })
      )
      assert.instanceOf(unknown, AggregateService.UnknownCommand)

      const invalid = yield* Effect.flip(gateway.dispatch({ type: "createTodo" }))
      assert.instanceOf(invalid, AggregateService.InvalidCommand)

      assert.strictEqual((yield* stored).length, 0)
    }).pipe(Effect.provide(Store)))

  it.effect("runs interceptors around commands, the first outermost", () =>
    Effect.gen(function*() {
      const calls: Array<string> = []
      const trace = (name: string): CommandGateway.Interceptor => (command, next) =>
        Effect.sync(() => calls.push(`${name} before ${command.aggregateName}.${command.type}`)).pipe(
          Effect.andThen(next),
          Effect.tap(() => Effect.sync(() => calls.push(`${name} after`)))
        )
      const gateway = yield* CommandGateway.make([Todo], { interceptors: [trace("outer"), trace("inner")] })

      yield* gateway.send({ aggregateName: "todo", aggregateId: "a", type: "createTodo", payload: { title: "Milk" } })
      yield* gateway.dispatch({ aggregateId: "a", type: "completeTodo", payload: {} })

      assert.deepStrictEqual(calls, [
        "outer before todo.createTodo",
        "inner before todo.createTodo",
        "inner after",
        "outer after",
        "outer before todo.completeTodo",
        "inner before todo.completeTodo",
        "inner after",
        "outer after"
      ])
    }).pipe(Effect.provide(Store)))

  it.effect("lets interceptors refuse commands and add metadata from the caller's context", () =>
    Effect.gen(function*() {
      const authorize: CommandGateway.Interceptor<Unauthorized, CurrentUser> = (command, next) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          if (user.id === "guest") return yield* new Unauthorized({ type: command.type })
          return yield* Metadata.withMetadata(next, { userId: user.id })
        })
      const gateway = yield* CommandGateway.make([Todo], { interceptors: [authorize] })
      const create = gateway.send({
        aggregateName: "todo",
        aggregateId: "a",
        type: "createTodo",
        payload: { title: "Buy Milk" }
      })

      const refused = yield* Effect.flip(Effect.provideService(create, CurrentUser, { id: "guest" }))
      assert.instanceOf(refused, Unauthorized)
      assert.strictEqual((yield* stored).length, 0)

      yield* Effect.provideService(create, CurrentUser, { id: "alice" })
      const events = yield* stored
      assert.deepStrictEqual(events.map((event) => event.metadata.userId), ["alice"])
    }).pipe(Effect.provide(Store)))

  it.effect("uses the services it is given", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo, { maxRetries: 1 })
      const gateway = yield* CommandGateway.make([todos, Account])
      assert.strictEqual(gateway.services.todo, todos)
      yield* gateway.send({ aggregateName: "todo", aggregateId: "a", type: "createTodo", payload: { title: "Milk" } })
      assert.strictEqual((yield* todos.history("a")).length, 1)
    }).pipe(Effect.provide(Store)))

  it.effect("refuses the same aggregate twice", () =>
    Effect.gen(function*() {
      const todos = yield* AggregateService.make(Todo)
      const exit = yield* Effect.exit(CommandGateway.make([Todo, todos]))
      assert.isTrue(Exit.hasDies(exit))
    }).pipe(Effect.provide(Store)))
})
