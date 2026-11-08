import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import {
  Aggregate,
  AggregateService,
  Checkpoints,
  EventStore,
  EventVersions,
  Fixture,
  SqliteEventStore,
  ViewModel
} from "effect-cqrs"

const Currency = Schema.Literals(["CHF", "EUR", "USD"])

// v1 had one owner field, v2 split the name, v3 added the currency.
const OpenedV1 = Schema.Struct({ owner: Schema.String })
const OpenedV2 = Schema.Struct({ firstName: Schema.String, lastName: Schema.String })

const AccountEvent = Schema.TaggedUnion({
  AccountOpened: { firstName: Schema.String, lastName: Schema.String, currency: Currency },
  MoneyDeposited: { amount: Schema.Int }
})

interface State {
  readonly name: string
  readonly currency: string
  readonly balance: number
}

const Account = Aggregate.make<State>()({
  name: "account",
  events: AccountEvent,
  initial: { name: "", currency: "", balance: 0 },
  projection: {
    AccountOpened: (state, { currency, firstName, lastName }) => ({ ...state, name: `${firstName} ${lastName}`, currency }),
    MoneyDeposited: (state, { amount }) => ({ ...state, balance: state.balance + amount })
  },
  versions: {
    AccountOpened: EventVersions.from(OpenedV1)
      .next(OpenedV2, ({ owner }) => {
        const [firstName = "", ...rest] = owner.trim().split(" ")
        return { firstName, lastName: rest.join(" ") }
      })
      .next(AccountEvent.cases.AccountOpened, (v2) => ({ ...v2, currency: "CHF" as const }))
  }
}).command("deposit", {
  payload: Schema.Struct({ amount: Schema.Int }),
  handler: (_, { amount }) => Effect.succeed(AccountEvent.cases.MoneyDeposited.make({ amount }))
})

const opened = AccountEvent.cases.AccountOpened.make({ firstName: "Alice", lastName: "Rossi", currency: "EUR" })

const readAll = EventStore.EventStore.use((store) => Stream.runCollect(store.read()))

describe("EventVersions", () => {
  it.effect("upcasts each old version to the current one before the projection sees it", () =>
    Effect.gen(function*() {
      const state = yield* Fixture.make(Account).given([
        Aggregate.historical(Account, "AccountOpened", 1, { owner: "Alice Maria Rossi" }),
        AccountEvent.cases.MoneyDeposited.make({ amount: 250 })
      ]).state

      assert.deepStrictEqual(state, { name: "Alice Maria Rossi", currency: "CHF", balance: 250 })
    }))

  it.effect("starts the chain at the version the event was stored in", () =>
    Effect.gen(function*() {
      const state = yield* Fixture.make(Account).given([
        Aggregate.historical(Account, "AccountOpened", 2, { firstName: "Bob", lastName: "Bianchi" })
      ]).state

      assert.deepStrictEqual(state, { name: "Bob Bianchi", currency: "CHF", balance: 0 })
    }))

  it.effect("runs commands on top of upcast history", () =>
    Effect.gen(function*() {
      const outcome = yield* Fixture.make(Account)
        .given([Aggregate.historical(Account, "AccountOpened", 1, { owner: "Alice Rossi" })])
        .when("deposit", { amount: 10 })

      assert.deepStrictEqual(outcome.state, { name: "Alice Rossi", currency: "CHF", balance: 10 })
    }))

  it.effect("writes new events in their current version", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      const store = yield* EventStore.EventStore
      yield* store.append({
        aggregateName: "account",
        aggregateId: "a",
        expectedVersion: 0,
        events: [{ type: "AccountOpened", schemaVersion: 3, payload: { ...opened } }]
      })
      yield* accounts.commands.deposit("a", { amount: 5 })

      const events = yield* readAll
      assert.deepStrictEqual(events.map((event) => [event.type, event.schemaVersion]), [
        ["AccountOpened", 3],
        // Events without a history are at version 1.
        ["MoneyDeposited", 1]
      ])
      assert.deepStrictEqual(yield* accounts.readModel("a"), { name: "Alice Rossi", currency: "EUR", balance: 5 })
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("fails with a typed error for a version the aggregate doesn't know", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* EventStore.EventStore.use((store) =>
        store.append({
          aggregateName: "account",
          aggregateId: "a",
          expectedVersion: 0,
          events: [{ type: "AccountOpened", schemaVersion: 4, payload: { ...opened } }]
        })
      )

      const error = yield* Effect.flip(accounts.readModel("a"))
      assert.strictEqual(error._tag, "EventDecodeError")
      assert.strictEqual((error as AggregateService.EventDecodeError).schemaVersion, 4)
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("fails with a typed error when an old payload doesn't fit its version", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(
        Fixture.make(Account).given([
          new Aggregate.Historical({ type: "AccountOpened", schemaVersion: 1, payload: { name: "no owner" } })
        ]).state
      )

      assert.strictEqual(error._tag, "EventDecodeError")
    }))

  it.effect("hands view models the current shape, from a store on SQLite", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const seen: Array<unknown> = []
      const Names = ViewModel.make({
        name: "names",
        aggregates: [Account],
        handlers: { AccountOpened: (event) => Effect.sync(() => void seen.push(event)) },
        dispose: Effect.void
      })
      const old = Aggregate.historical(Account, "AccountOpened", 1, { owner: "Carla Verdi" })
      yield* store.append({ aggregateName: "account", aggregateId: "c", expectedVersion: 0, events: [old] })

      yield* Effect.gen(function*() {
        yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp())
      }).pipe(Effect.provide(ViewModel.layer([Names]).pipe(Layer.provide(Checkpoints.layerMemory))))

      assert.deepStrictEqual(seen, [
        AccountEvent.cases.AccountOpened.make({ firstName: "Carla", lastName: "Verdi", currency: "CHF" })
      ])
    }).pipe(Effect.provide(SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" }))))))

  it("checks a historical payload against the schema of its version", () => {
    assert.throws(() =>
      Aggregate.historical(Account, "AccountOpened", 1, { owner: 42 } as unknown as { owner: string })
    )
    assert.throws(() => Aggregate.historical(Account, "AccountOpened", 9 as 1, { owner: "x" }))
  })
})
