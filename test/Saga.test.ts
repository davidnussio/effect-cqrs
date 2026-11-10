import { assert, describe, it } from "@effect/vitest"
import { Cause, Context, Effect, Exit, Layer, Ref, Schema, Stream } from "effect"
import { AggregateService, Checkpoints, EventStore, Ids, PersonalData, Saga, ViewModel } from "effect-cqrs"
import { WorkflowEngine } from "effect/workflow"
import { Account, AccountNotOpen } from "../examples/bank-ui/src/domain/account.ts"

class Bank extends Context.Service<Bank, AggregateService.AggregateService<typeof Account>>()("test/Bank") {}

const BankLayer = Layer.effect(Bank, AggregateService.make(Account))

class Runs extends Context.Service<Runs, Ref.Ref<number>>()("test/Runs") {}

const Transfer = Saga.workflow("Transfer", {
  payload: { transferId: Schema.String, from: Schema.String, to: Schema.String, amount: Schema.Int },
  idempotencyKey: ({ transferId }) => transferId
})

// The demo's saga: credit the recipient, and refund the sender when it is refused.
const TransferLayer = Saga.toLayer(Transfer, ({ amount, from, to, transferId }) =>
  Effect.gen(function*() {
    const bank = yield* Bank
    yield* Ref.update(yield* Runs, (n) => n + 1)
    yield* Saga.command("credit", {
      error: AccountNotOpen,
      execute: bank.commands.receiveTransfer(to, { transferId, from, amount })
    }).pipe(
      Effect.catchTag("AccountNotOpen", () =>
        Saga.command("refund", {
          execute: bank.commands.refundTransfer(from, { transferId, to, amount, reason: "AccountNotOpen" })
        }))
    )
  }))

const Transfers = Saga.make({
  name: "transfers",
  aggregates: [Account],
  handlers: {
    TransferSent: ({ amount, to, transferId }, stored) =>
      Saga.start(Transfer, { transferId, from: stored.aggregateId, to, amount })
  }
})

const Base = TransferLayer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provideMerge(BankLayer),
  Layer.provideMerge(Layer.mergeAll(
    EventStore.layerMemory,
    Checkpoints.layerMemory,
    // The account's owner is personal data.
    PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral)),
    Layer.effect(Runs, Ref.make(0))
  ))
)

// `ViewModel.layer` exposes the trigger to `replay` and `caughtUp`.
const App = ViewModel.layer([Transfers]).pipe(Layer.provideMerge(Base))

const Background = Saga.layer([Transfers]).pipe(Layer.provideMerge(Base))

const open = (id: string, deposit: number) =>
  Effect.gen(function*() {
    const bank = yield* Bank
    yield* bank.commands.openAccount(id, { owner: id, color: "" })
    if (deposit > 0) yield* bank.commands.deposit(id, { amount: deposit })
  })

/** Waits for the first event of this type, however long the saga takes to record it. */
const next = (type: string) =>
  EventStore.EventStore.use((store) => store.subscribe({ eventTypes: [type] }).pipe(Stream.runHead))

describe("Saga", () => {
  it.effect("runs the workflow for each trigger event", () =>
    Effect.gen(function*() {
      const bank = yield* Bank
      yield* open("alice", 100)
      yield* open("bob", 0)

      const sent = yield* bank.commands.sendTransfer("alice", { transferId: "t1", to: "bob", amount: 30 })
      yield* next("TransferReceived")

      assert.strictEqual((yield* bank.readModel("alice")).balance, 70)
      assert.strictEqual((yield* bank.readModel("bob")).balance, 30)

      // The credit records the transfer as its cause, in the correlation it started.
      const received = (yield* bank.history("bob")).find((entry) => entry.type === "TransferReceived")
      assert.strictEqual(received!.correlationId, sent.correlationId)
      assert.strictEqual(received!.causationId, EventStore.eventId({ ...sent, aggregateName: Account.name }))
    }).pipe(Effect.provide(Background)))

  it.effect("starts its own correlation with the current IdGenerator outside any causation", () =>
    Effect.gen(function*() {
      const bank = yield* Bank
      yield* open("alice", 100)
      yield* open("bob", 0)

      yield* Saga.start(Transfer, { transferId: "t1", from: "alice", to: "bob", amount: 30 }).pipe(
        Effect.provideService(Ids.IdGenerator, Ids.sequential("saga"))
      )
      yield* next("TransferReceived")

      const received = (yield* bank.history("bob")).find((entry) => entry.type === "TransferReceived")
      assert.strictEqual(received!.correlationId, "saga-1")
    }).pipe(Effect.provide(Base)))

  it.effect("compensates when a step fails with a domain error", () =>
    Effect.gen(function*() {
      const bank = yield* Bank
      yield* open("alice", 100)

      yield* bank.commands.sendTransfer("alice", { transferId: "t1", to: "nobody", amount: 30 })
      const refunded = yield* next("TransferRefunded")

      assert.strictEqual(refunded._tag === "Some" && refunded.value.aggregateId, "alice")
      assert.strictEqual((yield* bank.readModel("alice")).balance, 100)
    }).pipe(Effect.provide(Background)))

  it.effect("joins the execution when the trigger sees an event again", () =>
    Effect.gen(function*() {
      const bank = yield* Bank
      const viewModels = yield* ViewModel.ViewModels
      const runs = yield* Runs
      yield* open("alice", 100)
      yield* open("bob", 0)

      yield* bank.commands.sendTransfer("alice", { transferId: "t1", to: "bob", amount: 30 })
      yield* next("TransferReceived")
      // Replaying the trigger starts the saga again for every transfer.
      yield* viewModels.replay({ viewModels: ["transfers"] })
      yield* viewModels.caughtUp()

      // Executing again joins the finished run instead of starting another.
      const causation = { correlationId: "c", causationId: "c" }
      yield* Transfer.execute({ transferId: "t1", from: "alice", to: "bob", amount: 30, causation })
      assert.strictEqual(yield* Ref.get(runs), 1)
      assert.strictEqual((yield* bank.readModel("bob")).balance, 30)
    }).pipe(Effect.provide(App)))

  it.effect("ends the workflow on an unexpected failure and runs its compensations", () =>
    Effect.gen(function*() {
      const compensated = yield* Ref.make<ReadonlyArray<string>>([])
      const Steps = Saga.workflow("Steps", { payload: { id: Schema.String }, idempotencyKey: ({ id }) => id })
      const StepsLayer = Saga.toLayer(Steps, () =>
        Effect.gen(function*() {
          const bank = yield* Bank
          yield* Saga.command("deposit", { execute: bank.commands.deposit("alice", { amount: 10 }) }).pipe(
            Steps.withCompensation(() =>
              Saga.command("undo deposit", { execute: bank.commands.withdraw("alice", { amount: 10 }) }).pipe(
                Effect.andThen(Ref.update(compensated, (steps) => [...steps, "deposit"]))
              )
            )
          )
          // No such account, and the activity doesn't expect `AccountNotOpen`.
          yield* Saga.command("withdraw", { execute: bank.commands.withdraw("nobody", { amount: 10 }) })
        }))

      yield* open("alice", 0)
      const exit = yield* Steps.execute({ id: "s1", causation: { correlationId: "c", causationId: "c" } }).pipe(
        Effect.provide(StepsLayer),
        Effect.exit
      )

      assert.isTrue(Exit.isFailure(exit) && Cause.hasDies(exit.cause))
      assert.deepStrictEqual(yield* Ref.get(compensated), ["deposit"])
      assert.strictEqual((yield* Bank.use((bank) => bank.readModel("alice"))).balance, 0)
    }).pipe(Effect.provide(App)))
})
