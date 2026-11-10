import { assert, describe, it } from "@effect/vitest"
import { Arbitrary, Cause, DateTime, Effect, Exit, Schema } from "effect"
import { Aggregate, Fixture } from "effect-cqrs"
import { Todo, TodoEvent } from "../examples/todo/aggregate.ts"

const at = DateTime.makeUnsafe(0)
const created = TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: at })
const deleted = TodoEvent.cases.TodoDeleted.make({ message: "Done", deletedAt: at })

const defectOf = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.map(Effect.exit(effect), (exit) => Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined)

const ClockEvent = Schema.TaggedUnion({ Tick: {}, Tock: {} })
const Clock = Aggregate.make<number>()({
  name: "clock",
  events: ClockEvent,
  initial: 0,
  projection: { Tick: (n) => n + 1, Tock: (n) => n + 1 }
}).command("cycle", {
  payload: Schema.Struct({}),
  handler: () => Effect.succeed([ClockEvent.cases.Tick.make({}), ClockEvent.cases.Tock.make({})])
})

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

  describe("chain", () => {
    it.effect("reads like the moleculer-cqrs fixture", () =>
      Fixture.make(Todo)
        .givenEvents()
        .when("createTodo", { title: "Buy Milk" })
        .expectEvent(created)
        .inspectState((state) => assert.deepStrictEqual(state, { status: "open", title: "Buy Milk" })))

    it.effect("still yields the outcome", () =>
      Effect.gen(function*() {
        const outcome = yield* Fixture.make(Todo)
          .givenEvents([created])
          .when("deleteTodo", { message: "Done" })
          .expectEvent(deleted)

        assert.strictEqual(outcome.version, 2)
      }))

    it.effect("runs nothing until it is yielded", () =>
      Effect.gen(function*() {
        let inspected = 0
        const scenario = Fixture.make(Todo)
          .when("createTodo", { title: "Buy Milk" })
          .inspectState(() => inspected++)

        assert.strictEqual(inspected, 0)
        yield* scenario
        assert.strictEqual(inspected, 1)
      }))

    it.effect("can run the same scenario twice, each from the given events", () =>
      Effect.gen(function*() {
        const scenario = Fixture.make(Todo).givenEvents([created]).when("completeTodo", {})

        assert.strictEqual((yield* scenario).version, 2)
        assert.strictEqual((yield* scenario).version, 2)
      }))

    it.effect("checks each command against its own events", () =>
      Fixture.make(Todo)
        .when("createTodo", { title: "Buy Milk" })
        .expectEvent("TodoCreated")
        .when("deleteTodo", { message: "Done" })
        .expectEvent(deleted)
        .inspectState((state) => assert.strictEqual(state.status, "deleted")))

    it.effect("inspects the state of the given events without a command", () =>
      Fixture.make(Todo)
        .givenEvents([created])
        .inspectState((state) => assert.strictEqual(state.status, "open"))
        .when("completeTodo", {})
        .expectEvent("TodoCompleted"))

    it.effect("switches aggregate id with givenEvents", () =>
      Effect.gen(function*() {
        const error = yield* Fixture.make(Todo).givenEvents([], "custom-id").whenFails("deleteTodo", { message: "x" })

        assert.deepStrictEqual(error._tag === "TodoNotFound" && error.aggregateId, "custom-id")
      }))

    it.effect("expects a failure at the end of a chain", () =>
      Effect.gen(function*() {
        const error = yield* Fixture.make(Todo)
          .when("createTodo", { title: "Buy Milk" })
          .when("deleteTodo", { message: "Done" })
          .whenFails("deleteTodo", { message: "Again" })

        assert.strictEqual(error._tag, "TodoAlreadyDeleted")
      }))

    it.effect("stops at a failing command, keeping its typed error", () =>
      Effect.gen(function*() {
        let inspected = false
        const error = yield* Fixture.make(Todo)
          .when("deleteTodo", { message: "Done" })
          .inspectState(() => {
            inspected = true
          })
          .pipe(Effect.flip)

        assert.strictEqual(error._tag, "TodoNotFound")
        assert.isFalse(inspected)
      }))

    it.effect("accepts several events in order", () =>
      Fixture.make(Clock)
        .when("cycle", {})
        .expectEvents([ClockEvent.cases.Tick.make({}), "Tock"]))

    it.effect("accepts no events from a command that records nothing", () =>
      Fixture.make(Todo)
        .givenEvents([created])
        .when("completeTodo", {})
        .when("completeTodo", {})
        .expectEvents([]))
  })

  describe("failed expectations", () => {
    const createMilk = Fixture.make(Todo).when("createTodo", { title: "Buy Milk" })

    it.effect("reports what was expected and what happened", () =>
      Effect.gen(function*() {
        const expected = TodoEvent.cases.TodoCreated.make({ title: "Buy Eggs", createdAt: at })

        const defect = yield* defectOf(createMilk.expectEvent(expected))

        assert.instanceOf(defect, Fixture.ExpectationFailed)
        assert.deepStrictEqual((defect as Fixture.ExpectationFailed).expected, [expected])
        assert.deepStrictEqual((defect as Fixture.ExpectationFailed).actual, [created])
      }))

    it.effect("compares dates by value", () =>
      Effect.gen(function*() {
        const later = TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: DateTime.makeUnsafe(1000) })
        const same = TodoEvent.cases.TodoCreated.make({ title: "Buy Milk", createdAt: DateTime.makeUnsafe(0) })

        assert.instanceOf(yield* defectOf(createMilk.expectEvent(later)), Fixture.ExpectationFailed)
        assert.isUndefined(yield* defectOf(createMilk.expectEvent(same)))
      }))

    it.effect("rejects another event's tag", () =>
      Effect.gen(function*() {
        assert.instanceOf(yield* defectOf(createMilk.expectEvent("TodoDeleted")), Fixture.ExpectationFailed)
      }))

    it.effect("rejects a command that emitted nothing", () =>
      Effect.gen(function*() {
        const noop = Fixture.make(Todo).givenEvents([created]).when("completeTodo", {}).when("completeTodo", {})

        assert.instanceOf(yield* defectOf(noop.expectEvent("TodoCompleted")), Fixture.ExpectationFailed)
      }))

    it.effect("rejects extra, missing and reordered events", () =>
      Effect.gen(function*() {
        const cycle = Fixture.make(Clock).when("cycle", {})

        assert.instanceOf(yield* defectOf(cycle.expectEvent("Tick")), Fixture.ExpectationFailed)
        assert.instanceOf(yield* defectOf(cycle.expectEvents(["Tick"])), Fixture.ExpectationFailed)
        assert.instanceOf(yield* defectOf(cycle.expectEvents(["Tick", "Tock", "Tock"])), Fixture.ExpectationFailed)
        assert.instanceOf(yield* defectOf(cycle.expectEvents(["Tock", "Tick"])), Fixture.ExpectationFailed)
      }))

    it.effect("surfaces what inspectState throws", () =>
      Effect.gen(function*() {
        const failure = new Error("wrong state")

        const defect = yield* defectOf(
          createMilk.inspectState(() => {
            throw failure
          })
        )

        assert.strictEqual(defect, failure)
      }))
  })

  describe("checkInvariants", () => {
    const Amount = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))
    const WalletEvent = Schema.TaggedUnion({ Deposited: { amount: Amount }, Withdrawn: { amount: Amount } })
    const wallet = (canWithdraw: (balance: number, amount: number) => boolean) =>
      Aggregate.make<number>()({
        name: "wallet",
        events: WalletEvent,
        initial: 0,
        projection: {
          Deposited: (balance, { amount }) => balance + amount,
          Withdrawn: (balance, { amount }) => balance - amount
        }
      })
        .command("deposit", {
          payload: Schema.Struct({ amount: Amount }),
          handler: (_, { amount }) => Effect.succeed(WalletEvent.cases.Deposited.make({ amount }))
        })
        .command("withdraw", {
          payload: Schema.Struct({ amount: Amount }),
          handler: (balance, { amount }) =>
            canWithdraw(balance, amount)
              ? Effect.succeed(WalletEvent.cases.Withdrawn.make({ amount }))
              : Effect.fail("Insufficient funds" as const)
        })

    const neverNegative = (balance: number) => balance >= 0

    it.effect("passes when no sequence of commands breaks the invariant", () =>
      Fixture.make(wallet((balance, amount) => balance >= amount)).checkInvariants({ invariant: neverNegative }))

    it.effect("dies with the shortest sequence it found that breaks the invariant", () =>
      Effect.gen(function*() {
        const defect = yield* defectOf(
          Fixture.make(wallet((balance) => balance > 0)).checkInvariants({
            invariant: neverNegative,
            check: { seed: 1 }
          })
        )

        assert.instanceOf(defect, Fixture.ExpectationFailed)
        const steps = (defect as Fixture.ExpectationFailed).actual as ReadonlyArray<{ readonly command: string }>
        // A deposit and a larger withdrawal are as short as it gets.
        assert.deepStrictEqual(steps.map((step) => step.command), ["deposit", "withdraw"])
      }))

    it.effect("starts from the given history", () =>
      Effect.gen(function*() {
        const defect = yield* defectOf(
          Fixture.make(wallet((balance) => balance > 0))
            .given([WalletEvent.cases.Deposited.make({ amount: 1 })])
            .checkInvariants({ invariant: neverNegative, commands: { deposit: false }, check: { seed: 1 } })
        )

        const steps = (defect as Fixture.ExpectationFailed).actual as ReadonlyArray<{ readonly command: string }>
        assert.deepStrictEqual(steps.map((step) => step.command), ["withdraw"])
      }))

    it.effect("takes payloads from the arbitraries it is given", () =>
      Fixture.make(wallet((balance) => balance > 0)).checkInvariants({
        invariant: neverNegative,
        // Withdrawing nothing but single units, a positive balance can't go below zero.
        commands: { withdraw: Arbitrary.Constant({ amount: 1 }) }
      }))
  })
})
