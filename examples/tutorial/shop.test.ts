import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Metric, Ref, References } from "effect"
import { TestClock } from "effect/testing"
import { OrderList, Orders, program, reached, settled, Shop, Stocks } from "./shop.ts"

describe("tutorial", () => {
  it.effect("runs the shop as the tutorial describes it", () =>
    Effect.gen(function*() {
      const result = yield* program.pipe(Effect.provide(Shop))

      assert.deepStrictEqual(result.orders, {
        "order-0": "rejected: only 0 left",
        "order-1": "confirmed",
        "order-2": "rejected: only 2 left"
      })
      assert.deepStrictEqual(result.stock, { available: 2, reserved: { "order-1": 3 } })
      assert.deepStrictEqual(result.legacy, { status: "rejected", sku: "pear", quantity: 1, payment: null })
      assert.strictEqual(result.rejected, "OrderAlreadyPlaced")
      assert.deepStrictEqual(result.history.map(({ metadata, type }) => [type, metadata]), [
        ["OrderPlaced", { appVersion: "1.0.0", userId: "bob", channel: "mobile" }],
        ["OrderRejected", { appVersion: "1.0.0" }]
      ])
      assert.deepStrictEqual(result.timings.map(({ count, outcome }) => [outcome, count]), [
        ["success", 2],
        ["OrderAlreadyPlaced", 1]
      ])
    }).pipe(
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(References.MinimumLogLevel, "None")
    ))

  it.effect("rejects an order that isn't paid within 15 seconds and gives its stock back", () =>
    Effect.gen(function*() {
      const orders = yield* Orders
      const stocks = yield* Stocks
      yield* stocks.commands.add("apple", { quantity: 5 })
      yield* orders.commands.place("order-1", { sku: "apple", quantity: 3 })
      yield* reached("order-1", "PaymentRequested")
      assert.deepStrictEqual(yield* stocks.readModel("apple"), { available: 2, reserved: { "order-1": 3 } })

      // The saga starts its clock just after asking for the payment, so move time until it gives up.
      const done = yield* Effect.forkChild(settled("order-1"))
      let seconds = 0
      while (seconds < 600 && done.pollUnsafe() === undefined) {
        yield* TestClock.adjust("1 second")
        seconds++
      }
      yield* Fiber.join(done)
      assert.isAtLeast(seconds, 15)
      assert.isAtMost(seconds, 16)

      assert.strictEqual((yield* orders.readModel("order-1")).status, "rejected")
      assert.deepStrictEqual(yield* stocks.readModel("apple"), { available: 5, reserved: {} })
      const list = yield* OrderList.use(Ref.get)
      assert.strictEqual(list.get("order-1"), "rejected: not paid in time")

      const late = yield* Effect.flip(orders.commands.pay("order-1", {}))
      assert.strictEqual(late._tag, "OrderNotPayable")
    }).pipe(
      Effect.provide(Shop),
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provideService(References.MinimumLogLevel, "None")
    ))
})
