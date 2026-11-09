import { assert, describe, it } from "@effect/vitest"
import { Effect, Metric, References } from "effect"
import { program, Shop } from "../examples/tutorial/shop.ts"

describe("tutorial", () => {
  it.effect("runs the shop as the tutorial describes it", () =>
    Effect.gen(function*() {
      const result = yield* program.pipe(Effect.provide(Shop))

      assert.deepStrictEqual(result.orders, {
        "order-0": "rejected: only 0 left",
        "order-1": "confirmed",
        "order-2": "rejected: only 2 left"
      })
      assert.deepStrictEqual(result.legacy, { status: "rejected", sku: "pear", quantity: 1 })
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
})
