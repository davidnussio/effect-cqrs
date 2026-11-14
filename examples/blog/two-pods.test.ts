import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { Shared, sharedLeases, twoReplicas } from "./two-pods.ts"

const alice = { to: "alice", subject: "You borrowed book-1" }
const bob = { to: "bob", subject: "You borrowed book-2" }

describe("two pods, two emails", () => {
  it.live("emails every member twice when both replicas run the outbox", () =>
    Effect.gen(function*() {
      const { afterOneLoan } = yield* twoReplicas
      assert.deepStrictEqual(afterOneLoan, [alice, alice])
    }).pipe(Effect.provide(Shared)))

  it.live("emails once with leases, and the standby takes over when the holder stops", () =>
    Effect.gen(function*() {
      const { afterFailover, afterOneLoan } = yield* twoReplicas
      assert.deepStrictEqual(afterOneLoan, [alice])
      assert.deepStrictEqual(afterFailover, [alice, bob])
    }).pipe(Effect.provide(Layer.merge(Shared, sharedLeases))))
})
