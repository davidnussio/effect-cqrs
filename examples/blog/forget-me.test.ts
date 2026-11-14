import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { EventStore } from "effect-cqrs"
import { forgetting, Keys } from "./forget-me.ts"

describe("forget me, from the backups too", () => {
  it.effect("keeps names encrypted, and a backup restored after a forget can't read hers", () =>
    Effect.gen(function*() {
      const { alice, restored, stored } = yield* forgetting
      assert.notInclude(JSON.stringify(stored?.payload), "Alice")
      assert.strictEqual(alice.name, "[forgotten]")
      assert.strictEqual(restored.alice.name, "[forgotten]")
      assert.strictEqual(restored.bob.name, "Bob Bianchi")
    }).pipe(Effect.provide(Layer.merge(EventStore.layerMemory, Keys))))
})
