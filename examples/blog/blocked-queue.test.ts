import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, References } from "effect"
import { parked, retried, stalled, Storage } from "./blocked-queue.ts"

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

// View models wait before trying a failed event again, so these run on the real clock.
describe("the message that blocks the queue", () => {
  it.live("stops the shelf marks at Homer, and nothing else", () =>
    Effect.gen(function*() {
      const { catalog, marks, status } = yield* stalled
      assert.strictEqual(status?.position, 1)
      assert.isTrue(Option.isSome(status?.lastError ?? Option.none()))
      assert.deepStrictEqual([...marks], [["dune", "HER"]])
      assert.deepStrictEqual([...catalog.keys()], ["dune", "odyssey", "neuromancer"])
    }).pipe(Effect.provide(Storage), quiet))

  it.live("parks Homer, carries on, and handles him once the fix is deployed", () =>
    Effect.gen(function*() {
      const before = yield* parked
      assert.deepStrictEqual([...before.marks], [["dune", "HER"], ["neuromancer", "GIB"]])
      assert.deepStrictEqual(before.parked.map(({ attempts, eventId }) => ({ eventId, attempts })), [
        { eventId: "book/odyssey@1", attempts: 3 }
      ])

      const after = yield* retried
      assert.deepStrictEqual(after.result.succeeded, [2])
      assert.deepStrictEqual(after.result.failed, [])
      assert.strictEqual(after.marks.get("odyssey"), "HOM")
    }).pipe(Effect.provide(Storage), quiet))
})
