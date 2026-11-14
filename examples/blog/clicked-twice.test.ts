import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { Library, naturally, twice, withCommandId } from "./clicked-twice.ts"

describe("I clicked twice", () => {
  it.effect("takes the same payment twice without a command id", () =>
    Effect.gen(function*() {
      const alice = yield* twice
      assert.deepStrictEqual(alice, { balance: 0 })
    }).pipe(Effect.provide(Library)))

  it.effect("takes it once with a command id, and refuses the id for another command", () =>
    Effect.gen(function*() {
      const { again, alice, first, reused } = yield* withCommandId
      assert.deepStrictEqual(again, first)
      assert.deepStrictEqual(alice, { balance: 300 })
      assert.strictEqual(reused, "CommandIdReused")
    }).pipe(Effect.provide(Library)))

  it.effect("lends a book once for the same loan id", () =>
    Effect.gen(function*() {
      const { again, first } = yield* naturally
      assert.deepStrictEqual(first.events.map(({ _tag }) => _tag), ["BookLent"])
      assert.deepStrictEqual(again.events, [])
    }).pipe(Effect.provide(Library)))
})
