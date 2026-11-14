import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { Library, sameField, whatChanged, wholeForm } from "./form-overwrites.ts"

describe("the form that overwrites", () => {
  it.effect("loses an edit when both librarians save the whole form", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(yield* wholeForm, { title: "Dune", author: "Frank Herbet", shelf: "B1" })
    }).pipe(Effect.provide(Library)))

  it.effect("keeps both edits when each command says what changed", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(yield* whatChanged, { title: "Dune", author: "Frank Herbert", shelf: "B1" })
    }).pipe(Effect.provide(Library)))

  it.effect("refuses the second edit of the same field from an old screen", () =>
    Effect.gen(function*() {
      const { dune, refused } = yield* sameField
      assert.strictEqual(refused._tag, "WrongExpectedVersion")
      if (refused._tag === "WrongExpectedVersion") {
        assert.strictEqual(refused.expectedVersion, 1)
        assert.strictEqual(refused.actualVersion, 2)
      }
      assert.strictEqual(dune.title, "Dune (Deluxe Edition)")
    }).pipe(Effect.provide(Library)))
})
