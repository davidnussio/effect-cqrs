import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect } from "effect"
import { TestClock } from "effect/testing"
import { autumn, Library, summer } from "./change-the-rules.ts"

const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso))

describe("change the rules, keep the history", () => {
  it.effect("keeps August's decisions and applies September's rules to new ones", () =>
    Effect.gen(function*() {
      yield* TestClock.setTime(at("2026-08-20T09:00:00Z"))
      yield* summer
      yield* TestClock.setTime(at("2026-09-02T09:00:00Z"))
      const { alice, borrowed, refused } = yield* autumn

      const dueInSeptember = DateTime.makeUnsafe("2026-09-17T09:00:00Z")
      assert.deepStrictEqual(Object.keys(alice.books), ["dune", "emma", "ulysses", "solaris"])
      for (const dueOn of Object.values(alice.books)) assert.isTrue(DateTime.Equivalence(dueOn, dueInSeptember))

      assert.strictEqual(refused._tag, "LoanLimitReached")
      if (refused._tag === "LoanLimitReached") assert.strictEqual(refused.limit, 3)

      const [event] = borrowed.events
      assert.strictEqual(event?._tag, "BookBorrowed")
      if (event?._tag === "BookBorrowed") {
        assert.strictEqual(event.policy, "standard-2026")
        assert.isTrue(DateTime.Equivalence(event.dueOn, DateTime.makeUnsafe("2026-09-23T09:00:00Z")))
      }
    }).pipe(Effect.provide(Library)))
})
