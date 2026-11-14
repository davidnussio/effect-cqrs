import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { Storage, withTheBug, withTheFix } from "./fix-the-code.ts"

describe("fix the code, not the data", () => {
  it.effect("loses Alice's loan with the bug, and gets it back with the fix and a replay", () =>
    Effect.gen(function*() {
      const withBug = yield* withTheBug
      assert.deepStrictEqual(withBug, [])

      const { afterReplay, beforeReplay, eventCount } = yield* withTheFix
      assert.deepStrictEqual(beforeReplay, [])
      assert.strictEqual(eventCount, 3)
      assert.deepStrictEqual(afterReplay, [
        { loanId: "loan-1", bookId: "book-1", memberId: "alice", returned: true }
      ])
    }).pipe(Effect.provide(Storage)))
})
