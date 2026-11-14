import { assert, describe, it } from "@effect/vitest"
import { Effect, References } from "effect"
import { TestClock } from "effect/testing"
import { friday, investigate, tuesday } from "./last-tuesday.ts"
import { Library } from "./library.ts"

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

describe("what it looked like last Tuesday", () => {
  it.effect("explains a refusal from the state back then and the events of one request", () =>
    Effect.gen(function*() {
      yield* tuesday
      yield* TestClock.adjust("3 days")
      yield* friday
      yield* TestClock.adjust("3 days")
      const result = yield* investigate

      assert.strictEqual(result.today.status, "available")
      assert.deepStrictEqual(result.refusal.payload, {
        _tag: "LoanCancelled",
        loanId: "loan-2",
        reason: "BookAlreadyLent"
      })
      assert.strictEqual(result.refusal.causationId, "member/bob@2")
      assert.strictEqual(result.backThen.status, "lent")
      assert.strictEqual(result.backThen.loan?.memberId, "alice")
      assert.deepStrictEqual(result.bobsRequest, ["member/bob@2 LoanRequested", "member/bob@3 LoanCancelled"])
      assert.deepStrictEqual(result.alicesRequest, [
        "member/alice@2 LoanRequested",
        "book/book-1@2 BookLent",
        "member/alice@3 LoanConfirmed"
      ])
    }).pipe(Effect.provide(Library), quiet))
})
