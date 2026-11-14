import { assert, describe, it } from "@effect/vitest"
import { Effect, References } from "effect"
import { inOneRequest, Library, withOutbox, WithoutOutbox } from "./double-write.ts"

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

describe("the double write", () => {
  it.effect("loses the message or sends a false one when one request stores and sends", () =>
    Effect.gen(function*() {
      const result = yield* inOneRequest
      assert.strictEqual(result.failed, "BrokerDown")
      assert.strictEqual(result.book.loan?.memberId, "alice")
      assert.deepStrictEqual(result.afterStoring, [])
      assert.strictEqual(result.refused, "BookAlreadyLent")
      assert.deepStrictEqual(result.afterSending, [
        { id: null, type: "library.book-lent.v1", payload: { bookId: "book-1", loanId: "loan-2", memberId: "bob" } }
      ])
    }).pipe(Effect.provide(WithoutOutbox)))

  // The outbox waits before trying again, so this one runs on the real clock.
  it.live("publishes the loan once the broker is back, with the outbox", () =>
    Effect.gen(function*() {
      const { received } = yield* withOutbox
      assert.deepStrictEqual(received, [
        {
          id: "book/book-1@2",
          type: "library.book-lent.v1",
          payload: { bookId: "book-1", loanId: "loan-1", memberId: "alice" }
        }
      ])
    }).pipe(Effect.provide(Library), quiet))
})
