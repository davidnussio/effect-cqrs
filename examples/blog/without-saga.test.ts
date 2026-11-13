import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Option, References } from "effect"
import { EventStore } from "effect-cqrs"
import { Books } from "./library.ts"
import { callingFails, callingWorks, failingOnce, greedy, LibraryFailingOnce, withSaga } from "./without-saga.ts"

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

const booksOn = <E>(store: Layer.Layer<EventStore.EventStore, E>) => Books.layer.pipe(Layer.provideMerge(store))

describe("the saga you skipped", () => {
  it.effect("lends one copy twice when the member records the book's event", () =>
    Effect.gen(function*() {
      const { book } = yield* greedy.pipe(Effect.provide(booksOn(EventStore.layerMemory)))
      assert.strictEqual(book.status, "available")
      assert.isNull(book.loan)
    }))

  it.effect("refuses a book that is out when the member calls the book's command", () =>
    Effect.gen(function*() {
      const { refused } = yield* callingWorks.pipe(Effect.provide(booksOn(EventStore.layerMemory)))
      assert.strictEqual(refused, "BookAlreadyLent")
    }))

  it.effect("leaves the book lent and the member without a loan when the second write fails", () =>
    Effect.gen(function*() {
      const result = yield* callingFails.pipe(Effect.provide(booksOn(failingOnce("LoanStarted"))))
      assert.strictEqual(result.failure, "EventStoreError")
      assert.strictEqual(result.book.status, "lent")
      assert.strictEqual(result.book.loan?.memberId, "carol")
      assert.deepStrictEqual(result.carol, { joined: true, loans: [] })
    }))

  // The saga waits before trying a step again, so this one runs on the real clock.
  it.live("settles the loan with the saga when the same write fails", () =>
    Effect.gen(function*() {
      const result = yield* withSaga.pipe(Effect.provide(LibraryFailingOnce))
      assert.deepStrictEqual(result.outcome, Option.some("LoanConfirmed"))
      assert.strictEqual(result.book.loan?.memberId, "carol")
      assert.deepStrictEqual(result.carol.loans, { "loan-3": { bookId: "book-2", status: "active" } })
    }).pipe(quiet))
})
