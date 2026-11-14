import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { AggregateService } from "effect-cqrs"
import { TestClock } from "effect/testing"
import { Book, correcting, Library } from "./wrong-event.ts"

describe("the wrong event", () => {
  it.effect("corrects the due dates the bug stored, with an event of its own", () =>
    Effect.gen(function*() {
      const { afterCorrection, beforeCorrection, history } = yield* correcting
      assert.deepStrictEqual(beforeCorrection, ["loan-1"])
      assert.deepStrictEqual(afterCorrection, [])
      assert.deepStrictEqual(history.map(({ type }) => type), ["BookAdded", "BookLent", "DueDateCorrected"])
      assert.deepStrictEqual(history[2]?.metadata, { ranBy: "scripts/fix-due-dates.ts" })

      // Alice brings the book back ten days later, on time by the corrected date.
      yield* TestClock.adjust("10 days")
      const books = yield* AggregateService.make(Book)
      const returned = yield* books.commands.returnBook("book-1", {})
      assert.deepStrictEqual(returned.events.map((event) => event._tag === "BookReturned" && event.late), [false])
    }).pipe(Effect.provide(Library)))

  it.effect("records nothing when the same correction runs twice", () =>
    Effect.gen(function*() {
      yield* correcting
      const books = yield* AggregateService.make(Book)
      const loan = (yield* books.state("book-1")).loan
      assert.isNotNull(loan)
      const dueOn = loan.dueOn
      const again = yield* books.commands.correctDueDate("book-1", { loanId: "loan-1", dueOn, reason: "again" })
      assert.deepStrictEqual(again.events, [])
    }).pipe(Effect.provide(Library)))
})
