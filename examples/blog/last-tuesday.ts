/**
 * The post "What it looked like last Tuesday": Bob was refused a book that is on the
 * shelf today. The log answers why: the book's state at the moment of the refusal, and
 * every event of the request that refused him. `last-tuesday.test.ts` runs it, with the
 * days passing in between.
 */
import { DateTime, Effect, Stream } from "effect"
import { EventStore } from "effect-cqrs"
import { Books, Members, settled } from "./library.ts"

/** Tuesday: Alice borrows Dune, and Bob asks for it a moment later. */
export const tuesday = Effect.gen(function*() {
  const books = yield* Books
  const members = yield* Members

  // #region tuesday
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* members.commands.join("alice", { name: "Alice Rossi" })
  yield* members.commands.join("bob", { name: "Bob Bianchi" })

  yield* members.commands.requestLoan("alice", { loanId: "loan-1", bookId: "book-1" })
  yield* settled("alice", "loan-1")
  yield* members.commands.requestLoan("bob", { loanId: "loan-2", bookId: "book-1" })
  yield* settled("bob", "loan-2")
  // #endregion
})

/** Friday: Alice brings the book back. */
export const friday = Effect.gen(function*() {
  const books = yield* Books
  yield* books.commands.returnBook("book-1", {})
})

/** The following Monday: Bob asks why. */
export const investigate = Effect.gen(function*() {
  const books = yield* Books
  const members = yield* Members
  const store = yield* EventStore.EventStore

  // #region what-bob-sees
  const today = yield* books.state("book-1")
  // { status: "available", loan: null, … }: on the shelf
  // #endregion

  // #region bobs-history
  const bob = yield* members.history("bob", { payload: true })
  const refusal = bob.find(({ type }) => type === "LoanCancelled")!
  // { type: "LoanCancelled", payload: { loanId: "loan-2", reason: "BookAlreadyLent" },
  //   timestamp: Tuesday, causationId: "member/bob@2", correlationId: "…" }
  // #endregion

  // #region back-then
  // The book as it was when Bob was refused. `finishTime` leaves out events from that
  // instant on, so take the next millisecond.
  const backThen = yield* books.state("book-1", { finishTime: DateTime.add(refusal.timestamp, { milliseconds: 1 }) })
  // { status: "lent", loan: { loanId: "loan-1", memberId: "alice", dueOn: … }, … }
  // #endregion

  // #region one-request
  // Everything one request caused, across aggregates, by its correlation id.
  const causedBy = (correlationId: string | null) =>
    store.read().pipe(
      Stream.filter((event) => event.correlationId === correlationId),
      Stream.map(({ aggregateId, aggregateName, type, version }) =>
        `${aggregateName}/${aggregateId}@${version} ${type}`
      ),
      Stream.runCollect
    )

  const bobsRequest = yield* causedBy(refusal.correlationId)
  // ["member/bob@2 LoanRequested", "member/bob@3 LoanCancelled"]

  const alicesRequest = yield* causedBy(
    (yield* members.history("alice")).find(({ type }) => type === "LoanRequested")!.correlationId
  )
  // ["member/alice@2 LoanRequested", "book/book-1@2 BookLent", "member/alice@3 LoanConfirmed"]
  // #endregion

  return { today, refusal, backThen, bobsRequest, alicesRequest }
})
