/**
 * The post "The wrong event": a bug in a command stores wrong due dates, and the fix
 * records a correction for each one instead of editing the log. `wrong-event.test.ts`
 * runs it.
 */
import { Context, DateTime, Effect, Layer, Ref, Schema } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, Metadata, ViewModel } from "effect-cqrs"
import { BookAlreadyAdded, BookAlreadyLent, BookNotFound, BookNotLent } from "./library.ts"

// #region events
export const BookEvent = Schema.TaggedUnion({
  BookAdded: { title: Schema.NonEmptyString, author: Schema.NonEmptyString },
  BookLent: { loanId: Schema.String, memberId: Schema.String, dueOn: Schema.DateTimeUtc },
  BookReturned: { loanId: Schema.String, memberId: Schema.String, late: Schema.Boolean },
  // The correction is a fact of its own: what the due date is now, and why it changed.
  DueDateCorrected: { loanId: Schema.String, dueOn: Schema.DateTimeUtc, reason: Schema.String }
})
// #endregion

export class LoanNotFound extends Schema.TaggedError<LoanNotFound>()("LoanNotFound", {
  bookId: Schema.String,
  loanId: Schema.String
}) {}

interface BookState {
  readonly status: "unknown" | "available" | "lent"
  readonly loan: { readonly loanId: string; readonly memberId: string; readonly dueOn: DateTime.Utc } | null
}

// #region book
/** The book of the series, with the loan period as a parameter, so two deploys can differ. */
const makeBook = (loanPeriod: Partial<DateTime.DateTime.PartsForMath>) =>
  Aggregate.make<BookState>()({
    name: "book",
    events: BookEvent,
    initial: { status: "unknown", loan: null },
    projection: {
      BookAdded: (state) => ({ ...state, status: "available" }),
      BookLent: (_, { dueOn, loanId, memberId }) => ({ status: "lent", loan: { loanId, memberId, dueOn } }),
      BookReturned: () => ({ status: "available", loan: null }),
      DueDateCorrected: (state, { dueOn }) => state.loan === null ? state : { ...state, loan: { ...state.loan, dueOn } }
    }
  })
    .command("add", {
      payload: Schema.Struct({ title: Schema.NonEmptyString, author: Schema.NonEmptyString }),
      handler: Effect.fnUntraced(function*(state, payload, { aggregateId }) {
        if (state.status !== "unknown") return yield* new BookAlreadyAdded({ bookId: aggregateId })
        return BookEvent.cases.BookAdded.make(payload)
      })
    })
    .command("lend", {
      payload: Schema.Struct({ loanId: Schema.String, memberId: Schema.String }),
      handler: Effect.fnUntraced(function*(state, { loanId, memberId }, { aggregateId }) {
        if (state.status === "unknown") return yield* new BookNotFound({ bookId: aggregateId })
        if (state.loan !== null) return yield* new BookAlreadyLent({ bookId: aggregateId })
        const dueOn = DateTime.add(yield* DateTime.now, loanPeriod)
        return BookEvent.cases.BookLent.make({ loanId, memberId, dueOn })
      })
    })
    .command("returnBook", {
      payload: Schema.Struct({}),
      handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
        if (state.loan === null) return yield* new BookNotLent({ bookId: aggregateId })
        const { dueOn, loanId, memberId } = state.loan
        const late = DateTime.isGreaterThan(yield* DateTime.now, dueOn)
        return BookEvent.cases.BookReturned.make({ loanId, memberId, late })
      })
    })
    .command("correctDueDate", {
      payload: Schema.Struct({ loanId: Schema.String, dueOn: Schema.DateTimeUtc, reason: Schema.String }),
      handler: Effect.fnUntraced(function*(state, { dueOn, loanId, reason }, { aggregateId }) {
        if (state.loan?.loanId !== loanId) return yield* new LoanNotFound({ bookId: aggregateId, loanId })
        // Correcting it again to the same date records nothing, so the script can run twice.
        if (DateTime.Equivalence(state.loan.dueOn, dueOn)) return []
        return BookEvent.cases.DueDateCorrected.make({ loanId, dueOn, reason })
      })
    })

// The bug: hours where days were meant.
export const BookWithTheBug = makeBook({ hours: 21 })

// The fix.
export const Book = makeBook({ days: 21 })
// #endregion

// #region open-loans
export interface OpenLoan {
  readonly bookId: string
  readonly memberId: string
  readonly lentAt: DateTime.Utc
  readonly dueOn: DateTime.Utc
}

/** The loans still open, by loan id, with when each began and when it is due. */
export class OpenLoans extends Context.Service<OpenLoans, Ref.Ref<ReadonlyMap<string, OpenLoan>>>()(
  "library/OpenLoans"
) {
  static readonly layer = Layer.effect(OpenLoans, Ref.make<ReadonlyMap<string, OpenLoan>>(new Map()))
}

const change = (update: (loans: Map<string, OpenLoan>) => void) =>
  OpenLoans.use((open) =>
    Ref.update(open, (loans) => {
      const next = new Map(loans)
      update(next)
      return next
    })
  )

export const OpenLoansView = ViewModel.make({
  name: "open-loans",
  aggregates: [Book],
  dispose: OpenLoans.use((open) => Ref.set(open, new Map())),
  handlers: {
    BookLent: ({ dueOn, loanId, memberId }, { aggregateId, timestamp }) =>
      change((loans) => loans.set(loanId, { bookId: aggregateId, memberId, lentAt: timestamp, dueOn })),
    BookReturned: ({ loanId }) => change((loans) => loans.delete(loanId)),
    DueDateCorrected: ({ dueOn, loanId }) =>
      change((loans) => {
        const loan = loans.get(loanId)
        if (loan !== undefined) loans.set(loanId, { ...loan, dueOn })
      })
  }
})

/** The loans that are overdue at a given time, as the reminder job asks for them. */
export const overdueAt = (at: DateTime.Utc) =>
  OpenLoans.use((open) =>
    Ref.get(open).pipe(
      Effect.map((loans) =>
        [...loans].filter(([, { dueOn }]) => DateTime.isGreaterThan(at, dueOn)).map(([loanId]) => loanId)
      )
    )
  )
// #endregion

export const Library = ViewModel.layer([OpenLoansView]).pipe(
  Layer.provideMerge(Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, OpenLoans.layer))
)

/** What the post runs: Alice borrows during the bug, Bob after the fix, then the correction. */
export const correcting = Effect.gen(function*() {
  const viewModels = yield* ViewModel.ViewModels

  // #region with-the-bug
  const withTheBug = yield* AggregateService.make(BookWithTheBug)
  yield* withTheBug.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* withTheBug.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  // #endregion

  // #region after-the-fix
  const books = yield* AggregateService.make(Book)
  yield* books.commands.add("book-2", { title: "Neuromancer", author: "William Gibson" })
  const { position } = yield* books.commands.lend("book-2", { loanId: "loan-2", memberId: "bob" })
  yield* viewModels.caughtUp(position)

  const inTwoDays = DateTime.add(yield* DateTime.now, { days: 2 })
  const beforeCorrection = yield* overdueAt(inTwoDays)
  // ["loan-1"]: Alice's book is due 21 hours after she took it
  // #endregion

  // #region correct
  // Every open loan that is due sooner than 21 days after it began came from the bug.
  const affected = [...(yield* OpenLoans.use(Ref.get))].filter(([, { dueOn, lentAt }]) =>
    DateTime.isLessThan(dueOn, DateTime.add(lentAt, { days: 21 }))
  )

  for (const [loanId, { bookId, lentAt }] of affected) {
    yield* books.commands.correctDueDate(bookId, {
      loanId,
      dueOn: DateTime.add(lentAt, { days: 21 }),
      reason: "The loan period was 21 hours instead of 21 days"
    }).pipe(Metadata.withMetadata({ ranBy: "scripts/fix-due-dates.ts" }))
  }
  yield* viewModels.caughtUp()

  const afterCorrection = yield* overdueAt(inTwoDays)
  // []
  const history = yield* books.history("book-1", { payload: true })
  // BookAdded, BookLent, DueDateCorrected: the mistake and the correction, whose
  // metadata says { ranBy: "scripts/fix-due-dates.ts" }
  // #endregion

  return { beforeCorrection, afterCorrection, history }
})
