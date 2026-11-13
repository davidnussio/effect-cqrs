/**
 * The post "The saga you skipped": borrowing a book without a saga, in the two ways
 * that look right and aren't, then with the saga of `library.ts`. A store that loses
 * its connection once shows the difference. `without-saga.test.ts` runs all of it.
 */
import { DateTime, Effect, Layer, Ref, Schema } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, PersonalData } from "effect-cqrs"
import {
  Application,
  Books,
  loanLimit,
  LoanLimitReached,
  loanPeriod,
  MemberAlreadyJoined,
  MemberNotFound,
  Members,
  settled
} from "./library.ts"

/** What both wrong members keep: whether they joined, and the loans they have open. */
interface LoansState {
  readonly joined: boolean
  readonly loans: ReadonlyArray<string>
}

// #region greedy
const GreedyMemberEvent = Schema.TaggedUnion({
  MemberJoined: { name: Schema.NonEmptyString },
  LoanStarted: { loanId: Schema.String, bookId: Schema.String },
  // The book's event, recorded in the member's stream.
  BookLent: { loanId: Schema.String, bookId: Schema.String, dueOn: Schema.DateTimeUtc }
})

export const GreedyMember = Aggregate.make<LoansState>()({
  name: "member",
  events: GreedyMemberEvent,
  initial: { joined: false, loans: [] },
  projection: {
    MemberJoined: (state) => ({ ...state, joined: true }),
    LoanStarted: (state, { loanId }) => ({ ...state, loans: [...state.loans, loanId] }),
    BookLent: (state) => state
  }
})
  .command("join", {
    payload: Schema.Struct({ name: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { name }, { aggregateId }) {
      if (state.joined) return yield* new MemberAlreadyJoined({ memberId: aggregateId })
      return GreedyMemberEvent.cases.MemberJoined.make({ name })
    })
  })
  .command("borrow", {
    payload: Schema.Struct({ loanId: Schema.String, bookId: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { bookId, loanId }, { aggregateId }) {
      if (!state.joined) return yield* new MemberNotFound({ memberId: aggregateId })
      if (state.loans.length >= loanLimit) {
        return yield* new LoanLimitReached({ memberId: aggregateId, limit: loanLimit })
      }
      const dueOn = DateTime.add(yield* DateTime.now, loanPeriod)
      // Two events in one go: the member's, and the one the book should have decided on.
      return [
        GreedyMemberEvent.cases.LoanStarted.make({ loanId, bookId }),
        GreedyMemberEvent.cases.BookLent.make({ loanId, bookId, dueOn })
      ]
    })
  })
// #endregion

/** The first mistake: Alice and Bob both borrow the only copy of Dune. */
export const greedy = Effect.gen(function*() {
  const books = yield* Books

  // #region greedy-run
  const members = yield* AggregateService.make(GreedyMember)

  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* members.commands.join("alice", { name: "Alice Rossi" })
  yield* members.commands.join("bob", { name: "Bob Bianchi" })

  yield* members.commands.borrow("alice", { loanId: "loan-1", bookId: "book-1" })
  yield* members.commands.borrow("bob", { loanId: "loan-2", bookId: "book-1" })
  // Both succeed: two loans of one copy.

  const book = yield* books.readModel("book-1")
  // { status: "available", loan: null, … }: the book never heard of either
  // #endregion

  return { book }
})

// #region calling
const CallingMemberEvent = Schema.TaggedUnion({
  MemberJoined: { name: Schema.NonEmptyString },
  LoanStarted: { loanId: Schema.String, bookId: Schema.String }
})

export const CallingMember = Aggregate.make<LoansState>()({
  name: "member",
  events: CallingMemberEvent,
  initial: { joined: false, loans: [] },
  projection: {
    MemberJoined: (state) => ({ ...state, joined: true }),
    LoanStarted: (state, { loanId }) => ({ ...state, loans: [...state.loans, loanId] })
  }
})
  .command("join", {
    payload: Schema.Struct({ name: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { name }, { aggregateId }) {
      if (state.joined) return yield* new MemberAlreadyJoined({ memberId: aggregateId })
      return CallingMemberEvent.cases.MemberJoined.make({ name })
    })
  })
  .command("borrow", {
    payload: Schema.Struct({ loanId: Schema.String, bookId: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { bookId, loanId }, { aggregateId }) {
      if (!state.joined) return yield* new MemberNotFound({ memberId: aggregateId })
      if (state.loans.length >= loanLimit) {
        return yield* new LoanLimitReached({ memberId: aggregateId, limit: loanLimit })
      }
      // Now the book decides, through its own command, from inside the member's decision.
      const books = yield* Books
      yield* books.commands.lend(bookId, { loanId, memberId: aggregateId })
      return CallingMemberEvent.cases.LoanStarted.make({ loanId, bookId })
    })
  })
// #endregion

/** The second mistake, on a good day: the book refuses Bob. */
export const callingWorks = Effect.gen(function*() {
  const books = yield* Books

  // #region calling-run
  const members = yield* AggregateService.make(CallingMember)

  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* members.commands.join("alice", { name: "Alice Rossi" })
  yield* members.commands.join("bob", { name: "Bob Bianchi" })

  yield* members.commands.borrow("alice", { loanId: "loan-1", bookId: "book-1" })
  const refused = yield* Effect.flip(members.commands.borrow("bob", { loanId: "loan-2", bookId: "book-1" }))
  // refused._tag: "BookAlreadyLent". So far, so good.
  // #endregion

  return { refused: refused._tag }
})

// #region failing-once
/**
 * The store in memory, except that the first append with an event of this type fails,
 * as it would if the connection to the database dropped at that moment.
 */
export const failingOnce = (eventType: string) =>
  Layer.effect(
    EventStore.EventStore,
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const failed = yield* Ref.make(false)
      return EventStore.EventStore.of({
        ...store,
        append: (input) =>
          Effect.gen(function*() {
            if (input.events.some(({ type }) => type === eventType) && !(yield* Ref.getAndSet(failed, true))) {
              return yield* new EventStore.EventStoreError({ operation: "append", cause: new Error("Connection lost") })
            }
            return yield* store.append(input)
          })
      })
    })
  ).pipe(Layer.provide(EventStore.layerMemory))
// #endregion

/** The second mistake, on a bad day: the book is lent, the member has no loan. */
export const callingFails = Effect.gen(function*() {
  const books = yield* Books
  const members = yield* AggregateService.make(CallingMember)

  // #region calling-failure
  yield* books.commands.add("book-2", { title: "Neuromancer", author: "William Gibson" })
  yield* members.commands.join("carol", { name: "Carol Verdi" })

  // The member's append fails, after the book's went through.
  const failure = yield* Effect.flip(members.commands.borrow("carol", { loanId: "loan-3", bookId: "book-2" }))
  // failure._tag: "EventStoreError"

  const book = yield* books.readModel("book-2")
  // { status: "lent", loan: { loanId: "loan-3", memberId: "carol", … }, … }
  const carol = yield* members.readModel("carol")
  // { joined: true, loans: [] }
  // #endregion

  return { failure: failure._tag, book, carol }
})

/** The library of part 4, its saga included, on a store that fails to confirm a loan once. */
export const LibraryFailingOnce = Application.pipe(
  Layer.provideMerge(Layer.mergeAll(
    failingOnce("LoanConfirmed"),
    Checkpoints.layerMemory,
    PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
  ))
)

/** The same loan with the saga: the failed step is tried again, and the loan settles. */
export const withSaga = Effect.gen(function*() {
  const books = yield* Books
  const members = yield* Members

  // #region saga-run
  yield* books.commands.add("book-2", { title: "Neuromancer", author: "William Gibson" })
  yield* members.commands.join("carol", { name: "Carol Verdi" })

  // One write, to one aggregate: the member records what she asked for.
  yield* members.commands.requestLoan("carol", { loanId: "loan-3", bookId: "book-2" })

  // The saga lends the book, fails to confirm the loan, tries again, and confirms it.
  const outcome = yield* settled("carol", "loan-3")
  // Option.some("LoanConfirmed")

  const book = yield* books.readModel("book-2")
  // { status: "lent", loan: { loanId: "loan-3", memberId: "carol", … }, … }
  const carol = yield* members.readModel("carol")
  // { …, loans: { "loan-3": { bookId: "book-2", status: "active" } } }
  // #endregion

  return { outcome, book, carol }
})
