/**
 * The post "Fix the code, not the data": a history of every loan, kept by a view model
 * with a bug that loses loans, then the fix, deployed and replayed over the same log.
 * `fix-the-code.test.ts` runs it.
 */
import { Context, Effect, Layer, Ref } from "effect"
import { Checkpoints, EventStore, ViewModel } from "effect-cqrs"
import { Book, Books } from "./library.ts"

// #region history
export interface LoanRecord {
  readonly loanId: string
  readonly bookId: string
  readonly memberId: string
  readonly returned: boolean
}

/** Every loan the library has made: the `loans` table, as a read model. */
export class LoanHistory extends Context.Service<LoanHistory, Ref.Ref<ReadonlyMap<string, LoanRecord>>>()(
  "library/LoanHistory"
) {
  static readonly layer = Layer.effect(LoanHistory, Ref.make<ReadonlyMap<string, LoanRecord>>(new Map()))
}

const save = (key: string, record: LoanRecord) =>
  LoanHistory.use((history) => Ref.update(history, (loans) => new Map(loans).set(key, record)))

const markReturned = (key: string) =>
  LoanHistory.use((history) =>
    Ref.update(history, (loans) => {
      const record = loans.get(key)
      return record === undefined ? loans : new Map(loans).set(key, { ...record, returned: true })
    })
  )

/** The loans of one member, as the front desk asks for them. */
export const loansOf = (memberId: string) =>
  LoanHistory.use((history) =>
    Ref.get(history).pipe(Effect.map((loans) => [...loans.values()].filter((loan) => loan.memberId === memberId)))
  )
// #endregion

// #region buggy
export const BuggyLoanHistoryView = ViewModel.make({
  name: "loan-history",
  aggregates: [Book],
  dispose: LoanHistory.use((history) => Ref.set(history, new Map())),
  handlers: {
    // The bug: the record is kept under the book's id, so the next loan of the same
    // book replaces it.
    BookLent: ({ loanId, memberId }, { aggregateId }) =>
      save(aggregateId, { loanId, bookId: aggregateId, memberId, returned: false }),
    BookReturned: (_, { aggregateId }) => markReturned(aggregateId)
  }
})
// #endregion

// #region fixed
export const LoanHistoryView = ViewModel.make({
  name: "loan-history",
  aggregates: [Book],
  dispose: LoanHistory.use((history) => Ref.set(history, new Map())),
  handlers: {
    // The fix: one record per loan.
    BookLent: ({ loanId, memberId }, { aggregateId }) =>
      save(loanId, { loanId, bookId: aggregateId, memberId, returned: false }),
    BookReturned: ({ loanId }) => markReturned(loanId)
  }
})
// #endregion

// #region deploy
// What outlives a deploy: the log, the checkpoints and the loans table itself.
export const Storage = Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, LoanHistory.layer)

// What a deploy starts: the books and one version of the view model.
const deploy = (view: typeof LoanHistoryView) => ViewModel.layer([view]).pipe(Layer.provideMerge(Books.layer))
// #endregion

/** The first deploy, with the bug: Alice borrows Dune and brings it back, then Bob borrows it. */
export const withTheBug = Effect.gen(function*() {
  const books = yield* Books
  const viewModels = yield* ViewModel.ViewModels

  // #region with-the-bug
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  yield* books.commands.returnBook("book-1", {})
  const { position } = yield* books.commands.lend("book-1", { loanId: "loan-2", memberId: "bob" })
  yield* viewModels.caughtUp(position)

  const alice = yield* loansOf("alice")
  // []: Bob's loan took the place of hers
  // #endregion

  return alice
}).pipe(Effect.provide(deploy(BuggyLoanHistoryView)))

/** The second deploy, with the fix: the history is still wrong until it is replayed. */
export const withTheFix = Effect.gen(function*() {
  const viewModels = yield* ViewModel.ViewModels

  // #region with-the-fix
  yield* viewModels.caughtUp()
  const beforeReplay = yield* loansOf("alice")
  // []: the view model starts from its checkpoint, after every event the bug handled

  const { eventCount } = yield* viewModels.replay({ viewModels: ["loan-history"] })
  const afterReplay = yield* loansOf("alice")
  // [{ loanId: "loan-1", bookId: "book-1", memberId: "alice", returned: true }], from the 3 loan events
  // #endregion

  return { beforeReplay, eventCount, afterReplay }
}).pipe(Effect.provide(deploy(LoanHistoryView)))
