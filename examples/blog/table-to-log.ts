/**
 * The post "From a table to a log": a library that kept its books in a table moves to
 * events. Each row becomes the first event of its book's stream, an import that says
 * where it came from, and the books carry on from there. `table-to-log.test.ts` runs it.
 */
import { DateTime, Effect, Layer, Ref, Schema } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, Metadata, ViewModel } from "effect-cqrs"
import { BookAlreadyLent, BookNotFound, BookNotLent, Catalog } from "./library.ts"

// #region rows
/** A row of the old `books` table, as `SELECT * FROM books` returns it. */
export interface BookRow {
  readonly id: string
  readonly title: string
  readonly author: string
  readonly lent_to: string | null
  readonly loan_id: string | null
  readonly due_on: string | null
}

export const rows: ReadonlyArray<BookRow> = [
  {
    id: "book-1",
    title: "Dune",
    author: "Frank Herbert",
    lent_to: "alice",
    loan_id: "L-1042",
    due_on: "2026-12-01T00:00:00Z"
  },
  { id: "book-2", title: "Emma", author: "Jane Austen", lent_to: null, loan_id: null, due_on: null }
]
// #endregion

// #region events
const Loan = Schema.Struct({ loanId: Schema.String, memberId: Schema.String, dueOn: Schema.DateTimeUtc })

export const BookEvent = Schema.TaggedUnion({
  // What the old system knew about the book on the day of the import, and nothing more.
  BookImported: { title: Schema.String, author: Schema.String, loan: Schema.NullOr(Loan) },
  BookAdded: { title: Schema.NonEmptyString, author: Schema.NonEmptyString },
  BookLent: { loanId: Schema.String, memberId: Schema.String, dueOn: Schema.DateTimeUtc },
  BookReturned: { loanId: Schema.String, memberId: Schema.String, late: Schema.Boolean }
})
// #endregion

interface BookState {
  readonly status: "unknown" | "available" | "lent"
  readonly loan: typeof Loan.Type | null
}

// #region book
export const Book = Aggregate.make<BookState>()({
  name: "book",
  events: BookEvent,
  initial: { status: "unknown", loan: null },
  projection: {
    BookImported: (_, { loan }) => ({ status: loan === null ? "available" : "lent", loan }),
    BookAdded: () => ({ status: "available", loan: null }),
    BookLent: (_, loan) => ({ status: "lent", loan }),
    BookReturned: () => ({ status: "available", loan: null })
  }
})
  .command("importBook", {
    payload: Schema.Struct({ title: Schema.String, author: Schema.String, loan: Schema.NullOr(Loan) }),
    // A book that already has a stream was imported already: running the import again is harmless.
    handler: (state, payload) =>
      Effect.succeed(state.status === "unknown" ? BookEvent.cases.BookImported.make(payload) : [])
  })
  // #endregion
  .command("lend", {
    payload: Schema.Struct({ loanId: Schema.String, memberId: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { loanId, memberId }, { aggregateId }) {
      if (state.status === "unknown") return yield* new BookNotFound({ bookId: aggregateId })
      if (state.loan !== null) return yield* new BookAlreadyLent({ bookId: aggregateId })
      const dueOn = DateTime.add(yield* DateTime.now, { days: 21 })
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

// #region catalog
// The catalog of the series, which now also reads imported books.
export const CatalogView = ViewModel.make({
  name: "catalog",
  aggregates: [Book],
  dispose: Catalog.use((catalog) => Ref.set(catalog, new Map())),
  handlers: {
    BookImported: ({ author, loan, title }, { aggregateId }) =>
      Catalog.use((catalog) =>
        Ref.update(catalog, (entries) => new Map(entries).set(aggregateId, { title, author, available: loan === null }))
      ),
    BookAdded: ({ author, title }, { aggregateId }) =>
      Catalog.use((catalog) =>
        Ref.update(catalog, (entries) => new Map(entries).set(aggregateId, { title, author, available: true }))
      ),
    BookLent: (_, { aggregateId }) => setAvailable(aggregateId, false),
    BookReturned: (_, { aggregateId }) => setAvailable(aggregateId, true)
  }
})
// #endregion

const setAvailable = (bookId: string, available: boolean) =>
  Catalog.use((catalog) =>
    Ref.update(catalog, (entries) => {
      const entry = entries.get(bookId)
      return entry === undefined ? entries : new Map(entries).set(bookId, { ...entry, available })
    })
  )

export const Library = ViewModel.layer([CatalogView]).pipe(
  Layer.provideMerge(Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, Catalog.layer))
)

// #region import
/** Writes one `BookImported` per row. Safe to run again, for instance after a failure halfway. */
export const importBooks = Effect.fnUntraced(function*(rows: ReadonlyArray<BookRow>) {
  const books = yield* AggregateService.make(Book)
  for (const row of rows) {
    const loan = row.lent_to === null || row.loan_id === null || row.due_on === null
      ? null
      : { loanId: row.loan_id, memberId: row.lent_to, dueOn: DateTime.makeUnsafe(row.due_on) }
    yield* books.commands.importBook(row.id, { title: row.title, author: row.author, loan })
  }
}, Metadata.withMetadata({ importedFrom: "books table", script: "scripts/import-books.ts" }))
// #endregion

/** The import, run twice, then a day at the library on the imported books. */
export const migrating = Effect.gen(function*() {
  const books = yield* AggregateService.make(Book)
  const viewModels = yield* ViewModel.ViewModels

  // #region run
  yield* importBooks(rows)
  yield* importBooks(rows) // the second run records nothing

  // From here on, the books only change through commands.
  yield* books.commands.returnBook("book-1", {})
  yield* books.commands.lend("book-2", { loanId: "loan-1", memberId: "bob" })
  yield* viewModels.caughtUp()

  const history = yield* books.history("book-1")
  // BookImported, with metadata { importedFrom: "books table", … }, then BookReturned
  const catalog = yield* Catalog.use(Ref.get)
  // Map { "book-1" => { title: "Dune", …, available: true }, "book-2" => { title: "Emma", …, available: false } }
  // #endregion

  return { history, catalog }
})
