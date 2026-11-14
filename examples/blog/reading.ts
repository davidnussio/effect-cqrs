/**
 * The post "Three ways to read": a lent book read from its aggregate, from the catalog
 * as it is, and from the catalog once it has seen the command, first in one process and
 * then over HTTP. `reading.test.ts` runs it.
 */
import { Effect, Layer, Ref, Schema } from "effect"
import { CommandApi, ViewModel } from "effect-cqrs"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/http-api"
import {
  Book,
  BookAlreadyAdded,
  BookAlreadyLent,
  BookNotFound,
  BookNotLent,
  Books,
  Catalog,
  ReadSide
} from "./library.ts"

/** What the post runs in one process: the three reads after a loan and a return. */
export const inProcess = Effect.gen(function*() {
  const books = yield* Books
  const viewModels = yield* ViewModel.ViewModels

  // #region aggregate
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })

  const book = yield* books.readModel("book-1")
  // { status: "lent", …, loan: { memberId: "alice", … } }, every time
  // #endregion

  // #region as-it-is
  const rightNow = (yield* Catalog.use(Ref.get)).get("book-1")
  // { available: false }, { available: true } or undefined: the catalog may not have
  // seen the loan, or even the book, yet
  // #endregion

  // #region after-the-command
  const { position } = yield* books.commands.returnBook("book-1", {})
  yield* viewModels.caughtUp(position)

  const afterReturn = (yield* Catalog.use(Ref.get)).get("book-1")
  // { title: "Dune", author: "Frank Herbert", available: true }, every time
  // #endregion

  return { book, rightNow, afterReturn }
})

// #region api
export const BookState = Schema.Struct({
  status: Schema.Literals(["unknown", "available", "lent"]),
  title: Schema.String,
  author: Schema.String,
  loan: Schema.NullOr(Schema.Struct({ loanId: Schema.String, memberId: Schema.String, dueOn: Schema.DateTimeUtc }))
})

export const CatalogEntry = Schema.Struct({
  bookId: Schema.String,
  title: Schema.String,
  author: Schema.String,
  available: Schema.Boolean
})

export class LibraryApi extends HttpApi.make("library")
  // POST /book/:id/add, /lend and /returnBook, and GET /book/:id, which folds the events
  .add(
    CommandApi.group(Book, { state: BookState, errors: [BookAlreadyAdded, BookNotFound, BookAlreadyLent, BookNotLent] })
  )
  // GET /catalog, from the view model, after the position in Read-After if there is one
  .add(
    HttpApiGroup.make("catalog")
      .add(HttpApiEndpoint.get("list", "/catalog", { success: Schema.Array(CatalogEntry) }))
      .middleware(CommandApi.ReadAfter)
  )
{}
// #endregion

// #region server
const CatalogHandlers = HttpApiBuilder.group(LibraryApi, "catalog", (handlers) =>
  Effect.gen(function*() {
    const catalog = yield* Catalog
    return handlers.handle("list", () =>
      Ref.get(catalog).pipe(Effect.map((entries) =>
        Array.from(entries, ([bookId, entry]) => ({ bookId, ...entry }))
      )))
  }))

export const Server = HttpApiBuilder.layer(LibraryApi).pipe(
  Layer.provide([CommandApi.layer(LibraryApi, "book", Effect.service(Books)), CatalogHandlers]),
  // The catalog's handlers need the middleware, and the middleware needs the view models.
  Layer.provide(CommandApi.layerReadAfter({ timeout: "2 seconds" })),
  Layer.provide(ReadSide)
)
// #endregion

/** What the post runs over HTTP, with a `fetch` that reaches `Server`. */
export const overHttp = async (fetch: (path: string, init?: RequestInit) => Promise<Response>) => {
  // #region client
  const post = (path: string, body: object) =>
    fetch(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })

  await post("/book/book-1/add", { title: "Dune", author: "Frank Herbert" })
  const lent = await post("/book/book-1/lend", { loanId: "loan-1", memberId: "alice" })
  const position = lent.headers.get("event-position")!

  // The book's own page reads the aggregate: always current.
  const book = await (await fetch("/book/book-1")).json()

  // The catalog page sends the position back, and waits until the catalog has the loan.
  const catalog = await (await fetch("/catalog", { headers: { "read-after": position } })).json()
  // [{ bookId: "book-1", title: "Dune", author: "Frank Herbert", available: false }]
  // #endregion

  return { position, book, catalog }
}
