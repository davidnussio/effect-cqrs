/**
 * The lending library of the blog series "CQRS with effect-cqrs": books that members
 * borrow and bring back, a catalog and other read models, and a saga that arranges each
 * loan. Each post quotes the regions it explains, and `library.test.ts` runs them.
 */
import { Context, DateTime, Effect, Layer, Option, Predicate, Ref, Schema, Stream } from "effect"
import {
  Aggregate,
  AggregateService,
  Checkpoints,
  EventStore,
  EventVersions,
  PersonalData,
  Saga,
  ViewModel
} from "effect-cqrs"
import { WorkflowEngine } from "effect/workflow"

// #region book-events
export const BookEvent = Schema.TaggedUnion({
  BookAdded: { title: Schema.NonEmptyString, author: Schema.NonEmptyString },
  BookLent: { loanId: Schema.String, memberId: Schema.String, dueOn: Schema.DateTimeUtc },
  BookReturned: { loanId: Schema.String, memberId: Schema.String, late: Schema.Boolean }
})
// #endregion

// #region book-errors
export class BookAlreadyAdded extends Schema.TaggedError<BookAlreadyAdded>()("BookAlreadyAdded", {
  bookId: Schema.String
}) {}

export class BookNotFound extends Schema.TaggedError<BookNotFound>()("BookNotFound", {
  bookId: Schema.String
}) {}

export class BookAlreadyLent extends Schema.TaggedError<BookAlreadyLent>()("BookAlreadyLent", {
  bookId: Schema.String
}) {}

export class BookNotLent extends Schema.TaggedError<BookNotLent>()("BookNotLent", {
  bookId: Schema.String
}) {}
// #endregion

// #region book-versions
// The first version of the library didn't record authors.
const BookAddedV1 = Schema.Struct({ title: Schema.NonEmptyString })

const bookVersions = {
  BookAdded: EventVersions.from(BookAddedV1).next(
    BookEvent.cases.BookAdded,
    (v1) => ({ ...v1, author: "Unknown" })
  )
}
// #endregion

// #region book
/** How long a member may keep a book. */
export const loanPeriod = { days: 21 }

export interface BookState {
  readonly status: "unknown" | "available" | "lent"
  readonly title: string
  readonly author: string
  readonly loan: { readonly loanId: string; readonly memberId: string; readonly dueOn: DateTime.Utc } | null
}

export const Book = Aggregate.make<BookState>()({
  name: "book",
  events: BookEvent,
  initial: { status: "unknown", title: "", author: "", loan: null },
  projection: {
    BookAdded: (state, { author, title }) => ({ ...state, status: "available", title, author }),
    BookLent: (state, { dueOn, loanId, memberId }) => ({ ...state, status: "lent", loan: { loanId, memberId, dueOn } }),
    BookReturned: (state) => ({ ...state, status: "available", loan: null })
  },
  versions: bookVersions
})
  .command("add", {
    payload: Schema.Struct({ title: Schema.NonEmptyString, author: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { author, title }, { aggregateId }) {
      if (state.status !== "unknown") return yield* new BookAlreadyAdded({ bookId: aggregateId })
      return BookEvent.cases.BookAdded.make({ title, author })
    })
  })
  .command("lend", {
    payload: Schema.Struct({ loanId: Schema.String, memberId: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { loanId, memberId }, { aggregateId }) {
      if (state.status === "unknown") return yield* new BookNotFound({ bookId: aggregateId })
      // Lending it again for the same loan records nothing, so a retry is harmless.
      if (state.loan?.loanId === loanId) return []
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
// #endregion

// #region books-service
export class Books extends Context.Service<Books, AggregateService.AggregateService<typeof Book>>()("library/Books") {
  static readonly layer = Layer.effect(Books, AggregateService.make(Book))
}

export const BooksInMemory = Books.layer.pipe(Layer.provide(EventStore.layerMemory))
// #endregion

/** What the second post runs: commands, a refusal, untyped JSON, state and history. */
export const firstSteps = Effect.gen(function*() {
  const books = yield* Books

  // #region first-commands
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  const lent = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  // lent.events: [{ _tag: "BookLent", loanId: "loan-1", memberId: "alice", dueOn: … }]
  // lent.version: 2
  // #endregion

  // #region refused
  const answer = yield* books.commands.lend("book-1", { loanId: "loan-2", memberId: "bob" }).pipe(
    Effect.as("Enjoy your book"),
    Effect.catchTag("BookAlreadyLent", ({ bookId }) => Effect.succeed(`${bookId} is out, try again later`))
  )
  // #endregion

  // #region dispatch
  // What an HTTP handler hands over: JSON it knows nothing about.
  yield* books.dispatch({
    aggregateId: "book-2",
    type: "add",
    payload: { title: "Neuromancer", author: "William Gibson" }
  })

  // An empty title never reaches the handler.
  const invalid = yield* Effect.flip(books.dispatch({ aggregateId: "book-3", type: "add", payload: { title: "" } }))
  // invalid._tag: "InvalidCommand"
  // #endregion

  // #region read
  const state = yield* books.state("book-1")
  // { status: "lent", title: "Dune", author: "Frank Herbert", loan: { loanId: "loan-1", … } }

  const history = yield* books.history("book-1")
  // [{ version: 1, type: "BookAdded", timestamp: … }, { version: 2, type: "BookLent", … }]
  // #endregion

  return { lent, answer, invalid: invalid._tag, state, history }
})

// #region catalog
export interface CatalogEntry {
  readonly title: string
  readonly author: string
  readonly available: boolean
}

/** The catalog: one entry per book, kept in memory. */
export class Catalog extends Context.Service<Catalog, Ref.Ref<ReadonlyMap<string, CatalogEntry>>>()(
  "library/Catalog"
) {
  static readonly layer = Layer.effect(Catalog, Ref.make<ReadonlyMap<string, CatalogEntry>>(new Map()))
}

const updateEntry = (bookId: string, update: (entry: CatalogEntry) => CatalogEntry) =>
  Catalog.use((catalog) =>
    Ref.update(catalog, (entries) => {
      const entry = entries.get(bookId)
      return entry === undefined ? entries : new Map(entries).set(bookId, update(entry))
    })
  )

export const CatalogView = ViewModel.make({
  name: "catalog",
  aggregates: [Book],
  dispose: Catalog.use((catalog) => Ref.set(catalog, new Map())),
  handlers: {
    BookAdded: ({ author, title }, { aggregateId }) =>
      Catalog.use((catalog) =>
        Ref.update(catalog, (entries) => new Map(entries).set(aggregateId, { title, author, available: true }))
      ),
    BookLent: (_, { aggregateId }) => updateEntry(aggregateId, (entry) => ({ ...entry, available: false })),
    BookReturned: (_, { aggregateId }) => updateEntry(aggregateId, (entry) => ({ ...entry, available: true }))
  }
})
// #endregion

// #region loans
/** The books each member has, by member id. */
export class Loans extends Context.Service<Loans, Ref.Ref<ReadonlyMap<string, ReadonlySet<string>>>>()(
  "library/Loans"
) {
  static readonly layer = Layer.effect(Loans, Ref.make<ReadonlyMap<string, ReadonlySet<string>>>(new Map()))
}

const setOnLoan = (memberId: string, bookId: string, onLoan: boolean) =>
  Loans.use((loans) =>
    Ref.update(loans, (members) => {
      const books = new Set(members.get(memberId))
      if (onLoan) books.add(bookId)
      else books.delete(bookId)
      return new Map(members).set(memberId, books)
    })
  )

export const LoansView = ViewModel.make({
  name: "loans",
  aggregates: [Book],
  dispose: Loans.use((loans) => Ref.set(loans, new Map())),
  handlers: {
    BookLent: ({ memberId }, { aggregateId }) => setOnLoan(memberId, aggregateId, true),
    BookReturned: ({ memberId }, { aggregateId }) => setOnLoan(memberId, aggregateId, false)
  }
})
// #endregion

// #region read-side
export const ReadSide = ViewModel.layer([CatalogView, LoansView]).pipe(
  Layer.provideMerge(Books.layer),
  Layer.provideMerge(Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, Catalog.layer, Loans.layer))
)
// #endregion

/** What the third post runs: commands, then the read models, then a replay. */
export const readSide = Effect.gen(function*() {
  const books = yield* Books
  const viewModels = yield* ViewModel.ViewModels

  // #region caught-up
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* books.commands.add("book-2", { title: "Neuromancer", author: "William Gibson" })
  const { position } = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })

  // The view models run in the background: wait until they have seen the loan.
  yield* viewModels.caughtUp(position)

  const catalog = yield* Catalog.use(Ref.get)
  // Map { "book-1" => { title: "Dune", …, available: false }, "book-2" => { …, available: true } }
  const alice = (yield* Loans.use(Ref.get)).get("alice")
  // Set { "book-1" }
  // #endregion

  // #region replay
  // Throw the catalog away and build it again from the log.
  const { eventCount } = yield* viewModels.replay({ viewModels: ["catalog"] })
  const rebuilt = yield* Catalog.use(Ref.get)
  // The same entries as before, from 3 events
  // #endregion

  return { catalog, alice, eventCount, rebuilt }
})

// #region member-events
export const MemberEvent = Schema.TaggedUnion({
  MemberJoined: { name: PersonalData.field(Schema.NonEmptyString, { redacted: "[forgotten]" }) },
  LoanRequested: { loanId: Schema.String, bookId: Schema.String },
  LoanConfirmed: { loanId: Schema.String },
  LoanCancelled: { loanId: Schema.String, reason: Schema.String },
  LoanClosed: { loanId: Schema.String }
})
// #endregion

// #region member-errors
export class MemberAlreadyJoined extends Schema.TaggedError<MemberAlreadyJoined>()("MemberAlreadyJoined", {
  memberId: Schema.String
}) {}

export class MemberNotFound extends Schema.TaggedError<MemberNotFound>()("MemberNotFound", {
  memberId: Schema.String
}) {}

export class LoanLimitReached extends Schema.TaggedError<LoanLimitReached>()("LoanLimitReached", {
  memberId: Schema.String,
  limit: Schema.Int
}) {}
// #endregion

// #region member
/** How many books a member may have at once, counting the loans still being arranged. */
export const loanLimit = 3

export interface MemberState {
  readonly joined: boolean
  readonly name: string
  /** Open loans by id: requested until the book is lent, then active. */
  readonly loans: Readonly<Record<string, { readonly bookId: string; readonly status: "requested" | "active" }>>
}

const withoutLoan = (state: MemberState, loanId: string): MemberState => {
  const { [loanId]: _, ...loans } = state.loans
  return { ...state, loans }
}

export const Member = Aggregate.make<MemberState>()({
  name: "member",
  events: MemberEvent,
  initial: { joined: false, name: "", loans: {} },
  projection: {
    MemberJoined: (state, { name }) => ({ ...state, joined: true, name }),
    LoanRequested: (state, { bookId, loanId }) => ({
      ...state,
      loans: { ...state.loans, [loanId]: { bookId, status: "requested" } }
    }),
    LoanConfirmed: (state, { loanId }) => {
      const loan = state.loans[loanId]
      return loan === undefined
        ? state
        : { ...state, loans: { ...state.loans, [loanId]: { ...loan, status: "active" } } }
    },
    LoanCancelled: (state, { loanId }) => withoutLoan(state, loanId),
    LoanClosed: (state, { loanId }) => withoutLoan(state, loanId)
  }
})
  .command("join", {
    payload: Schema.Struct({ name: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { name }, { aggregateId }) {
      if (state.joined) return yield* new MemberAlreadyJoined({ memberId: aggregateId })
      return MemberEvent.cases.MemberJoined.make({ name })
    })
  })
  .command("requestLoan", {
    payload: Schema.Struct({ loanId: Schema.String, bookId: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { bookId, loanId }, { aggregateId }) {
      if (!state.joined) return yield* new MemberNotFound({ memberId: aggregateId })
      if (state.loans[loanId] !== undefined) return []
      if (Object.keys(state.loans).length >= loanLimit) {
        return yield* new LoanLimitReached({ memberId: aggregateId, limit: loanLimit })
      }
      return MemberEvent.cases.LoanRequested.make({ loanId, bookId })
    })
  })
  // The saga runs the next three. Each records nothing when there is nothing to do, so
  // running one twice is harmless.
  .command("confirmLoan", {
    payload: Schema.Struct({ loanId: Schema.String }),
    handler: (state, { loanId }) =>
      Effect.succeed(
        state.loans[loanId]?.status === "requested" ? MemberEvent.cases.LoanConfirmed.make({ loanId }) : []
      )
  })
  .command("cancelLoan", {
    payload: Schema.Struct({ loanId: Schema.String, reason: Schema.String }),
    handler: (state, { loanId, reason }) =>
      Effect.succeed(
        state.loans[loanId]?.status === "requested" ? MemberEvent.cases.LoanCancelled.make({ loanId, reason }) : []
      )
  })
  .command("closeLoan", {
    payload: Schema.Struct({ loanId: Schema.String }),
    handler: (state, { loanId }) =>
      Effect.succeed(state.loans[loanId]?.status === "active" ? MemberEvent.cases.LoanClosed.make({ loanId }) : [])
  })
// #endregion

// #region members-service
export class Members extends Context.Service<Members, AggregateService.AggregateService<typeof Member>>()(
  "library/Members"
) {
  static readonly layer = Layer.effect(Members, AggregateService.make(Member))
}
// #endregion

// #region borrow
// One run per loan: lend the book and confirm the loan, or cancel it if the book is out.
export const Borrow = Saga.workflow("Borrow", {
  payload: { loanId: Schema.String, memberId: Schema.String, bookId: Schema.String },
  idempotencyKey: ({ loanId }) => loanId
})

export const BorrowWorkflow = Saga.toLayer(Borrow, ({ bookId, loanId, memberId }) =>
  Effect.gen(function*() {
    const books = yield* Books
    const members = yield* Members

    const lent = yield* Saga.command("lend", {
      error: Schema.Union([BookAlreadyLent, BookNotFound]),
      execute: books.commands.lend(bookId, { loanId, memberId })
    }).pipe(
      Effect.as(true),
      Effect.catchTag(["BookAlreadyLent", "BookNotFound"], (error) =>
        Saga.command("cancel", {
          execute: members.commands.cancelLoan(memberId, { loanId, reason: error._tag })
        }).pipe(Effect.as(false)))
    )

    if (lent) {
      yield* Saga.command("confirm", { execute: members.commands.confirmLoan(memberId, { loanId }) })
    }
  }))
// #endregion

// #region close-loan
// When a book comes back, the loan is closed and the member has a place free again.
export const CloseLoan = Saga.workflow("CloseLoan", {
  payload: { loanId: Schema.String, memberId: Schema.String },
  idempotencyKey: ({ loanId }) => loanId
})

export const CloseLoanWorkflow = Saga.toLayer(CloseLoan, ({ loanId, memberId }) =>
  Effect.gen(function*() {
    const members = yield* Members
    yield* Saga.command("close", { execute: members.commands.closeLoan(memberId, { loanId }) })
  }))
// #endregion

// #region trigger
export const Lending = Saga.make({
  name: "lending",
  aggregates: [Member, Book],
  handlers: {
    LoanRequested: ({ bookId, loanId }, { aggregateId }) =>
      Saga.start(Borrow, { loanId, memberId: aggregateId, bookId }),
    BookReturned: ({ loanId, memberId }) => Saga.start(CloseLoan, { loanId, memberId })
  }
})
// #endregion

// #region directory
/** Members' names, for the front desk. */
export class Directory extends Context.Service<Directory, Ref.Ref<ReadonlyMap<string, string>>>()(
  "library/Directory"
) {
  static readonly layer = Layer.effect(Directory, Ref.make<ReadonlyMap<string, string>>(new Map()))
}

export const DirectoryView = ViewModel.make({
  name: "directory",
  aggregates: [Member],
  dispose: Directory.use((directory) => Ref.set(directory, new Map())),
  handlers: {
    MemberJoined: ({ name }, { aggregateId }) =>
      Directory.use((directory) => Ref.update(directory, (names) => new Map(names).set(aggregateId, name)))
  },
  // The names are personal data: erase a member's when they are forgotten.
  onForget: (subject) =>
    Directory.use((directory) =>
      Ref.update(directory, (names) => {
        const rest = new Map(names)
        rest.delete(subject)
        return rest
      })
    )
})
// #endregion

// #region library
const Sagas = Saga.layer([Lending]).pipe(
  Layer.provideMerge(Layer.merge(BorrowWorkflow, CloseLoanWorkflow)),
  Layer.provide(WorkflowEngine.layerMemory)
)

// Everything but the storage, which the last post swaps for a database.
export const Application = Layer.mergeAll(Sagas, ViewModel.layer([CatalogView, LoansView, DirectoryView])).pipe(
  Layer.provideMerge(Layer.merge(Books.layer, Members.layer)),
  Layer.provideMerge(Layer.mergeAll(Catalog.layer, Loans.layer, Directory.layer))
)

// Events, checkpoints and the keys of personal data, all in memory.
export const InMemory = Layer.mergeAll(
  EventStore.layerMemory,
  Checkpoints.layerMemory,
  PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
)

export const Library = Application.pipe(Layer.provideMerge(InMemory))
// #endregion

// #region settled
/** Waits until one of these events is stored for a member's loan, and the view models have seen it. */
export const loanReached = (memberId: string, loanId: string, ...eventTypes: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const viewModels = yield* ViewModel.ViewModels
    const reached = yield* store.subscribe({ aggregateNames: ["member"], aggregateIds: [memberId], eventTypes }).pipe(
      Stream.filter(({ payload }) => Predicate.hasProperty(payload, "loanId") && payload.loanId === loanId),
      Stream.runHead
    )
    if (Option.isSome(reached)) yield* viewModels.caughtUp(reached.value.position)
    return Option.map(reached, ({ type }) => type)
  })

/** Waits until the saga has confirmed or cancelled a loan. */
export const settled = (memberId: string, loanId: string) =>
  loanReached(memberId, loanId, "LoanConfirmed", "LoanCancelled")
// #endregion

/** What the fourth post runs: two members after the same book, then a return. */
export const borrowing = Effect.gen(function*() {
  const books = yield* Books
  const members = yield* Members

  // #region borrow-run
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* members.commands.join("alice", { name: "Alice Rossi" })
  yield* members.commands.join("bob", { name: "Bob Bianchi" })

  yield* members.commands.requestLoan("alice", { loanId: "loan-1", bookId: "book-1" })
  const forAlice = yield* settled("alice", "loan-1")
  // Option.some("LoanConfirmed")

  yield* members.commands.requestLoan("bob", { loanId: "loan-2", bookId: "book-1" })
  const forBob = yield* settled("bob", "loan-2")
  // Option.some("LoanCancelled"): Alice has the book
  // #endregion

  // #region return-run
  yield* books.commands.returnBook("book-1", {})
  yield* loanReached("alice", "loan-1", "LoanClosed")
  const alice = yield* members.state("alice")
  // { joined: true, name: "Alice Rossi", loans: {} }
  // #endregion

  // #region correlation
  const bobsHistory = yield* members.history("bob")
  // LoanRequested and LoanCancelled share a correlationId, and LoanCancelled's
  // causationId names the event that started the saga: "member/bob@2"
  // #endregion

  const alicesHistory = yield* members.history("alice")
  const bookHistory = yield* books.history("book-1")

  return { forAlice, forBob, alice, bobsHistory, alicesHistory, bookHistory }
})
