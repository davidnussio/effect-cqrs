/**
 * The post "Change the rules, keep the history": the library's summer rules let members
 * keep five books for four weeks, and from September it is three books for three weeks.
 * Decisions made under the old rules stay as they were. `change-the-rules.test.ts` runs
 * it, moving the clock from August to September.
 */
import { DateTime, Effect, Schema } from "effect"
import { Aggregate, AggregateService, EventStore } from "effect-cqrs"
import { LoanLimitReached } from "./library.ts"

// #region policies
export interface Policy {
  readonly name: string
  /** From when it applies, inclusive. */
  readonly from: DateTime.Utc
  readonly limit: number
  readonly days: number
}

/** The library's lending rules over time, newest last. */
export const policies: ReadonlyArray<Policy> = [
  { name: "standard-2025", from: DateTime.makeUnsafe("2025-01-01T00:00:00Z"), limit: 3, days: 21 },
  { name: "summer-2026", from: DateTime.makeUnsafe("2026-06-01T00:00:00Z"), limit: 5, days: 28 },
  { name: "standard-2026", from: DateTime.makeUnsafe("2026-09-01T00:00:00Z"), limit: 3, days: 21 }
]

/** The policy in force at a given time. */
const policyAt = (at: DateTime.Utc): Policy =>
  policies.findLast(({ from }) => DateTime.isLessThanOrEqualTo(from, at)) ?? policies[0]!
// #endregion

// #region member
export const MemberEvent = Schema.TaggedUnion({
  // The decision, with the rule that made it: the due date the member was given, and why.
  BookBorrowed: { bookId: Schema.String, dueOn: Schema.DateTimeUtc, policy: Schema.String },
  BookReturned: { bookId: Schema.String }
})

export interface MemberState {
  /** The books the member has, with the due date each was given. */
  readonly books: Readonly<Record<string, DateTime.Utc>>
}

export const Member = Aggregate.make<MemberState>()({
  name: "member",
  events: MemberEvent,
  initial: { books: {} },
  // The projection records what was decided. It checks no rule, so a member who kept five
  // books under the summer rules still loads in September.
  projection: {
    BookBorrowed: ({ books }, { bookId, dueOn }) => ({ books: { ...books, [bookId]: dueOn } }),
    BookReturned: ({ books }, { bookId }) => {
      const { [bookId]: _, ...rest } = books
      return { books: rest }
    }
  }
})
  .command("borrow", {
    payload: Schema.Struct({ bookId: Schema.String }),
    // The rules live here, and run on the day of the decision.
    handler: Effect.fnUntraced(function*({ books }, { bookId }, { aggregateId }) {
      const now = yield* DateTime.now
      const policy = policyAt(now)
      if (Object.keys(books).length >= policy.limit) {
        return yield* new LoanLimitReached({ memberId: aggregateId, limit: policy.limit })
      }
      const dueOn = DateTime.add(now, { days: policy.days })
      return MemberEvent.cases.BookBorrowed.make({ bookId, dueOn, policy: policy.name })
    })
  })
  .command("giveBack", {
    payload: Schema.Struct({ bookId: Schema.String }),
    handler: ({ books }, { bookId }) =>
      Effect.succeed(books[bookId] === undefined ? [] : MemberEvent.cases.BookReturned.make({ bookId }))
  })
// #endregion

export const Library = EventStore.layerMemory

/** Late August: the summer rules. Alice borrows four books. */
export const summer = Effect.gen(function*() {
  const members = yield* AggregateService.make(Member)
  // #region summer
  for (const bookId of ["dune", "emma", "ulysses", "solaris"]) {
    yield* members.commands.borrow("alice", { bookId })
  }
  // Four BookBorrowed, each due in 28 days, under "summer-2026"
  // #endregion
})

/** Early September: the standard rules again. */
export const autumn = Effect.gen(function*() {
  const members = yield* AggregateService.make(Member)

  // #region autumn
  const alice = yield* members.state("alice")
  // Four books, each with the due date she was given in August, four weeks after she took it

  const refused = yield* Effect.flip(members.commands.borrow("alice", { bookId: "emma-2" }))
  // refused: LoanLimitReached { limit: 3 }

  yield* members.commands.giveBack("alice", { bookId: "dune" })
  yield* members.commands.giveBack("alice", { bookId: "emma" })
  const borrowed = yield* members.commands.borrow("alice", { bookId: "neuromancer" })
  // borrowed.events: [BookBorrowed { bookId: "neuromancer", dueOn: in 21 days, policy: "standard-2026" }]
  // #endregion

  return { alice, refused, borrowed }
})
