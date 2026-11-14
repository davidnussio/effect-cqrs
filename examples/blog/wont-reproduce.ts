/**
 * The post "The bug that won't reproduce": a member who can't borrow anything, a test
 * that can't reproduce it, and her stream copied from production into a fixture that
 * does. `wont-reproduce.test.ts` runs it.
 */
import { Effect, Schema } from "effect"
import { Aggregate, AggregateService, EventStore } from "effect-cqrs"
import { loanLimit, LoanLimitReached, MemberAlreadyJoined, MemberNotFound } from "./library.ts"

export const MemberEvent = Schema.TaggedUnion({
  MemberJoined: {},
  LoanRequested: { loanId: Schema.String, bookId: Schema.String },
  LoanConfirmed: { loanId: Schema.String },
  LoanCancelled: { loanId: Schema.String, reason: Schema.String }
})

export interface Loan {
  readonly bookId: string
  readonly status: "requested" | "active" | "cancelled"
}

export interface MemberState {
  readonly joined: boolean
  readonly loans: Readonly<Record<string, Loan>>
}

// #region member
const makeMember = (countsAgainstLimit: (loan: Loan) => boolean) =>
  Aggregate.make<MemberState>()({
    name: "member",
    events: MemberEvent,
    initial: { joined: false, loans: {} },
    projection: {
      MemberJoined: (state) => ({ ...state, joined: true }),
      LoanRequested: (state, { bookId, loanId }) => ({
        ...state,
        loans: { ...state.loans, [loanId]: { bookId, status: "requested" } }
      }),
      LoanConfirmed: (state, { loanId }) => setStatus(state, loanId, "active"),
      // Cancelled loans are kept, so the member's page can list them.
      LoanCancelled: (state, { loanId }) => setStatus(state, loanId, "cancelled")
    }
  })
    .command("join", {
      payload: Schema.Struct({}),
      handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
        if (state.joined) return yield* new MemberAlreadyJoined({ memberId: aggregateId })
        return MemberEvent.cases.MemberJoined.make({})
      })
    })
    .command("requestLoan", {
      payload: Schema.Struct({ loanId: Schema.String, bookId: Schema.String }),
      handler: Effect.fnUntraced(function*(state, { bookId, loanId }, { aggregateId }) {
        if (!state.joined) return yield* new MemberNotFound({ memberId: aggregateId })
        if (Object.values(state.loans).filter(countsAgainstLimit).length >= loanLimit) {
          return yield* new LoanLimitReached({ memberId: aggregateId, limit: loanLimit })
        }
        return MemberEvent.cases.LoanRequested.make({ loanId, bookId })
      })
    })
    // What the saga runs when the book is out.
    .command("cancelLoan", {
      payload: Schema.Struct({ loanId: Schema.String, reason: Schema.String }),
      handler: (state, payload) =>
        Effect.succeed(
          state.loans[payload.loanId]?.status === "requested" ? MemberEvent.cases.LoanCancelled.make(payload) : []
        )
    })

const setStatus = (state: MemberState, loanId: string, status: Loan["status"]): MemberState => {
  const loan = state.loans[loanId]
  return loan === undefined ? state : { ...state, loans: { ...state.loans, [loanId]: { ...loan, status } } }
}

// The bug: every loan the member ever had counts against the limit, cancelled ones too.
export const Member = makeMember(() => true)
// #endregion

// #region fixed
// The fix: only loans that are open count.
export const FixedMember = makeMember((loan) => loan.status !== "cancelled")
// #endregion

/** Production: Carla asks for three books that are out, then for a fourth one. */
export const production = Effect.gen(function*() {
  const members = yield* AggregateService.make(Member)

  // #region production
  yield* members.commands.join("carla", {})
  for (const [loanId, bookId] of [["loan-1", "dune"], ["loan-2", "emma"], ["loan-3", "ulysses"]]) {
    yield* members.commands.requestLoan("carla", { loanId, bookId })
    yield* members.commands.cancelLoan("carla", { loanId, reason: "BookAlreadyLent" })
  }

  const refused = yield* Effect.flip(members.commands.requestLoan("carla", { loanId: "loan-4", bookId: "solaris" }))
  // refused: LoanLimitReached { memberId: "carla", limit: 3 }, and Carla has no books
  // #endregion

  // #region export
  // The support script: Carla's stream, as JSON, for the bug report.
  const history = yield* members.history("carla", { payload: true })
  const encode = Schema.encodeSync(Schema.toCodecJson(MemberEvent))
  const exported = JSON.stringify(history.map(({ payload }) => encode(payload!)), null, 2)
  // #endregion

  return { refused, exported }
}).pipe(Effect.provide(EventStore.layerMemory))

/** Turns an exported stream back into events, as the test does with the file. */
export const decodeStream = (json: string) =>
  Schema.decodeUnknownSync(Schema.Array(Schema.toCodecJson(MemberEvent)))(JSON.parse(json))
