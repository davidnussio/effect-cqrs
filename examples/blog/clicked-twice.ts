/**
 * The post "I clicked twice": a member pays a fine, and the request arrives twice. First
 * without a command id, then with one, and a loan whose own id makes it safe to repeat.
 * `clicked-twice.test.ts` runs it.
 */
import { Context, Effect, Layer, Schema } from "effect"
import { Aggregate, AggregateService, EventStore } from "effect-cqrs"
import { Books } from "./library.ts"

// #region account
export const AccountEvent = Schema.TaggedUnion({
  FineCharged: { amount: Schema.Int, reason: Schema.String },
  PaymentReceived: { amount: Schema.Int }
})

export class NothingToPay extends Schema.TaggedError<NothingToPay>()("NothingToPay", {
  memberId: Schema.String
}) {}

/** What a member owes the library, in cents. Negative is credit. */
export const Account = Aggregate.make<{ readonly balance: number }>()({
  name: "account",
  events: AccountEvent,
  initial: { balance: 0 },
  projection: {
    FineCharged: ({ balance }, { amount }) => ({ balance: balance + amount }),
    PaymentReceived: ({ balance }, { amount }) => ({ balance: balance - amount })
  }
})
  .command("chargeFine", {
    payload: Schema.Struct({ amount: Schema.Int, reason: Schema.String }),
    handler: (_, payload) => Effect.succeed(AccountEvent.cases.FineCharged.make(payload))
  })
  .command("pay", {
    payload: Schema.Struct({ amount: Schema.Int }),
    handler: Effect.fnUntraced(function*({ balance }, { amount }, { aggregateId }) {
      if (balance <= 0) return yield* new NothingToPay({ memberId: aggregateId })
      return AccountEvent.cases.PaymentReceived.make({ amount })
    })
  })

export class Accounts extends Context.Service<Accounts, AggregateService.AggregateService<typeof Account>>()(
  "library/Accounts"
) {
  static readonly layer = Layer.effect(Accounts, AggregateService.make(Account))
}
// #endregion

export const Library = Layer.merge(Accounts.layer, Books.layer).pipe(Layer.provideMerge(EventStore.layerMemory))

/** Alice owes 3 euros and pays twice, the second time while she still owed some. */
export const twice = Effect.gen(function*() {
  const accounts = yield* Accounts

  // #region twice
  yield* accounts.commands.chargeFine("alice", { amount: 300, reason: "Dune, 10 days late" })
  yield* accounts.commands.chargeFine("alice", { amount: 300, reason: "Neuromancer, 10 days late" })

  // The payment page sends 3 euros. The response is lost, and the page sends it again.
  yield* accounts.commands.pay("alice", { amount: 300 })
  yield* accounts.commands.pay("alice", { amount: 300 })

  const alice = yield* accounts.state("alice")
  // { balance: 0 }: it looks paid, and Alice was charged 6 euros for one click
  // #endregion

  return alice
})

/** The same, with the id the page made for the request. */
export const withCommandId = Effect.gen(function*() {
  const accounts = yield* Accounts

  // #region with-id
  yield* accounts.commands.chargeFine("alice", { amount: 300, reason: "Dune, 10 days late" })
  yield* accounts.commands.chargeFine("alice", { amount: 300, reason: "Neuromancer, 10 days late" })

  // The page makes one id when Alice clicks, and sends it with every attempt.
  const requestId = "7b6f0e1c-pay-dune"
  const first = yield* accounts.commands.pay("alice", { amount: 300 }, { commandId: requestId })
  const again = yield* accounts.commands.pay("alice", { amount: 300 }, { commandId: requestId })
  // again equals first: the same version, position and events, and nothing new is stored

  const alice = yield* accounts.state("alice")
  // { balance: 300 }: one payment, 3 euros still owed
  // #endregion

  // #region reused
  const reused = yield* Effect.flip(
    accounts.commands.chargeFine("alice", { amount: 100, reason: "Lost card" }, { commandId: requestId })
  )
  // reused._tag: "CommandIdReused", with previous: "pay" and command: "chargeFine"
  // #endregion

  return { first, again, alice, reused: reused._tag }
})

/** A loan carries its own id, so lending it again for the same loan is harmless. */
export const naturally = Effect.gen(function*() {
  const books = yield* Books

  // #region naturally
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  const first = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  const again = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  // first.events: [BookLent], again.events: []: the book already has that loan
  // #endregion

  return { first, again }
})
