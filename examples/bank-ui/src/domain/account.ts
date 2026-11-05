/**
 * The classic event sourcing example: a bank account whose balance is never
 * stored, only folded from the money that went in and out.
 */
import { DateTime, Effect, Schema } from "effect"
import * as Aggregate from "effect-cqrs/Aggregate"

const Amount = Schema.Int.check(Schema.isGreaterThan(0))

export const AccountEvent = Schema.TaggedUnion({
  AccountOpened: { owner: Schema.NonEmptyString, color: Schema.String, openedAt: Schema.DateTimeUtc },
  MoneyDeposited: { amount: Amount },
  MoneyWithdrawn: { amount: Amount },
  TransferSent: { transferId: Schema.String, to: Schema.String, amount: Amount },
  TransferReceived: { transferId: Schema.String, from: Schema.String, amount: Amount },
  TransferRefunded: { transferId: Schema.String, to: Schema.String, amount: Amount, reason: Schema.String },
  AccountClosed: { closedAt: Schema.DateTimeUtc }
})
export type AccountEvent = typeof AccountEvent.Type
export type AccountEventType = AccountEvent["_tag"]

export class AccountAlreadyOpened extends Schema.TaggedError<AccountAlreadyOpened>()("AccountAlreadyOpened", {
  aggregateId: Schema.String
}) {}

export class AccountNotOpen extends Schema.TaggedError<AccountNotOpen>()("AccountNotOpen", {
  aggregateId: Schema.String
}) {}

export class InsufficientFunds extends Schema.TaggedError<InsufficientFunds>()("InsufficientFunds", {
  balance: Schema.Number,
  requested: Schema.Number
}) {}

export class SameAccountTransfer extends Schema.TaggedError<SameAccountTransfer>()("SameAccountTransfer", {}) {}

export class BalanceNotZero extends Schema.TaggedError<BalanceNotZero>()("BalanceNotZero", {
  balance: Schema.Number
}) {}

export interface AccountState {
  readonly status: "none" | "open" | "closed"
  readonly owner: string
  readonly color: string
  readonly balance: number
  /** Transfers already credited, so a redelivered `receiveTransfer` records nothing. */
  readonly received: ReadonlyArray<string>
}

export const initialAccount: AccountState = { status: "none", owner: "", color: "", balance: 0, received: [] }

export const Account = Aggregate.make<AccountState>()({
  name: "account",
  events: AccountEvent,
  initial: initialAccount,
  projection: {
    AccountOpened: (state, { color, owner }) => ({ ...state, status: "open", owner, color }),
    MoneyDeposited: (state, { amount }) => ({ ...state, balance: state.balance + amount }),
    MoneyWithdrawn: (state, { amount }) => ({ ...state, balance: state.balance - amount }),
    TransferSent: (state, { amount }) => ({ ...state, balance: state.balance - amount }),
    TransferReceived: (state, { amount, transferId }) => ({
      ...state,
      balance: state.balance + amount,
      received: [...state.received, transferId]
    }),
    TransferRefunded: (state, { amount }) => ({ ...state, balance: state.balance + amount }),
    AccountClosed: (state) => ({ ...state, status: "closed" })
  }
})
  .command("openAccount", {
    payload: Schema.Struct({ owner: Schema.NonEmptyString, color: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { color, owner }, { aggregateId }) {
      if (state.status !== "none") return yield* new AccountAlreadyOpened({ aggregateId })
      return AccountEvent.cases.AccountOpened.make({ owner, color, openedAt: yield* DateTime.now })
    })
  })
  .command("deposit", {
    payload: Schema.Struct({ amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ aggregateId })
      return AccountEvent.cases.MoneyDeposited.make({ amount })
    })
  })
  .command("withdraw", {
    payload: Schema.Struct({ amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ aggregateId })
      if (state.balance < amount) return yield* new InsufficientFunds({ balance: state.balance, requested: amount })
      return AccountEvent.cases.MoneyWithdrawn.make({ amount })
    })
  })
  .command("sendTransfer", {
    payload: Schema.Struct({ transferId: Schema.NonEmptyString, to: Schema.NonEmptyString, amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount, to, transferId }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ aggregateId })
      if (to === aggregateId) return yield* new SameAccountTransfer()
      if (state.balance < amount) return yield* new InsufficientFunds({ balance: state.balance, requested: amount })
      return AccountEvent.cases.TransferSent.make({ transferId, to, amount })
    })
  })
  .command("receiveTransfer", {
    payload: Schema.Struct({ transferId: Schema.NonEmptyString, from: Schema.NonEmptyString, amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount, from, transferId }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ aggregateId })
      if (state.received.includes(transferId)) return []
      return AccountEvent.cases.TransferReceived.make({ transferId, from, amount })
    })
  })
  .command("refundTransfer", {
    payload: Schema.Struct({
      transferId: Schema.NonEmptyString,
      to: Schema.NonEmptyString,
      amount: Amount,
      reason: Schema.String
    }),
    handler: Effect.fnUntraced(function*(_, { amount, reason, to, transferId }) {
      return AccountEvent.cases.TransferRefunded.make({ transferId, to, amount, reason })
    })
  })
  .command("closeAccount", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ aggregateId })
      if (state.balance !== 0) return yield* new BalanceNotZero({ balance: state.balance })
      return AccountEvent.cases.AccountClosed.make({ closedAt: yield* DateTime.now })
    })
  })

export type AccountCommand = Aggregate.CommandName<typeof Account>

/** How much an event moves the balance, for ledgers and charts. */
export const balanceDelta = (event: AccountEvent): number => {
  switch (event._tag) {
    case "MoneyDeposited":
    case "TransferReceived":
    case "TransferRefunded":
      return event.amount
    case "MoneyWithdrawn":
    case "TransferSent":
      return -event.amount
    default:
      return 0
  }
}
