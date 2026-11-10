/**
 * A participant's bank account. The balance is never stored: it is folded from
 * the money that went in and out. The nickname is personal data, encrypted with
 * a key of the account's own that closing the account destroys.
 */
import { DateTime, Effect, Schema } from "effect"
import * as Aggregate from "effect-cqrs/Aggregate"
import * as PersonalData from "effect-cqrs/PersonalData"

export const forgotten = "[forgotten]"

export const Nickname = Schema.Trim.pipe(
  Schema.decodeTo(Schema.NonEmptyString.check(Schema.isMaxLength(24)))
)

export const Amount = Schema.Int.check(Schema.isGreaterThan(0))

/** The most one deposit brings in, so that nobody inflates the bank with one click. */
export const maxDeposit = 100

export const DepositAmount = Amount.check(Schema.isLessThanOrEqualTo(maxDeposit))

export const AccountEvent = Schema.TaggedUnion({
  AccountOpened: {
    nickname: PersonalData.field(Schema.String, { redacted: forgotten }),
    openedAt: Schema.DateTimeUtc
  },
  MoneyDeposited: { amount: Amount },
  /** Only when the account closes: what was left is paid out. */
  MoneyWithdrawn: { amount: Amount },
  TransferSent: { transferId: Schema.String, to: Schema.String, amount: Amount },
  TransferReceived: { transferId: Schema.String, from: Schema.String, amount: Amount },
  TransferRefunded: { transferId: Schema.String, to: Schema.String, amount: Amount, reason: Schema.String },
  /** Money taken for an order in the shop. */
  PurchasePaid: { orderId: Schema.String, amount: Amount },
  /** Money given back for an order the shop couldn't deliver or the customer cancelled. */
  PurchaseRefunded: { orderId: Schema.String, amount: Amount, reason: Schema.String },
  AccountClosed: { closedAt: Schema.DateTimeUtc }
})
export type AccountEvent = typeof AccountEvent.Type

export class AccountAlreadyOpened extends Schema.TaggedError<AccountAlreadyOpened>()("AccountAlreadyOpened", {
  accountId: Schema.String
}, { httpApiStatus: 409 }) {}

export class AccountNotOpen extends Schema.TaggedError<AccountNotOpen>()("AccountNotOpen", {
  accountId: Schema.String
}, { httpApiStatus: 409 }) {}

export class InsufficientFunds extends Schema.TaggedError<InsufficientFunds>()("InsufficientFunds", {
  balance: Schema.Number,
  requested: Schema.Number
}, { httpApiStatus: 422 }) {}

export class SameAccountTransfer extends Schema.TaggedError<SameAccountTransfer>()("SameAccountTransfer", {}, {
  httpApiStatus: 422
}) {}

export interface AccountState {
  readonly status: "none" | "open" | "closed"
  readonly nickname: string
  readonly balance: number
  /** Transfers already credited, so a redelivered `receiveTransfer` records nothing. */
  readonly received: ReadonlyArray<string>
  /** Transfers already refunded, for the same reason. */
  readonly refunded: ReadonlyArray<string>
  /** Orders paid, and orders refunded, so a saga that runs a step again pays or refunds once. */
  readonly purchases: ReadonlyArray<string>
  readonly purchaseRefunds: ReadonlyArray<string>
}

export const AccountStatus = Schema.Literals(["none", "open", "closed"])

export const initialAccount: AccountState = {
  status: "none",
  nickname: "",
  balance: 0,
  received: [],
  refunded: [],
  purchases: [],
  purchaseRefunds: []
}

export const Account = Aggregate.make<AccountState>()({
  name: "account",
  events: AccountEvent,
  initial: initialAccount,
  projection: {
    AccountOpened: (state, { nickname }) => ({ ...state, status: "open", nickname }),
    MoneyDeposited: (state, { amount }) => ({ ...state, balance: state.balance + amount }),
    MoneyWithdrawn: (state, { amount }) => ({ ...state, balance: state.balance - amount }),
    TransferSent: (state, { amount }) => ({ ...state, balance: state.balance - amount }),
    TransferReceived: (state, { amount, transferId }) => ({
      ...state,
      balance: state.balance + amount,
      received: [...state.received, transferId]
    }),
    TransferRefunded: (state, { amount, transferId }) => ({
      ...state,
      balance: state.balance + amount,
      refunded: [...state.refunded, transferId]
    }),
    PurchasePaid: (state, { amount, orderId }) => ({
      ...state,
      balance: state.balance - amount,
      purchases: [...state.purchases, orderId]
    }),
    PurchaseRefunded: (state, { amount, orderId }) => ({
      ...state,
      balance: state.balance + amount,
      purchaseRefunds: [...state.purchaseRefunds, orderId]
    }),
    AccountClosed: (state) => ({ ...state, status: "closed" })
  }
})
  .command("openAccount", {
    payload: Schema.Struct({ nickname: Nickname }),
    handler: Effect.fnUntraced(function*(state, { nickname }, { aggregateId }) {
      if (state.status !== "none") return yield* new AccountAlreadyOpened({ accountId: aggregateId })
      return AccountEvent.cases.AccountOpened.make({ nickname, openedAt: yield* DateTime.now })
    })
  })
  .command("deposit", {
    payload: Schema.Struct({ amount: DepositAmount }),
    handler: Effect.fnUntraced(function*(state, { amount }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ accountId: aggregateId })
      return AccountEvent.cases.MoneyDeposited.make({ amount })
    })
  })
  .command("sendTransfer", {
    payload: Schema.Struct({ transferId: Schema.NonEmptyString, to: Schema.NonEmptyString, amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount, to, transferId }, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ accountId: aggregateId })
      if (to === aggregateId) return yield* new SameAccountTransfer()
      if (state.balance < amount) return yield* new InsufficientFunds({ balance: state.balance, requested: amount })
      return AccountEvent.cases.TransferSent.make({ transferId, to, amount })
    })
  })
  .command("receiveTransfer", {
    payload: Schema.Struct({ transferId: Schema.NonEmptyString, from: Schema.NonEmptyString, amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount, from, transferId }, { aggregateId }) {
      if (state.received.includes(transferId)) return []
      if (state.status !== "open") return yield* new AccountNotOpen({ accountId: aggregateId })
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
    // A refund goes through even on a closed account: the money was the sender's.
    handler: Effect.fnUntraced(function*(state, { amount, reason, to, transferId }) {
      if (state.refunded.includes(transferId)) return []
      return AccountEvent.cases.TransferRefunded.make({ transferId, to, amount, reason })
    })
  })
  .command("payOrder", {
    payload: Schema.Struct({ orderId: Schema.NonEmptyString, amount: Amount }),
    handler: Effect.fnUntraced(function*(state, { amount, orderId }, { aggregateId }) {
      if (state.purchases.includes(orderId)) return []
      if (state.status !== "open") return yield* new AccountNotOpen({ accountId: aggregateId })
      if (state.balance < amount) return yield* new InsufficientFunds({ balance: state.balance, requested: amount })
      return AccountEvent.cases.PurchasePaid.make({ orderId, amount })
    })
  })
  .command("refundOrder", {
    payload: Schema.Struct({ orderId: Schema.NonEmptyString, amount: Amount, reason: Schema.String }),
    // Like a transfer's refund, it goes through on a closed account too.
    handler: Effect.fnUntraced(function*(state, { amount, orderId, reason }) {
      if (!state.purchases.includes(orderId) || state.purchaseRefunds.includes(orderId)) return []
      return AccountEvent.cases.PurchaseRefunded.make({ orderId, amount, reason })
    })
  })
  .command("closeAccount", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
      if (state.status !== "open") return yield* new AccountNotOpen({ accountId: aggregateId })
      const closed = AccountEvent.cases.AccountClosed.make({ closedAt: yield* DateTime.now })
      return state.balance > 0 ? [AccountEvent.cases.MoneyWithdrawn.make({ amount: state.balance }), closed] : closed
    })
  })

/** How much an event moves the balance. */
export const balanceDelta = (event: AccountEvent): number => {
  switch (event._tag) {
    case "MoneyDeposited":
    case "TransferReceived":
    case "TransferRefunded":
    case "PurchaseRefunded":
      return event.amount
    case "MoneyWithdrawn":
    case "TransferSent":
    case "PurchasePaid":
      return -event.amount
    default:
      return 0
  }
}
