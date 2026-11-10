/**
 * The bank around the account aggregate: the service that runs its commands, the
 * saga that completes transfers and the view models the API reads from.
 */
import { Context, DateTime, Effect, Layer, Ref, Schema } from "effect"
import type { EventMeta } from "effect-cqrs/Aggregate"
import * as AggregateService from "effect-cqrs/AggregateService"
import * as Saga from "effect-cqrs/Saga"
import * as ViewModel from "effect-cqrs/ViewModel"
import { WorkflowEngine } from "effect/workflow"
import { Account, type AccountEvent, AccountNotOpen, balanceDelta, forgotten } from "./account.ts"

export class Accounts extends Context.Service<Accounts, AggregateService.AggregateService<typeof Account>>()(
  "workshop/Accounts"
) {}

// ---------------------------------------------------------------------------
// Transfers
// ---------------------------------------------------------------------------

/**
 * A transfer touches two accounts, so no single command can do it. For each
 * `TransferSent` the saga credits the other account, and refunds the sender when
 * that account isn't open. Both commands are idempotent per transfer, so the saga
 * can run again after a restart.
 */
export const Transfer = Saga.workflow("Transfer", {
  payload: { transferId: Schema.String, from: Schema.String, to: Schema.String, amount: Schema.Int },
  idempotencyKey: ({ transferId }) => transferId
})

const TransferWorkflow = Saga.toLayer(Transfer, ({ amount, from, to, transferId }) =>
  Effect.gen(function*() {
    const accounts = yield* Accounts
    yield* Saga.command("credit", {
      error: AccountNotOpen,
      execute: accounts.commands.receiveTransfer(to, { transferId, from, amount })
    }).pipe(
      Effect.catchTag("AccountNotOpen", ({ _tag }) =>
        Saga.command("refund", {
          execute: accounts.commands.refundTransfer(from, { transferId, to, amount, reason: _tag })
        }))
    )
  }))

export const Transfers = Saga.make({
  name: "transfers",
  aggregates: [Account],
  handlers: {
    TransferSent: ({ amount, to, transferId }, stored) =>
      Saga.start(Transfer, { transferId, from: stored.aggregateId, to, amount })
  }
})

/** Runs the transfer saga. Its engine and checkpoint live in memory: it starts again from the log. */
export const TransferSaga = Saga.layer([Transfers]).pipe(
  Layer.provideMerge(TransferWorkflow),
  Layer.provide(WorkflowEngine.layerMemory)
)

// ---------------------------------------------------------------------------
// Participants
// ---------------------------------------------------------------------------

export const Participant = Schema.Struct({
  id: Schema.String,
  nickname: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  balance: Schema.Number,
  joinedAt: Schema.DateTimeUtcFromString
})
export type Participant = typeof Participant.Type

export class ParticipantsStore extends Context.Service<ParticipantsStore, Ref.Ref<ReadonlyMap<string, Participant>>>()(
  "workshop/ParticipantsStore"
) {
  static readonly layer = Layer.effect(ParticipantsStore, Ref.make<ReadonlyMap<string, Participant>>(new Map()))
}

const updateParticipant = (id: string, f: (participant: Participant) => Participant) =>
  ParticipantsStore.use((store) =>
    Ref.update(store, (participants) => {
      const current = participants.get(id)
      return current === undefined ? participants : new Map(participants).set(id, f(current))
    })
  )

const moveBalance = (event: AccountEvent, { aggregateId }: EventMeta) =>
  updateParticipant(
    aggregateId,
    (participant) => ({ ...participant, balance: participant.balance + balanceDelta(event) })
  )

/** Everyone who joined, with their balance. Rebuilt from the log on every start. */
export const ParticipantsView = ViewModel.make({
  name: "participants",
  aggregates: [Account],
  dispose: ParticipantsStore.use((store) => Ref.set(store, new Map())),
  handlers: {
    AccountOpened: ({ nickname, openedAt }, { aggregateId }) =>
      ParticipantsStore.use((store) =>
        Ref.update(store, (participants) =>
          new Map(participants).set(aggregateId, {
            id: aggregateId,
            nickname,
            status: "open",
            balance: 0,
            joinedAt: openedAt
          }))
      ),
    MoneyDeposited: moveBalance,
    MoneyWithdrawn: moveBalance,
    TransferSent: moveBalance,
    TransferReceived: moveBalance,
    TransferRefunded: moveBalance,
    PurchasePaid: moveBalance,
    PurchaseRefunded: moveBalance,
    AccountClosed: (_, { aggregateId }) =>
      updateParticipant(aggregateId, (participant) => ({ ...participant, status: "closed" }))
  },
  // Events read from now on carry the replacement already: erase what is stored.
  onForget: (subject) => updateParticipant(subject, (participant) => ({ ...participant, nickname: forgotten }))
})

export const listParticipants = ParticipantsStore.use((store) =>
  Effect.map(
    Ref.get(store),
    (participants) =>
      Array.from(participants.values()).sort((a, b) =>
        DateTime.toEpochMillis(a.joinedAt) - DateTime.toEpochMillis(b.joinedAt)
      )
  )
)

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

export const LedgerEntry = Schema.Struct({
  position: Schema.Number,
  timestamp: Schema.DateTimeUtcFromString,
  type: Schema.String,
  /** Positive when money came in. */
  amount: Schema.Number,
  /** The other account of a transfer. */
  counterparty: Schema.optionalKey(Schema.String),
  transferId: Schema.optionalKey(Schema.String),
  orderId: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String)
})
export type LedgerEntry = typeof LedgerEntry.Type

/** How many movements the ledger keeps per account, newest first. */
export const ledgerLength = 50

export class LedgerStore
  extends Context.Service<LedgerStore, Ref.Ref<ReadonlyMap<string, ReadonlyArray<LedgerEntry>>>>()(
    "workshop/LedgerStore"
  )
{
  static readonly layer = Layer.effect(
    LedgerStore,
    Ref.make<ReadonlyMap<string, ReadonlyArray<LedgerEntry>>>(new Map())
  )
}

const record = (event: AccountEvent, { aggregateId, position, timestamp }: EventMeta) => {
  const entry: LedgerEntry = {
    position,
    timestamp,
    type: event._tag,
    amount: balanceDelta(event),
    ...(event._tag === "TransferSent" || event._tag === "TransferRefunded" ?
      { counterparty: event.to, transferId: event.transferId } :
      {}),
    ...(event._tag === "TransferReceived" ? { counterparty: event.from, transferId: event.transferId } : {}),
    ...(event._tag === "PurchasePaid" ? { orderId: event.orderId } : {}),
    ...(event._tag === "PurchaseRefunded" ? { orderId: event.orderId } : {}),
    ...(event._tag === "TransferRefunded" || event._tag === "PurchaseRefunded" ? { reason: event.reason } : {})
  }
  return LedgerStore.use((store) =>
    Ref.update(store, (ledgers) => {
      const entries = ledgers.get(aggregateId) ?? []
      // Idempotent: an event seen again is not recorded twice.
      if (entries.some((existing) => existing.position === position)) return ledgers
      return new Map(ledgers).set(aggregateId, [entry, ...entries].slice(0, ledgerLength))
    })
  )
}

/** The movements of each account. It holds no personal data. */
export const LedgerView = ViewModel.make({
  name: "ledger",
  aggregates: [Account],
  dispose: LedgerStore.use((store) => Ref.set(store, new Map())),
  handlers: {
    MoneyDeposited: record,
    MoneyWithdrawn: record,
    TransferSent: record,
    TransferReceived: record,
    TransferRefunded: record,
    PurchasePaid: record,
    PurchaseRefunded: record
  }
})

export const ledgerOf = (accountId: string) =>
  LedgerStore.use((store) => Effect.map(Ref.get(store), (ledgers) => ledgers.get(accountId) ?? []))

// ---------------------------------------------------------------------------
// Books
// ---------------------------------------------------------------------------

/**
 * Where the money went, counted by kind of movement. It is folded apart from the
 * participants' balances on purpose: the control room checks that both agree.
 */
export const Books = Schema.Struct({
  deposited: Schema.Number,
  withdrawn: Schema.Number,
  /** Paid for orders, and given back for orders cancelled or not delivered. */
  purchases: Schema.Number,
  purchaseRefunds: Schema.Number,
  transfersSent: Schema.Number,
  transfersReceived: Schema.Number,
  transfersRefunded: Schema.Number
})
export type Books = typeof Books.Type

export const emptyBooks: Books = {
  deposited: 0,
  withdrawn: 0,
  purchases: 0,
  purchaseRefunds: 0,
  transfersSent: 0,
  transfersReceived: 0,
  transfersRefunded: 0
}

export class BooksStore extends Context.Service<BooksStore, Ref.Ref<Books>>()("workshop/BooksStore") {
  static readonly layer = Layer.effect(BooksStore, Ref.make(emptyBooks))
}

const add = (key: keyof Books) => ({ amount }: { readonly amount: number }) =>
  BooksStore.use((store) => Ref.update(store, (books) => ({ ...books, [key]: books[key] + amount })))

export const BooksView = ViewModel.make({
  name: "books",
  aggregates: [Account],
  dispose: BooksStore.use((store) => Ref.set(store, emptyBooks)),
  handlers: {
    MoneyDeposited: add("deposited"),
    MoneyWithdrawn: add("withdrawn"),
    PurchasePaid: add("purchases"),
    PurchaseRefunded: add("purchaseRefunds"),
    TransferSent: add("transfersSent"),
    TransferReceived: add("transfersReceived"),
    TransferRefunded: add("transfersRefunded")
  }
})

/** What the participants' balances should add up to, according to the books. */
export const expectedBalances = (books: Books) =>
  books.deposited - books.withdrawn - (books.purchases - books.purchaseRefunds) -
  (books.transfersSent - books.transfersReceived - books.transfersRefunded)

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

/** The bank's view models. `domain/workshop.ts` runs them with the shop's. */
export const bankViews = [ParticipantsView, LedgerView, BooksView] as const

export const BankStores = Layer.mergeAll(ParticipantsStore.layer, LedgerStore.layer, BooksStore.layer)

export const AccountsLive = Layer.effect(Accounts, AggregateService.make(Account))
