import { evolve } from "effect-cqrs/Aggregate"
import { useSyncExternalStore } from "react"
import { devstore, type EventEntry, type Snapshot } from "./devstore.ts"
import { Account, type AccountState, initialAccount } from "./domain/account.ts"
import type { Rejection } from "./engine.ts"

// #region use-snapshot
export const useSnapshot = <A>(select: (snapshot: Snapshot) => A): A =>
  useSyncExternalStore(devstore.subscribe, () => select(devstore.get()))
// #endregion

export const head = (events: ReadonlyArray<EventEntry>) => events.at(-1)?.position ?? 0

// #region fold-at
/**
 * The aggregate's state is a left fold of its events. This is the same
 * projection the command side uses, run on the events up to `position`.
 */
export const foldAt = (events: ReadonlyArray<EventEntry>, position: number): Record<string, AccountState> => {
  const states: Record<string, AccountState> = {}
  for (const entry of events) {
    if (entry.position > position) break
    states[entry.aggregateId] = evolve(Account, states[entry.aggregateId] ?? initialAccount, entry.event, entry.stored)
  }
  return states
}
// #endregion

export const money = (amount: number) => `€${Math.round(amount).toLocaleString("en-US")}`

// #region explain
/** Each case narrows the error to its class, so its fields are typed. */
export const explain = (error: Rejection): string => {
  switch (error._tag) {
    case "InsufficientFunds":
      return `the balance is ${money(error.balance)}, the request ${money(error.requested)}`
    case "BalanceNotZero":
      return `${money(error.balance)} are still in the account`
    case "AccountNotOpen":
    case "AccountAlreadyOpened":
      return `account ${error.aggregateId}`
    default:
      return error.message
  }
}
// #endregion

export const signed = (amount: number) => (amount > 0 ? `+${money(amount)}` : amount < 0 ? `−${money(-amount)}` : "€0")

export const eventColor: Record<string, string> = {
  AccountOpened: "#a78bfa",
  MoneyDeposited: "#34d399",
  MoneyWithdrawn: "#fbbf24",
  TransferSent: "#f472b6",
  TransferReceived: "#38bdf8",
  TransferRefunded: "#fb923c",
  AccountClosed: "#94a3b8"
}

export const originColor: Record<string, string> = {
  app: "#a78bfa",
  saga: "#38bdf8",
  autopilot: "#fbbf24",
  console: "#34d399",
  seed: "#64748b"
}
