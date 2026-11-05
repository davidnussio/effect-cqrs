import { useSyncExternalStore } from "react"
import { evolve } from "effect-cqrs/Aggregate"
import { devstore, type EventEntry, type Snapshot } from "./devstore.ts"
import { Account, type AccountState, initialAccount } from "./domain/account.ts"

export const useSnapshot = <A>(select: (snapshot: Snapshot) => A): A =>
  useSyncExternalStore(devstore.subscribe, () => select(devstore.get()))

export const head = (events: ReadonlyArray<EventEntry>) => events.at(-1)?.position ?? 0

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

export const money = (amount: number) => `€${Math.round(amount).toLocaleString("en-US")}`

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
