/**
 * The read side. Each view model folds the same events into a different shape,
 * keeps its own checkpoint and can be thrown away and rebuilt by a replay.
 *
 * Their storage is the devstore, so the screen shows exactly what they hold. A
 * configurable sleep per event makes the lag behind the event store visible.
 */
import { Duration, Effect, Schema } from "effect"
import type { EventMeta } from "effect-cqrs/Aggregate"
import * as ViewModel from "effect-cqrs/ViewModel"
import { devstore, type FeedItem, type ViewName } from "../devstore.ts"
import { Account, type AccountEvent, balanceDelta } from "./account.ts"

export class ProjectionCrashed extends Schema.TaggedError<ProjectionCrashed>()("ProjectionCrashed", {
  reason: Schema.String
}) {}

const forgotten = "[forgotten]"

/** How slow each view model is compared to the projection latency setting. */
export const lagFactor: Record<ViewName, number> = { accounts: 1, totals: 1.6, feed: 0.6 }

const pace = (name: ViewName) =>
  Effect.suspend(() => {
    const { settings, views } = devstore.get()
    const millis = views[name].replaying ? settings.replayDelay : settings.projectionDelay * lagFactor[name]
    return millis <= 0 ? Effect.void : Effect.sleep(Duration.millis(millis))
  })

type Handler<Tag extends AccountEvent["_tag"]> = (
  event: Extract<AccountEvent, { readonly _tag: Tag }>,
  meta: EventMeta
) => Effect.Effect<void, ProjectionCrashed>

type AllHandlers = { readonly [Tag in AccountEvent["_tag"]]: Handler<Tag> }

/** Builds a handler for every event type from one function, paced like a slow database. */
const everyEvent = (
  name: ViewName,
  apply: (event: AccountEvent, meta: EventMeta) => Effect.Effect<void, ProjectionCrashed>
): AllHandlers => {
  const handler = (event: AccountEvent, meta: EventMeta) => Effect.andThen(pace(name), apply(event, meta))
  return {
    AccountOpened: handler,
    MoneyDeposited: handler,
    MoneyWithdrawn: handler,
    TransferSent: handler,
    TransferReceived: handler,
    TransferRefunded: handler,
    AccountClosed: handler
  }
}

// #region accounts-view
export const AccountsView = ViewModel.make({
  name: "accounts",
  aggregates: [Account],
  dispose: Effect.sync(() => devstore.dispose("accounts")),
  handlers: everyEvent(
    "accounts",
    (event, { aggregateId, position }) =>
      Effect.sync(() =>
        devstore.apply("accounts", position, (accounts) => {
          const current = accounts[aggregateId]
          if (event._tag === "AccountOpened") {
            return {
              ...accounts,
              [aggregateId]: {
                id: aggregateId,
                owner: event.owner,
                color: event.color,
                balance: 0,
                status: "open",
                txCount: 0,
                lastPosition: position,
                lastDelta: 0
              }
            }
          }
          // Idempotent: an event seen again leaves the row as it is.
          if (current === undefined || current.lastPosition >= position) return accounts
          const delta = balanceDelta(event)
          return {
            ...accounts,
            [aggregateId]: {
              ...current,
              balance: current.balance + delta,
              status: event._tag === "AccountClosed" ? "closed" : current.status,
              txCount: current.txCount + (delta === 0 ? 0 : 1),
              lastPosition: position,
              lastDelta: delta
            }
          }
        })
      )
  ),
  // The owner's name is personal data: once they are forgotten, the row keeps the
  // balance and loses the name.
  onForget: (subject, { position }) =>
    Effect.sync(() =>
      devstore.apply("accounts", position, (accounts) => {
        const current = accounts[subject]
        return current === undefined ? accounts : { ...accounts, [subject]: { ...current, owner: forgotten } }
      })
    )
})
// #endregion

export const TotalsView = ViewModel.make({
  name: "bank-totals",
  aggregates: [Account],
  dispose: Effect.sync(() => devstore.dispose("totals")),
  handlers: everyEvent("totals", (event, { position }) =>
    Effect.gen(function*() {
      if (devstore.get().settings.breakTotals) {
        return yield* new ProjectionCrashed({ reason: `handler threw on ${event._tag} #${position}` })
      }
      devstore.apply("totals", position, (totals) => {
        const delta = balanceDelta(event)
        const amount = "amount" in event ? event.amount : 0
        return {
          moneyInBank: totals.moneyInBank + delta,
          inFlight: totals.inFlight + (event._tag === "TransferSent" ? amount : 0) -
            (event._tag === "TransferReceived" || event._tag === "TransferRefunded" ? amount : 0),
          deposited: totals.deposited + (event._tag === "MoneyDeposited" ? amount : 0),
          withdrawn: totals.withdrawn + (event._tag === "MoneyWithdrawn" ? amount : 0),
          transferred: totals.transferred + (event._tag === "TransferReceived" ? amount : 0),
          refunded: totals.refunded + (event._tag === "TransferRefunded" ? amount : 0),
          openAccounts: totals.openAccounts +
            (event._tag === "AccountOpened" ? 1 : event._tag === "AccountClosed" ? -1 : 0),
          byType: { ...totals.byType, [event._tag]: (totals.byType[event._tag] ?? 0) + 1 }
        }
      })
    }))
})

const describe = (event: AccountEvent): string => {
  switch (event._tag) {
    case "AccountOpened":
      return `${event.owner} opened an account`
    case "MoneyDeposited":
      return `deposited €${event.amount}`
    case "MoneyWithdrawn":
      return `withdrew €${event.amount}`
    case "TransferSent":
      return `sent €${event.amount} to ${event.to}`
    case "TransferReceived":
      return `received €${event.amount} from ${event.from}`
    case "TransferRefunded":
      return `refunded €${event.amount}: ${event.reason}`
    case "AccountClosed":
      return "closed the account"
  }
}

const feedSize = 40

export const FeedView = ViewModel.make({
  name: "activity-feed",
  aggregates: [Account],
  dispose: Effect.sync(() => devstore.dispose("feed")),
  handlers: everyEvent(
    "feed",
    (event, { aggregateId, position }) =>
      Effect.sync(() =>
        devstore.apply("feed", position, (feed) => {
          if (feed.some((item) => item.position === position)) return feed
          const item: FeedItem = {
            position,
            aggregateId,
            type: event._tag,
            text: describe(event),
            delta: balanceDelta(event)
          }
          return [item, ...feed].slice(0, feedSize)
        })
      )
  ),
  // Only the opening line names the owner.
  onForget: (subject, { position }) =>
    Effect.sync(() =>
      devstore.apply(
        "feed",
        position,
        (feed) =>
          feed.map((item) =>
            item.aggregateId === subject && item.type === "AccountOpened"
              ? { ...item, text: `${forgotten} opened an account` }
              : item
          )
      )
    )
})

export const allViews = [AccountsView, TotalsView, FeedView] as const
