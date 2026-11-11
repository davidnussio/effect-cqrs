/**
 * Two actors that run inside the server: a bot that restocks the shop, and a
 * simulator of shoppers that keeps the demo moving when the room is quiet. Both
 * use the same aggregate services as the HTTP handlers, and record who they are in
 * the metadata of the events they cause.
 *
 * With several replicas, each actor runs on the one that holds its lease, from the
 * `Leases` in context, and keeps its settings and counters in the `SharedState`, so
 * the presenter tunes it from whichever replica answers.
 */
import {
  Clock,
  Context,
  DateTime,
  Duration,
  Effect,
  Layer,
  Option,
  Random,
  Ref,
  Schedule,
  Schema,
  Semaphore
} from "effect"
import * as Ids from "effect-cqrs/Ids"
import * as Leases from "effect-cqrs/Leases"
import * as Metadata from "effect-cqrs/Metadata"
import { Accounts, ParticipantsStore } from "./bank.ts"
import { shared } from "./shared.ts"
import { type CatalogProduct, CatalogStore, maxQuantity, Orders, OrdersStore, Products } from "./shop.ts"

/** The lease called `name`, or one always held when nothing coordinates replicas. */
const lease = (name: string) =>
  Effect.flatMap(
    Effect.serviceOption(Leases.Leases),
    (leases) => Option.getOrElse(leases, () => Leases.memory).acquire(name)
  )

// ---------------------------------------------------------------------------
// Restock bot
// ---------------------------------------------------------------------------

export const BotSettings = Schema.Struct({
  enabled: Schema.Boolean,
  /** How often the bot looks at the shelves. */
  intervalSeconds: Schema.Int.check(Schema.isBetween({ minimum: 5, maximum: 600 })),
  /** It restocks a product with this many pieces or fewer. */
  threshold: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 20 })),
  /** How many pieces it adds, on top of what the queue is waiting for. */
  batch: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }))
})
export type BotSettings = typeof BotSettings.Type

export const Restock = Schema.Struct({ productId: Schema.String, quantity: Schema.Number })
export type Restock = typeof Restock.Type

export const BotStatus = Schema.Struct({
  settings: BotSettings,
  nextRunAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  lastRunAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  runs: Schema.Number,
  /** Pieces added since the server started. */
  restocked: Schema.Number,
  lastRestocks: Schema.Array(Restock)
})
export type BotStatus = typeof BotStatus.Type

export const defaultBotSettings: BotSettings = { enabled: true, intervalSeconds: 30, threshold: 2, batch: 5 }

/** The most one restock adds, the limit of the product's command. */
const maxRestock = 50

/**
 * What the bot restocks: every product at or below the threshold, and every
 * product with orders waiting. It adds the batch plus what the queue still lacks.
 */
export const planRestocks = (
  catalog: ReadonlyArray<CatalogProduct>,
  settings: BotSettings
): ReadonlyArray<Restock> =>
  catalog.flatMap((product) => {
    const waiting = product.queue.reduce((sum, entry) => sum + entry.quantity, 0)
    if (product.available > settings.threshold && waiting === 0) return []
    const missing = Math.max(0, waiting - product.available)
    return [{ productId: product.id, quantity: Math.min(maxRestock, settings.batch + missing) }]
  })

export class Bot extends Context.Service<Bot, {
  readonly status: Effect.Effect<BotStatus>
  configure(patch: Partial<BotSettings>): Effect.Effect<BotStatus>
  /** Looks at the shelves now, whatever the schedule says. */
  readonly runNow: Effect.Effect<BotStatus>
}>()("workshop/Bot") {}

const BotState = Schema.Struct({
  settings: BotSettings,
  nextRunAt: Schema.NullOr(Schema.Number),
  lastRunAt: Schema.NullOr(Schema.Number),
  /** The presenter asked for a run, from a replica that doesn't run the bot. */
  requested: Schema.Boolean,
  runs: Schema.Number,
  restocked: Schema.Number,
  lastRestocks: Schema.Array(Restock)
})
type BotState = typeof BotState.Type

const toDateTime = (millis: number | null) => millis === null ? undefined : DateTime.makeUnsafe(millis)

export const makeBot = Effect.fnUntraced(function*(initial: BotSettings = defaultBotSettings) {
  const products = yield* Products
  const catalog = yield* CatalogStore
  const now = yield* Clock.currentTimeMillis
  const state = yield* shared("bot", BotState, {
    settings: initial,
    nextRunAt: initial.enabled ? now + initial.intervalSeconds * 1000 : null,
    lastRunAt: null,
    requested: false,
    runs: 0,
    restocked: 0,
    lastRestocks: []
  })
  // The first replica to start stores the defaults; the others find them.
  yield* state.update((current) => current)
  const held = yield* lease("workshop/bot")
  // The schedule and `runNow` never restock at the same time.
  const lock = yield* Semaphore.make(1)

  const status = Effect.map(state.get, (current): BotStatus => {
    const nextRunAt = toDateTime(current.nextRunAt)
    const lastRunAt = toDateTime(current.lastRunAt)
    return {
      settings: current.settings,
      ...(nextRunAt === undefined ? {} : { nextRunAt }),
      ...(lastRunAt === undefined ? {} : { lastRunAt }),
      runs: current.runs,
      restocked: current.restocked,
      lastRestocks: current.lastRestocks
    }
  })

  const run = lock.withPermit(Effect.gen(function*() {
    const { settings } = yield* state.get
    const plan = planRestocks(Array.from((yield* Ref.get(catalog)).values()), settings)
    const done = yield* Effect.forEach(
      plan,
      (restock) =>
        products.commands.restock(restock.productId, { quantity: restock.quantity }).pipe(
          Effect.as([restock]),
          Effect.catchCause((cause) => Effect.as(Effect.logWarning("The bot couldn't restock", cause), []))
        )
    ).pipe(Effect.map((results) => results.flat()))
    const now = yield* Clock.currentTimeMillis
    yield* state.update((current): BotState => ({
      ...current,
      lastRunAt: now,
      nextRunAt: current.settings.enabled ? now + current.settings.intervalSeconds * 1000 : null,
      requested: false,
      runs: current.runs + 1,
      restocked: current.restocked + done.reduce((sum, restock) => sum + restock.quantity, 0),
      lastRestocks: done
    }))
    if (done.length > 0) {
      yield* Effect.logInfo(`The bot restocked ${done.map((r) => `${r.quantity} ${r.productId}`).join(", ")}`)
    }
  })).pipe(Metadata.withMetadata({ actor: "bot" }))

  // Wakes up every second and runs when it is time, where the lease is held: a new
  // interval applies at once, and a replica that takes the lease over goes on with
  // the schedule.
  yield* Effect.gen(function*() {
    yield* Effect.sleep(Duration.seconds(1))
    if (!(yield* held.held)) return
    const current = yield* state.get
    const now = yield* Clock.currentTimeMillis
    const due = current.settings.enabled && current.nextRunAt !== null && now >= current.nextRunAt
    if (due || current.requested) yield* run
  }).pipe(Effect.forever, Effect.forkScoped)

  const configure = Effect.fnUntraced(function*(patch: Partial<BotSettings>) {
    const now = yield* Clock.currentTimeMillis
    yield* state.update((current) => {
      const settings = { ...current.settings, ...patch }
      const rescheduled = settings.enabled &&
        (!current.settings.enabled || settings.intervalSeconds !== current.settings.intervalSeconds)
      return {
        ...current,
        settings,
        nextRunAt: !settings.enabled
          ? null
          : rescheduled
          ? now + settings.intervalSeconds * 1000
          : current.nextRunAt
      }
    })
    return yield* status
  })

  // The replica that holds the lease runs the bot at once. Another one asks it to,
  // and waits a few seconds for the run to show.
  const runNow = Effect.gen(function*() {
    if (yield* held.held) return yield* Effect.andThen(run, status)
    const before = yield* state.modify((current) => [current.runs, { ...current, requested: true }] as const)
    yield* state.get.pipe(
      Effect.repeat({ until: (current) => current.runs > before, schedule: Schedule.spaced("250 millis") }),
      Effect.timeout("5 seconds"),
      Effect.ignore
    )
    return yield* status
  })

  return Bot.of({ status, configure, runNow })
})

// ---------------------------------------------------------------------------
// Shopper simulator
// ---------------------------------------------------------------------------

export const SimulatorSettings = Schema.Struct({
  enabled: Schema.Boolean,
  /** How many simulated shoppers take part. */
  shoppers: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })),
  /** How many things they do in a minute, all together. */
  actionsPerMinute: Schema.Int.check(Schema.isBetween({ minimum: 6, maximum: 600 }))
})
export type SimulatorSettings = typeof SimulatorSettings.Type

export const SimulatorStatus = Schema.Struct({
  settings: SimulatorSettings,
  actions: Schema.Number,
  /** The last few things the shoppers did, newest first. */
  recent: Schema.Array(Schema.String)
})
export type SimulatorStatus = typeof SimulatorStatus.Type

export const defaultSimulatorSettings: SimulatorSettings = { enabled: false, shoppers: 5, actionsPerMinute: 60 }

const SimulatorState = Schema.Struct({
  settings: SimulatorSettings,
  actions: Schema.Number,
  recent: Schema.Array(Schema.String)
})

export class Simulator extends Context.Service<Simulator, {
  readonly status: Effect.Effect<SimulatorStatus>
  configure(patch: Partial<SimulatorSettings>): Effect.Effect<SimulatorStatus>
  /** Makes one shopper do one thing now. */
  readonly step: Effect.Effect<void>
}>()("workshop/Simulator") {}

const names = [
  "Ada",
  "Bea",
  "Ciro",
  "Dora",
  "Ezio",
  "Fede",
  "Gino",
  "Ines",
  "Lia",
  "Max",
  "Nora",
  "Otto",
  "Pia",
  "Rino",
  "Sole",
  "Teo",
  "Ugo",
  "Vera",
  "Zeno",
  "Alba"
]

/** The account of the `index`th simulated shopper. Fixed, so a restart finds the same ones. */
export const shopperId = (index: number) => `sim-${index + 1}`

export const makeSimulator = Effect.fnUntraced(function*(initial: SimulatorSettings = defaultSimulatorSettings) {
  const accounts = yield* Accounts
  const orders = yield* Orders
  const products = yield* Products
  const catalog = yield* CatalogStore
  const participants = yield* ParticipantsStore
  const orderList = yield* OrdersStore
  const state = yield* shared("simulator", SimulatorState, { settings: initial, actions: 0, recent: [] })
  yield* state.update((current) => current)
  const held = yield* lease("workshop/simulator")

  const note = (line: string) =>
    state.update((current) => ({
      ...current,
      actions: current.actions + 1,
      recent: [line, ...current.recent].slice(0, 10)
    }))

  /** Opens the shoppers' accounts that don't exist yet. */
  const ensureShoppers = (count: number) =>
    Effect.forEach(Array.from({ length: count }, (_, index) => index), (index) =>
      Effect.gen(function*() {
        const id = shopperId(index)
        const account = yield* accounts.readModel(id)
        if (account.status === "none") {
          yield* accounts.commands.openAccount(id, { nickname: `🤖 ${names[index % names.length]}` })
        }
      }), { discard: true })

  const act = Effect.fnUntraced(function*(id: string) {
    const account = yield* accounts.readModel(id)
    if (account.status !== "open") return
    const nickname = account.nickname
    const roll = yield* Random.next

    if (account.balance < 20 || roll < 0.15) {
      const amount = yield* Random.nextIntBetween(20, 100)
      yield* accounts.commands.deposit(id, { amount })
      return yield* note(`${nickname} carica ${amount}`)
    }

    if (roll < 0.7) {
      const affordable = Array.from((yield* Ref.get(catalog)).values()).filter((p) => p.price <= account.balance)
      if (affordable.length === 0) return
      const product = yield* Random.choice(affordable)
      const quantity = Math.min(
        yield* Random.nextIntBetween(1, 2),
        Math.floor(account.balance / product.price),
        maxQuantity
      )
      const backorder = (yield* Random.next) < 0.5
      yield* orders.commands.place(yield* Ids.next, {
        customer: id,
        productId: product.id,
        quantity,
        total: product.price * quantity,
        backorder
      })
      return yield* note(`${nickname} ${backorder ? "prenota" : "compra"} ${quantity} ${product.emoji}`)
    }

    if (roll < 0.9) {
      const others = Array.from((yield* Ref.get(participants)).values()).filter((p) =>
        p.status === "open" && p.id !== id
      )
      if (others.length === 0) return
      const to = yield* Random.choice(others)
      const amount = yield* Random.nextIntBetween(1, Math.min(10, account.balance))
      yield* accounts.commands.sendTransfer(id, { transferId: yield* Ids.next, to: to.id, amount })
      return yield* note(`${nickname} manda ${amount} a ${to.nickname}`)
    }

    // Now and then someone gets tired of waiting.
    const waiting = Array.from((yield* Ref.get(orderList)).values()).filter((order) =>
      order.customer === id && order.status === "backordered"
    )
    if (waiting.length === 0) return
    const order = yield* Random.choice(waiting)
    yield* products.commands.dequeue(order.productId, { orderId: order.id })
    yield* orders.commands.cancel(order.id, {})
    yield* note(`${nickname} annulla una prenotazione`)
  })

  const step = Effect.gen(function*() {
    const { settings } = yield* state.get
    yield* ensureShoppers(settings.shoppers)
    const index = yield* Random.nextIntBetween(0, settings.shoppers - 1)
    yield* act(shopperId(index))
  }).pipe(
    // A refused command is part of the game: log it and carry on.
    Effect.catchCause((cause) => Effect.logDebug("A simulated shopper was refused", cause)),
    Metadata.withMetadata({ actor: "simulator" })
  )

  // The shoppers act on the replica that holds the lease.
  yield* Effect.gen(function*() {
    const { settings } = yield* state.get
    yield* Effect.sleep(Duration.millis(Math.round(60_000 / settings.actionsPerMinute)))
    if ((yield* held.held) && (yield* state.get).settings.enabled) yield* step
  }).pipe(Effect.forever, Effect.forkScoped)

  const status = Effect.map(state.get, ({ actions, recent, settings }): SimulatorStatus => ({
    settings,
    actions,
    recent
  }))

  const configure = Effect.fnUntraced(function*(patch: Partial<SimulatorSettings>) {
    yield* state.update((current) => ({ ...current, settings: { ...current.settings, ...patch } }))
    return yield* status
  })

  return Simulator.of({ status, configure, step })
})

/** The bot and the simulator, with their default settings. */
export const AutomationLive = Layer.mergeAll(
  Layer.effect(Bot, makeBot()),
  Layer.effect(Simulator, makeSimulator())
)
