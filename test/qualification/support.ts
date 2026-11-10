/**
 * What the qualification tests share: how a test claims a requirement, the storage
 * configurations they run on, and an aggregate with an invariant to defend.
 */
import { Effect, Layer, Schema, Stream } from "effect"
import { Aggregate, EventStore } from "effect-cqrs"
import { type Backend, postgresUrl, storage } from "../../bench/common.ts"
import { anomalies, type AnomalyId, type RequirementId } from "./requirements.ts"

/**
 * The title of a test that verifies requirements: `[ES-001] stores nothing ...`.
 * `report.ts` reads the ids back from the results, so an id has to be one of
 * `requirements.ts`, which the type checks.
 */
export const verifies = (ids: RequirementId | ReadonlyArray<RequirementId>, behaviour: string): string =>
  `${(typeof ids === "string" ? [ids] : ids).map((id) => `[${id}]`).join("")} ${behaviour}`

/**
 * The title of an `it.fails` test that shows a known anomaly: the requirement it
 * breaks, then the anomaly, as in `[ES-014][ANOM-001] ...`.
 */
export const knownAnomaly = (id: AnomalyId, behaviour: string): string =>
  `[${anomalies[id].requirement}][${id}] ${behaviour}`

export interface Configuration {
  readonly name: Backend
  /** Whether the tests can run on it here: PostgreSQL needs `POSTGRES_URL`. */
  readonly available: boolean
  /** Whether a constraint of the database can fail an append half way. */
  readonly sql: boolean
}

/**
 * Every storage backend, including PostgreSQL when it isn't configured: its tests
 * are then reported as not run, rather than missing from the report.
 */
export const configurations: ReadonlyArray<Configuration> = [
  { name: "memory", available: true, sql: false },
  { name: "sqlite-memory", available: true, sql: true },
  { name: "sqlite-file", available: true, sql: true },
  { name: "postgres", available: postgresUrl !== undefined, sql: true }
]

/** The suffix of a `describe` title that names its configuration, which `report.ts` reads back. */
export const on = (configuration: Pick<Configuration, "name">): string => `on ${configuration.name}`

/** An empty store with checkpoints, new for each `Effect.provide`. */
export const storageOf = (configuration: Configuration) => Layer.suspend(() => storage(configuration.name))

export const range = (count: number): ReadonlyArray<number> => Array.from({ length: count }, (_, index) => index)

export const readAll = (filter?: EventStore.EventFilter) =>
  EventStore.EventStore.use((store) => Stream.runCollect(store.read(filter)))

export const head = EventStore.EventStore.use((store) => store.head)

/** The tag of the error an effect fails with, or `"ok"`. */
export const outcome = <A, E extends { readonly _tag: string }, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.match(effect, { onFailure: (error): string => error._tag, onSuccess: () => "ok" })

/**
 * A pseudo-random sequence that is the same on every run, so a failure can be
 * reproduced from the report. Mulberry32.
 */
export const seeded = (seed: number) => {
  let state = seed >>> 0
  return (): number => {
    state = (state + 0x6D2B79F5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

// ---------------------------------------------------------------------------
// A ledger that must never give out more than its limit
// ---------------------------------------------------------------------------

export const LedgerEvent = Schema.TaggedUnion({
  Opened: { limit: Schema.Int },
  Drawn: { amount: Schema.Int, by: Schema.String },
  Closed: {}
})

export class NotOpen extends Schema.TaggedError<NotOpen>()("NotOpen", {
  status: Schema.String
}) {}

export class LimitExceeded extends Schema.TaggedError<LimitExceeded>()("LimitExceeded", {
  limit: Schema.Int,
  drawn: Schema.Int,
  amount: Schema.Int
}) {}

export const LedgerState = Schema.Struct({
  status: Schema.Literals(["new", "open", "closed"]),
  limit: Schema.Int,
  drawn: Schema.Int,
  draws: Schema.Int
})
export type LedgerState = typeof LedgerState.Type

const Amount = Schema.Int.check(Schema.isGreaterThan(0))

/**
 * The invariant every test relies on: `drawn` never exceeds `limit`. The handlers
 * let other fibers run before they decide, as a handler that reads a service
 * would, so writers that can race do.
 */
export const Ledger = Aggregate.make<LedgerState>()({
  name: "ledger",
  events: LedgerEvent,
  initial: { status: "new", limit: 0, drawn: 0, draws: 0 },
  projection: {
    Opened: (state, { limit }) => ({ ...state, status: "open", limit }),
    Drawn: (state, { amount }) => ({ ...state, drawn: state.drawn + amount, draws: state.draws + 1 }),
    Closed: (state) => ({ ...state, status: "closed" })
  }
})
  .command("open", {
    payload: Schema.Struct({ limit: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
    handler: Effect.fnUntraced(function*(state, { limit }) {
      if (state.status !== "new") return yield* new NotOpen({ status: state.status })
      return LedgerEvent.cases.Opened.make({ limit })
    })
  })
  .command("draw", {
    payload: Schema.Struct({ amount: Amount, by: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { amount, by }) {
      yield* Effect.yieldNow
      if (state.status !== "open") return yield* new NotOpen({ status: state.status })
      if (state.drawn + amount > state.limit) {
        return yield* new LimitExceeded({ limit: state.limit, drawn: state.drawn, amount })
      }
      return LedgerEvent.cases.Drawn.make({ amount, by })
    })
  })
  .command("drawEach", {
    payload: Schema.Struct({ amounts: Schema.Array(Amount), by: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { amounts, by }) {
      yield* Effect.yieldNow
      if (state.status !== "open") return yield* new NotOpen({ status: state.status })
      const amount = amounts.reduce((sum, each) => sum + each, 0)
      if (state.drawn + amount > state.limit) {
        return yield* new LimitExceeded({ limit: state.limit, drawn: state.drawn, amount })
      }
      return amounts.map((each) => LedgerEvent.cases.Drawn.make({ amount: each, by }))
    })
  })
  .command("close", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state) {
      if (state.status !== "open") return yield* new NotOpen({ status: state.status })
      return LedgerEvent.cases.Closed.make({})
    })
  })

export const ledgerStream = (aggregateId: string) =>
  readAll({ aggregateNames: [Ledger.name], aggregateIds: [aggregateId] })
