/**
 * Runs effect-cqrs in the browser: the in-memory event store, the account
 * aggregate, three view models, a saga for transfers and a command bus that can
 * be slowed down, paused and stepped so every stage is visible.
 */
import {
  Context,
  DateTime,
  Duration,
  Effect,
  Latch,
  Layer,
  ManagedRuntime,
  Metric,
  Schedule,
  Schema,
  Semaphore,
  Stream,
  Tracer
} from "effect"
import { KeyValueStore } from "effect/persistence"
import { WorkflowEngine } from "effect/workflow"
import * as AggregateService from "effect-cqrs/AggregateService"
import * as Checkpoints from "effect-cqrs/Checkpoints"
import * as EventStore from "effect-cqrs/EventStore"
import * as Metadata from "effect-cqrs/Metadata"
import * as Saga from "effect-cqrs/Saga"
import * as ViewModel from "effect-cqrs/ViewModel"
import { version as appVersion } from "../../../package.json"
import { makeRecordingTracer } from "../../devtools/tracing.ts"
import {
  type AggregateMetrics,
  type CommandTiming,
  devstore,
  type Origin,
  type Outcome,
  type Stage,
  viewModelNames,
  type ViewName
} from "./devstore.ts"
import { Account, type AccountEvent, AccountNotOpen } from "./domain/account.ts"
import { allViews } from "./domain/views.ts"

class Bank extends Context.Service<Bank, AggregateService.AggregateService<typeof Account>>()("bank-ui/Bank") {}

export interface CommandInput {
  readonly aggregateId: string
  readonly type: string
  readonly payload: unknown
}

const toOutcome = (result: AggregateService.CommandResult<AccountEvent>): Outcome => ({
  ok: true,
  version: result.version,
  position: result.position,
  events: result.events.map((event) => event._tag)
})

/** Fields of a tagged error, minus the tag, as a short line. */
const describeError = (error: unknown): Outcome => {
  if (typeof error !== "object" || error === null) return { ok: false, tag: "Error", message: String(error) }
  const { _tag, message, ...fields } = error as { _tag?: string; message?: string } & Record<string, unknown>
  const details = Object.entries(fields)
    .filter(([key]) => key !== "stack" && key !== "name" && key !== "cause")
    .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
    .join(", ")
  return { ok: false, tag: _tag ?? "Error", message: (typeof message === "string" && message !== "" ? message : details) || "" }
}

type Dispatched = ReturnType<Bank["Service"]["dispatch"]>

class Bus extends Context.Service<Bus, {
  /** Runs a command through the bus, failing as `dispatch` does. */
  run(input: CommandInput, origin: Origin): Effect.Effect<Effect.Success<Dispatched>, Effect.Error<Dispatched>>
  /** Runs a command through the bus and reports how it went. */
  submit(input: CommandInput, origin: Origin): Effect.Effect<Outcome>
}>()("bank-ui/Bus") {}

// Open while the bus runs. Pausing closes it, and "step" releases whoever waits at
// it without opening it: one command at a time goes through the bus, so that is
// one stage.
const gate = Latch.makeUnsafe(!devstore.get().settings.paused)

const setPaused = (paused: boolean) => {
  if (paused) gate.closeUnsafe()
  else gate.openUnsafe()
  devstore.settings({ paused })
}

const pause = (origin: Origin) =>
  Effect.suspend(() => {
    const millis = origin === "seed" ? 0 : devstore.get().settings.commandDelay
    return millis <= 0 ? Effect.yieldNow : Effect.sleep(Duration.millis(millis))
  })

const BusLayer = Layer.effect(
  Bus,
  Effect.gen(function*() {
    const bank = yield* Bank
    // One command at a time, so the pipeline reads like a queue.
    const lane = yield* Semaphore.make(1)

    const run = (input: CommandInput, origin: Origin) =>
      Effect.gen(function*() {
        const id = devstore.addCommand({ origin, ...input })
        return yield* lane.withPermit(Effect.gen(function*() {
          // Each stage is a span, so the trace shows where the bus held the command.
          const advance = (stage: Stage) =>
            Effect.gen(function*() {
              if (origin !== "seed") yield* gate.await
              devstore.stage(id, stage)
              yield* pause(origin)
            }).pipe(Effect.withSpan(`bus.${stage}`))
          yield* advance("validating")
          yield* advance("loading")
          yield* advance("deciding")
          yield* advance("appending")
          // #region dispatch
          // `dispatch` is the real entry point: it validates the JSON envelope and
          // payload, rehydrates the aggregate, runs the handler and appends.
          return yield* bank.dispatch(input).pipe(
            // Recorded on the events beside the app version the service was built with.
            Metadata.withMetadata({ origin }),
            Effect.tap((result) => Effect.sync(() => devstore.finish(id, toOutcome(result)))),
            Effect.tapError((error) => Effect.sync(() => devstore.finish(id, describeError(error)))),
            Effect.tapDefect((defect) => Effect.sync(() => devstore.finish(id, describeError(defect))))
          )
          // #endregion
        }))
      }).pipe(
        Effect.withSpan(`bus.${input.type}`, { attributes: { origin, "aggregate.id": input.aggregateId } })
      )

    const submit = (input: CommandInput, origin: Origin) =>
      run(input, origin).pipe(
        Effect.map(toOutcome),
        Effect.catch((error) => Effect.succeed(describeError(error))),
        Effect.catchDefect((defect) => Effect.succeed(describeError(defect)))
      )

    return Bus.of({ run, submit })
  })
)

/** Mirrors every stored event to the devtools, decoded with the aggregate's schema. */
const Bridge = Layer.effectDiscard(Effect.gen(function*() {
  const store = yield* EventStore.EventStore
  const decode = AggregateService.makeEventDecoder(Account)
  yield* store.subscribe().pipe(
    Stream.runForEach((stored) =>
      decode(stored).pipe(
        Effect.map((event) =>
          devstore.addEvent({
            position: stored.position,
            aggregateId: stored.aggregateId,
            version: stored.version,
            type: event._tag,
            timestamp: DateTime.toEpochMillis(stored.timestamp),
            payload: stored.payload,
            event,
            stored
          })
        )
      )
    ),
    Effect.catchCause((cause) => Effect.logError("bridge stopped", cause)),
    Effect.forkScoped
  )
}))

// #region saga
/**
 * A transfer touches two aggregates, so no single command can do it. For each
 * `TransferSent` the trigger starts a workflow that credits the other account,
 * and refunds the sender when that is refused because the account isn't open.
 * Each command is an activity whose outcome the engine keeps, and starting the
 * same transfer again joins the run already under way.
 */
const Transfer = Saga.workflow("Transfer", {
  payload: { transferId: Schema.String, from: Schema.String, to: Schema.String, amount: Schema.Int },
  idempotencyKey: ({ transferId }) => transferId
})

const TransferWorkflow = Saga.toLayer(Transfer, ({ amount, from, to, transferId }) =>
  Effect.gen(function*() {
    const bus = yield* Bus
    yield* Saga.command("credit", {
      error: AccountNotOpen,
      execute: bus.run({ aggregateId: to, type: "receiveTransfer", payload: { transferId, from, amount } }, "saga")
    }).pipe(
      Effect.catchTag("AccountNotOpen", ({ _tag }) =>
        Saga.command("refund", {
          execute: bus.run({
            aggregateId: from,
            type: "refundTransfer",
            payload: { transferId, to, amount, reason: _tag }
          }, "saga")
        }))
    )
    // Both commands record `TransferSent` as their cause, in the correlation it started.
  }))

const Transfers = Saga.make({
  name: "transfers",
  aggregates: [Account],
  handlers: {
    TransferSent: ({ amount, to, transferId }, stored) =>
      Saga.start(Transfer, { transferId, from: stored.aggregateId, to, amount })
  }
})

const TransferSaga = Saga.layer([Transfers]).pipe(
  Layer.provideMerge(TransferWorkflow),
  Layer.provide(WorkflowEngine.layerMemory)
)
// #endregion

/** Polls `ViewModels.status` so the devtools show each checkpoint and stall. */
const StatusPoll = Layer.effectDiscard(Effect.gen(function*() {
  const viewModels = yield* ViewModel.ViewModels
  const byName = new Map(Object.entries(viewModelNames).map(([view, name]) => [name, view as ViewName]))
  yield* viewModels.status.pipe(
    Effect.map((statuses) => {
      for (const status of statuses) {
        const view = byName.get(status.name)
        if (view === undefined) continue
        const error = status.lastError._tag === "Some" ? formatViewError(status.lastError.value) : undefined
        devstore.status(view, status.position, error)
      }
    }),
    Effect.repeat(Schedule.spaced("100 millis")),
    Effect.forkScoped
  )
}))

const { MetricNames } = AggregateService

/** Folds the series `AggregateService` records into what the metrics tab shows. */
const summarize = (snapshots: ReadonlyArray<Metric.Metric.Snapshot>): AggregateMetrics => {
  const ours = snapshots.filter((snapshot) => snapshot.attributes?.aggregate === Account.name)
  const of = (id: string, attributes: Record<string, string> = {}) =>
    ours.filter((snapshot) =>
      snapshot.id === id && Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value)
    )
  const count = (id: string, attributes?: Record<string, string>) =>
    of(id, attributes).reduce((sum, { state }) => sum + Number((state as { count: number | bigint }).count), 0)
  const histogram = (snapshot: Metric.Metric.Snapshot) => snapshot.state as Metric.HistogramState
  const [read] = of(MetricNames.eventsReplayed).map(histogram)
  const [size] = of(MetricNames.stateCacheSize)
  const commands = of(MetricNames.commandDuration).map((snapshot): CommandTiming => {
    const { count, max, sum } = histogram(snapshot)
    return {
      command: String(snapshot.attributes?.command),
      outcome: String(snapshot.attributes?.outcome),
      count,
      totalMs: sum,
      maxMs: max
    }
  })
  return {
    loads: {
      memory: count(MetricNames.stateLoads, { source: "memory" }),
      snapshot: count(MetricNames.stateLoads, { source: "snapshot" }),
      replay: count(MetricNames.stateLoads, { source: "replay" })
    },
    eventsRead: read?.sum ?? 0,
    maxEventsRead: read !== undefined && read.count > 0 ? read.max : 0,
    cacheSize: Number((size?.state as { value: number } | undefined)?.value ?? 0),
    evictions: count(MetricNames.stateCacheEvictions),
    snapshotsSaved: count(MetricNames.snapshotSaves, { result: "success" }),
    snapshotsFailed: count(MetricNames.snapshotSaves, { result: "failure" }),
    snapshotReadFailures: count(MetricNames.snapshotReadFailures),
    conflicts: count(MetricNames.concurrencyConflicts),
    commands: commands.sort((a, b) => a.command.localeCompare(b.command) || b.count - a.count)
  }
}

/** Polls `Metric.snapshot` so the devtools show what the account service records. */
const MetricsPoll = Layer.effectDiscard(
  Metric.snapshot.pipe(
    Effect.map((snapshots) => devstore.metrics(summarize(snapshots))),
    Effect.repeat(Schedule.spaced("250 millis")),
    Effect.forkScoped
  )
)

const formatViewError = (error: ViewModel.ViewModelError): string => {
  const cause = error.cause as { _tag?: string; reason?: string } | undefined
  return `${cause?._tag ?? "Error"} at #${error.position}${cause?.reason ? `: ${cause.reason}` : ""}`
}

const owners = ["Alice", "Bob", "Carol", "Dave", "Erin", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
export const palette = ["#7c5cff", "#ff5c8a", "#22d3a6", "#ffb020", "#38bdf8", "#f472b6", "#a3e635", "#fb7185"]

const pick = <A>(items: ReadonlyArray<A>): A => items[Math.floor(Math.random() * items.length)]!
const amount = (max: number) => Math.max(5, Math.round((Math.random() * max) / 5) * 5)

/** Random traffic, a few commands of which the aggregate will refuse. */
const nextRandomCommand = (): CommandInput | undefined => {
  const accounts = Object.values(devstore.get().views.accounts.data)
  const open = accounts.filter((account) => account.status === "open")
  const roll = Math.random()
  if (open.length < 2 || (roll < 0.06 && accounts.length < owners.length)) {
    const owner = owners.find((name) => !accounts.some((account) => account.owner === name))
    if (owner === undefined) return undefined
    return { aggregateId: owner.toLowerCase(), type: "openAccount", payload: { owner, color: pick(palette) } }
  }
  const account = pick(open)
  if (roll < 0.4) return { aggregateId: account.id, type: "deposit", payload: { amount: amount(200) } }
  if (roll < 0.65) return { aggregateId: account.id, type: "withdraw", payload: { amount: amount(account.balance + 60) } }
  const target = pick(accounts.filter((other) => other.id !== account.id))
  return {
    aggregateId: account.id,
    type: "sendTransfer",
    payload: { transferId: newTransferId(), to: target.id, amount: amount(account.balance + 40) }
  }
}

let transferCounter = 0
export const newTransferId = () => `tx-${(++transferCounter).toString().padStart(3, "0")}`

const Autopilot = Layer.effectDiscard(Effect.gen(function*() {
  const bus = yield* Bus
  yield* Effect.gen(function*() {
    const { autopilot, autopilotEvery } = devstore.get().settings
    yield* Effect.sleep(Duration.millis(autopilot ? autopilotEvery : 250))
    if (!devstore.get().settings.autopilot) return
    const command = nextRandomCommand()
    if (command !== undefined) yield* bus.submit(command, "autopilot")
  }).pipe(Effect.forever, Effect.forkScoped)
}))

// #region wiring
const Storage = Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, KeyValueStore.layerMemory)

const AccountState = Schema.Struct({
  status: Schema.Literals(["none", "open", "closed"]),
  owner: Schema.String,
  color: Schema.String,
  balance: Schema.Number,
  received: Schema.Array(Schema.String)
})

// Only four accounts stay in memory, so the metrics tab shows loads from snapshots too.
const BankService = Layer.effect(
  Bank,
  AggregateService.make(Account, { stateCacheSize: 4, snapshots: { every: 5, schema: AccountState } })
)

const Core = Layer.merge(BankService, ViewModel.layer(allViews)).pipe(Layer.provideMerge(Storage))

const WithBus = BusLayer.pipe(Layer.provideMerge(Core))

const Main = Layer.mergeAll(Bridge, TransferSaga, StatusPoll, MetricsPoll, Autopilot).pipe(
  Layer.provideMerge(WithBus),
  // Every command records the version of the software that ran it.
  Layer.provide(Metadata.layer({ appVersion })),
  // A registry of its own, so a reset counts from zero.
  Layer.provideMerge(Layer.sync(Metric.MetricRegistry, () => new Map())),
  // Every span reports to the devtools' trace tab as it starts, changes and ends.
  Layer.provideMerge(Layer.succeed(Tracer.Tracer, makeRecordingTracer(devstore.span)))
)
// #endregion

const seed: ReadonlyArray<CommandInput> = [
  { aggregateId: "alice", type: "openAccount", payload: { owner: "Alice", color: palette[0] } },
  { aggregateId: "bob", type: "openAccount", payload: { owner: "Bob", color: palette[1] } },
  { aggregateId: "carol", type: "openAccount", payload: { owner: "Carol", color: palette[2] } },
  { aggregateId: "alice", type: "deposit", payload: { amount: 250 } },
  { aggregateId: "bob", type: "deposit", payload: { amount: 120 } },
  { aggregateId: "carol", type: "deposit", payload: { amount: 80 } },
  { aggregateId: "alice", type: "withdraw", payload: { amount: 40 } }
]

let runtime = ManagedRuntime.make(Main)

const start = () => runtime.runPromise(Effect.gen(function*() {
  const bus = yield* Bus
  for (const command of seed) yield* bus.submit(command, "seed")
}))

void start()

export const engine = {
  send: (input: CommandInput, origin: Origin = "app"): Promise<Outcome> =>
    runtime.runPromise(Bus.use((bus) => bus.submit(input, origin))),

  /**
   * Disposes the view models and rebuilds them. With `upTo`, only events up to
   * that position are replayed: the replay API takes a time window, so the
   * window ends just after that event's timestamp.
   */
  replay: async (views: ReadonlyArray<ViewName>, upTo?: number) => {
    const { events } = devstore.get()
    const last = upTo === undefined ? undefined : events.find((event) => event.position === upTo)
    const finishTime = last === undefined ? undefined : DateTime.makeUnsafe(last.timestamp + 1)
    for (const view of views) devstore.replaying(view, true)
    try {
      const result = await runtime.runPromise(Effect.gen(function*() {
        const viewModels = yield* ViewModel.ViewModels
        return yield* Effect.forEach(views, (view) =>
          viewModels.replay({ viewModels: [viewModelNames[view]], finishTime }).pipe(
            Effect.map(({ eventCount }) => [view, eventCount] as const)
          ), { concurrency: "unbounded" })
      }))
      // Events in the same millisecond as `upTo` fall inside the window too.
      const reached = last === undefined
        ? undefined
        : Math.max(...events.filter((event) => event.timestamp <= last.timestamp).map((event) => event.position))
      for (const [view, eventCount] of result) devstore.replayed(view, { upTo: reached, eventCount })
    } finally {
      for (const view of views) devstore.replaying(view, false)
    }
  },

  setPaused,

  /** Lets the command waiting at the gate through one stage. */
  step: () => void Effect.runSync(gate.release),

  /** Throws everything away and starts a fresh store. */
  reset: async () => {
    const previous = runtime
    devstore.settings({ autopilot: false })
    setPaused(false)
    await previous.dispose()
    devstore.reset()
    transferCounter = 0
    runtime = ManagedRuntime.make(Main)
    await start()
  }
}
