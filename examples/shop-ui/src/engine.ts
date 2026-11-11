/**
 * Runs the tutorial's shop in the browser: its aggregates, its saga and its
 * trigger, on the in-memory event store and workflow engine. A tracer of its own
 * hands every span to the devtools, which also poll the engine, the view models'
 * checkpoints and the metrics.
 */
import {
  Context,
  DateTime,
  Duration,
  Effect,
  Exit,
  Layer,
  ManagedRuntime,
  Metric,
  Option,
  Predicate,
  Schedule,
  Stream,
  Tracer
} from "effect"
import type * as Aggregate from "effect-cqrs/Aggregate"
import * as AggregateService from "effect-cqrs/AggregateService"
import * as Checkpoints from "effect-cqrs/Checkpoints"
import * as CommandGateway from "effect-cqrs/CommandGateway"
import * as EventStore from "effect-cqrs/EventStore"
import * as Metadata from "effect-cqrs/Metadata"
import * as ViewModel from "effect-cqrs/ViewModel"
import { WorkflowEngine } from "effect/workflow"
import { version as appVersion } from "../../../package.json"
import { makeRecordingTracer } from "../../devtools/tracing.ts"
import {
  Fulfil,
  Fulfilment,
  FulfilWorkflow,
  Order,
  Orders,
  paymentTimeout,
  Stock,
  Stocks
} from "../../tutorial/shop.ts"
import { type CommandTiming, devstore, type EngineStatus, type Metrics } from "./devstore.ts"

export const catalog = [
  { sku: "apple", label: "Apples", color: "#fb7185" },
  { sku: "pear", label: "Pears", color: "#a3e635" },
  { sku: "plum", label: "Plums", color: "#a78bfa" }
] as const

export const customers = ["alice", "bob", "carol"] as const

// Tracing: every span reports to the devtools as it starts, changes and ends.
const tracer = makeRecordingTracer(devstore.span)

// ---------------------------------------------------------------------------
// The command side: the tutorial's aggregates, slowed down so the saga can be watched.

let seeding = false

// A span of its own, so the trace shows where the time went.
const latency = Effect.suspend(() => {
  const { latency } = devstore.get().settings
  return seeding || latency <= 0
    ? Effect.void
    : Effect.sleep(Duration.millis(latency)).pipe(Effect.withSpan("demo.latency", { attributes: { ms: latency } }))
})

const slow = <A extends Aggregate.Any>(
  service: AggregateService.AggregateService<A>
): AggregateService.AggregateService<A> => ({
  ...service,
  commands: Object.fromEntries(
    Object.entries(service.commands).map(([name, run]) => [
      name,
      (aggregateId: string, payload: unknown) =>
        Effect.andThen(
          latency,
          (run as (id: string, payload: unknown) => Effect.Effect<unknown, unknown>)(aggregateId, payload)
        )
    ])
  ) as AggregateService.AggregateService<A>["commands"]
})

const OrdersLive = Layer.effect(Orders, Effect.map(AggregateService.make(Order), slow))
const StocksLive = Layer.effect(Stocks, Effect.map(AggregateService.make(Stock), slow))

// The screen sends its commands through a gateway, which finds the aggregate each
// one is for. Its interceptor records the current customer on what they do to
// orders. Restocking is the staff's job, so stock commands record no customer.
const asCustomer: CommandGateway.Interceptor = (command, next) =>
  command.aggregateName === Order.name
    ? Effect.suspend(() => Metadata.withMetadata(next, { userId: devstore.get().settings.customer, channel: "web" }))
    : next

// Built on the services above, so commands from the screen are slowed down too.
// The saga keeps using the services directly: it acts for no customer.
const makeShop = Effect.gen(function*() {
  return yield* CommandGateway.make([yield* Orders, yield* Stocks], { interceptors: [asCustomer] })
})

class Shop extends Context.Service<Shop, Effect.Success<typeof makeShop>>()("shop-ui/Shop") {}

// ---------------------------------------------------------------------------
// The query side: two view models for the shop's screen. The saga's trigger runs
// next to them, so the devtools see its checkpoint too.

const OrdersView = ViewModel.make({
  name: "orders",
  aggregates: [Order],
  dispose: Effect.sync(devstore.clearOrders),
  handlers: {
    OrderPlaced: ({ quantity, sku }, meta) =>
      Effect.gen(function*() {
        // The id the engine gives this order's saga, from the workflow's idempotency key.
        const executionId = yield* Fulfil.executionId({
          orderId: meta.aggregateId,
          sku,
          quantity,
          causation: { correlationId: "", causationId: "" }
        })
        const { appVersion, userId } = meta.metadata
        devstore.order(meta.aggregateId, () => ({
          id: meta.aggregateId,
          sku,
          quantity,
          status: "placed",
          legacy: false,
          position: meta.position,
          executionId,
          ...(Predicate.isString(userId) ? { userId } : {}),
          ...(Predicate.isString(appVersion) ? { appVersion } : {})
        }))
      }),
    // The saga starts its clock right after it records the request, so the deadline is close enough.
    PaymentRequested: (_, meta) =>
      Effect.sync(() =>
        devstore.order(meta.aggregateId, (order) =>
          order && {
            ...order,
            status: "awaiting-payment",
            deadline: DateTime.toEpochMillis(meta.timestamp) + Duration.toMillis(paymentTimeout),
            position: meta.position
          })
      ),
    OrderPaid: (_, meta) =>
      Effect.sync(() =>
        devstore.order(meta.aggregateId, (order) => order && { ...order, status: "paid", position: meta.position })
      ),
    OrderConfirmed: (_, meta) =>
      Effect.sync(() =>
        devstore.order(meta.aggregateId, (order) => order && { ...order, status: "confirmed", position: meta.position })
      ),
    OrderRejected: ({ reason }, meta) =>
      Effect.sync(() =>
        devstore.order(
          meta.aggregateId,
          (order) => order && { ...order, status: "rejected", reason, position: meta.position }
        )
      )
  }
})

const InventoryView = ViewModel.make({
  name: "inventory",
  aggregates: [Stock],
  dispose: Effect.sync(devstore.clearStock),
  handlers: {
    StockAdded: ({ quantity }, meta) =>
      Effect.sync(() => devstore.stock(meta.aggregateId, (row) => ({ ...row, available: row.available + quantity }))),
    StockReserved: ({ quantity }, meta) =>
      Effect.sync(() =>
        devstore.stock(meta.aggregateId, (row) => ({
          ...row,
          available: row.available - quantity,
          reserved: row.reserved + quantity
        }))
      ),
    StockReleased: ({ quantity }, meta) =>
      Effect.sync(() =>
        devstore.stock(meta.aggregateId, (row) => ({
          ...row,
          available: row.available + quantity,
          reserved: row.reserved - quantity
        }))
      )
  }
})

// ---------------------------------------------------------------------------
// The devtools' feeds. They run with the tracer off, so polling doesn't fill the trace.

/** Mirrors every stored event, as stored and as its aggregate decodes it. */
const Bridge = Layer.effectDiscard(Effect.gen(function*() {
  const store = yield* EventStore.EventStore
  const decoders: Record<string, (stored: EventStore.StoredEvent) => Effect.Effect<unknown, unknown>> = {
    [Order.name]: AggregateService.makeEventDecoder(Order),
    [Stock.name]: AggregateService.makeEventDecoder(Stock)
  }
  yield* store.subscribe().pipe(
    Stream.runForEach((stored) =>
      (decoders[stored.aggregateName]?.(stored) ?? Effect.succeed(null)).pipe(
        Effect.orElseSucceed(() => null),
        Effect.map((decoded) =>
          devstore.addEvent({
            position: stored.position,
            aggregateName: stored.aggregateName,
            aggregateId: stored.aggregateId,
            version: stored.version,
            type: stored.type,
            schemaVersion: stored.schemaVersion,
            payload: stored.payload,
            decoded,
            metadata: stored.metadata,
            correlationId: stored.correlationId,
            causationId: stored.causationId,
            timestamp: stored.timestamp.epochMilliseconds
          })
        )
      )
    ),
    Effect.withTracerEnabled(false),
    Effect.forkScoped
  )
}))

const poll = <E, R>(effect: Effect.Effect<void, E, R>, every: Duration.Input) =>
  Layer.effectDiscard(
    effect.pipe(
      Effect.ignore,
      Effect.repeat(Schedule.spaced(every)),
      Effect.withTracerEnabled(false),
      Effect.forkScoped
    )
  )

const CheckpointPoll = poll(
  Effect.gen(function*() {
    const viewModels = yield* ViewModel.ViewModels
    const statuses = yield* viewModels.status
    devstore.checkpoints(statuses.map((status) => ({
      name: status.name,
      position: status.position,
      ...(Option.isSome(status.lastError) ? { error: String(status.lastError.value.cause) } : {})
    })))
  }),
  "150 millis"
)

/** Asks the engine how each saga's execution stands. */
const EnginePoll = poll(
  Effect.gen(function*() {
    const { orders, spans } = devstore.get()
    const started = new Set(
      spans.flatMap((span) => span.name === "Fulfil.execute" ? [span.attributes.executionId] : [])
    )
    const statuses: Record<string, EngineStatus> = {}
    for (const { executionId } of Object.values(orders)) {
      const result = yield* Fulfil.poll(executionId)
      if (Option.isNone(result)) {
        if (started.has(executionId)) statuses[executionId] = "running"
      } else if (result.value._tag === "Suspended") {
        statuses[executionId] = "suspended"
      } else {
        statuses[executionId] = Exit.isSuccess(result.value.exit) ? "succeeded" : "failed"
      }
    }
    devstore.engine(statuses)
  }),
  "150 millis"
)

const { MetricNames } = AggregateService

/** Folds the series the aggregate services record into what the metrics tab shows. */
const summarize = (snapshots: ReadonlyArray<Metric.Metric.Snapshot>): Metrics => {
  const of = (id: string) => snapshots.filter((snapshot) => snapshot.id === id)
  const count = (id: string, source?: string) =>
    of(id)
      .filter((snapshot) => source === undefined || snapshot.attributes?.source === source)
      .reduce((sum, snapshot) => sum + (snapshot.type === "Counter" ? Number(snapshot.state.count) : 0), 0)
  const commands = of(MetricNames.commandDuration).flatMap((snapshot): Array<CommandTiming> =>
    snapshot.type === "Histogram"
      ? [{
        aggregate: String(snapshot.attributes?.aggregate),
        command: String(snapshot.attributes?.command),
        outcome: String(snapshot.attributes?.outcome),
        count: snapshot.state.count,
        totalMs: snapshot.state.sum,
        maxMs: snapshot.state.max
      }]
      : []
  )
  return {
    commands: commands.sort((a, b) =>
      a.aggregate.localeCompare(b.aggregate) || a.command.localeCompare(b.command) || b.count - a.count
    ),
    loads: {
      memory: count(MetricNames.stateLoads, "memory"),
      snapshot: count(MetricNames.stateLoads, "snapshot"),
      replay: count(MetricNames.stateLoads, "replay")
    },
    conflicts: count(MetricNames.concurrencyConflicts),
    cacheSize: of(MetricNames.stateCacheSize).reduce(
      (sum, snapshot) => sum + (snapshot.type === "Gauge" ? Number(snapshot.state.value) : 0),
      0
    )
  }
}

const MetricsPoll = poll(
  Effect.map(Metric.snapshot, (snapshots) => devstore.metrics(summarize(snapshots))),
  "250 millis"
)

// ---------------------------------------------------------------------------
// Wiring, as in the tutorial, plus the devtools' feeds.

const Storage = Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory)

const Commands = Layer.effect(Shop, makeShop).pipe(Layer.provideMerge(Layer.merge(OrdersLive, StocksLive)))

// The saga's workflow, registered with an engine that keeps executions in memory.
const Workflows = FulfilWorkflow.pipe(Layer.provideMerge(WorkflowEngine.layerMemory))

const Main = Layer.mergeAll(Bridge, CheckpointPoll, EnginePoll, MetricsPoll).pipe(
  Layer.provideMerge(ViewModel.layer([OrdersView, InventoryView, Fulfilment])),
  Layer.provideMerge(Workflows),
  Layer.provideMerge(Commands),
  Layer.provideMerge(Storage),
  // Every command records the version of the software that ran it.
  Layer.provide(Metadata.layer({ appVersion })),
  // A registry of its own, so a reset counts from zero.
  Layer.provideMerge(Layer.sync(Metric.MetricRegistry, () => new Map())),
  Layer.provideMerge(Layer.succeed(Tracer.Tracer, tracer))
)

// ---------------------------------------------------------------------------
// What the screen can do.

export type Outcome = { readonly ok: true } | { readonly ok: false; readonly tag: string }

const outcome = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<Outcome, never, R> =>
  effect.pipe(
    Effect.as<Outcome>({ ok: true }),
    Effect.catch((error) =>
      Effect.succeed<Outcome>({
        ok: false,
        tag: Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag) ? error._tag : "Error"
      })
    )
  )

let runtime = ManagedRuntime.make(Main)
let nextOrder = 1
let nextLegacy = 1

const seed = Effect.gen(function*() {
  const stocks = yield* Stocks
  yield* stocks.commands.add("apple", { quantity: 5 })
  yield* stocks.commands.add("pear", { quantity: 2 })
  yield* stocks.commands.add("plum", { quantity: 8 })
}).pipe(Effect.withSpan("seed.stock"))

const start = async () => {
  seeding = true
  try {
    await runtime.runPromise(seed)
  } finally {
    seeding = false
  }
}

void start()

export const engine = {
  /** Places an order as the current customer, whom the gateway records in the command's metadata. */
  placeOrder: (sku: string, quantity: number): Promise<Outcome> => {
    const orderId = `order-${nextOrder++}`
    const { customer } = devstore.get().settings
    return runtime.runPromise(
      Shop.use((shop) =>
        shop.send({ aggregateName: "order", aggregateId: orderId, type: "place", payload: { sku, quantity } })
      ).pipe(
        Effect.withSpan("ui.placeOrder", { attributes: { orderId, sku, quantity, customer } }),
        outcome
      )
    )
  },

  /** Pays for an order the saga is waiting on. The trigger turns `OrderPaid` into the saga's signal. */
  pay: (orderId: string): Promise<Outcome> => {
    const { customer } = devstore.get().settings
    return runtime.runPromise(
      Shop.use((shop) => shop.send({ aggregateName: "order", aggregateId: orderId, type: "pay", payload: {} })).pipe(
        Effect.withSpan("ui.pay", { attributes: { orderId, customer } }),
        outcome
      )
    )
  },

  restock: (sku: string, quantity: number): Promise<Outcome> =>
    runtime.runPromise(
      Shop.use((shop) => shop.send({ aggregateName: "stock", aggregateId: sku, type: "add", payload: { quantity } }))
        .pipe(
          Effect.withSpan("ui.restock", { attributes: { sku, quantity } }),
          outcome
        )
    ),

  /**
   * Appends an order the way the shop's first version stored it, before orders had
   * a quantity. The aggregate reads it in the current shape, and so does the saga.
   */
  importLegacy: (sku: string): Promise<Outcome> => {
    const orderId = `legacy-${nextLegacy++}`
    return runtime.runPromise(
      EventStore.EventStore.use((store) =>
        store.append({
          aggregateName: Order.name,
          aggregateId: orderId,
          expectedVersion: 0,
          events: [{ type: "OrderPlaced", schemaVersion: 1, payload: { sku } }],
          metadata: { importedFrom: "shop v0" }
        })
      ).pipe(Effect.withSpan("ui.importLegacy", { attributes: { orderId, sku } }), outcome)
    )
  },

  /** Throws everything away and starts a fresh store and engine. */
  reset: async () => {
    const previous = runtime
    await previous.dispose()
    devstore.reset()
    nextOrder = 1
    nextLegacy = 1
    runtime = ManagedRuntime.make(Main)
    await start()
  }
}
