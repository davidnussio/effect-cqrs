/**
 * The tutorial's shop: orders that reserve stock through a saga. It uses
 * commands, events, a view model, a saga, metadata, metrics and an event
 * version. Run it with `pnpm tutorial`.
 */
import { Context, Effect, Layer, Metric, Option, Ref, Schema, Stream } from "effect"
import { WorkflowEngine } from "effect/workflow"
import {
  Aggregate,
  AggregateService,
  Checkpoints,
  EventStore,
  EventVersions,
  Metadata,
  Saga,
  ViewModel
} from "effect-cqrs"

// #region events
const Quantity = Schema.Int.check(Schema.isGreaterThan(0))

export const StockEvent = Schema.TaggedUnion({
  StockAdded: { quantity: Quantity },
  StockReserved: { orderId: Schema.String, quantity: Quantity }
})

export const OrderEvent = Schema.TaggedUnion({
  OrderPlaced: { sku: Schema.String, quantity: Quantity },
  OrderConfirmed: {},
  OrderRejected: { reason: Schema.String }
})
// #endregion

// #region errors
export class OutOfStock extends Schema.TaggedError<OutOfStock>()("OutOfStock", {
  sku: Schema.String,
  available: Schema.Int
}) {}

export class OrderAlreadyPlaced extends Schema.TaggedError<OrderAlreadyPlaced>()("OrderAlreadyPlaced", {
  orderId: Schema.String
}) {}
// #endregion

// #region stock
export interface StockState {
  readonly available: number
  readonly reserved: ReadonlyArray<string>
}

export const Stock = Aggregate.make<StockState>()({
  name: "stock",
  events: StockEvent,
  initial: { available: 0, reserved: [] },
  projection: {
    StockAdded: (state, { quantity }) => ({ ...state, available: state.available + quantity }),
    StockReserved: (state, { orderId, quantity }) => ({
      available: state.available - quantity,
      reserved: [...state.reserved, orderId]
    })
  }
})
  .command("add", {
    payload: Schema.Struct({ quantity: Quantity }),
    handler: (_, { quantity }) => Effect.succeed(StockEvent.cases.StockAdded.make({ quantity }))
  })
  .command("reserve", {
    payload: Schema.Struct({ orderId: Schema.String, quantity: Quantity }),
    handler: Effect.fnUntraced(function*(state, { orderId, quantity }, { aggregateId }) {
      // A saga may run this twice: the second time records nothing.
      if (state.reserved.includes(orderId)) return []
      if (state.available < quantity) return yield* new OutOfStock({ sku: aggregateId, available: state.available })
      return StockEvent.cases.StockReserved.make({ orderId, quantity })
    })
  })
// #endregion

// #region versions
// Before quantities existed, an order was always for one item.
const OrderPlacedV1 = Schema.Struct({ sku: Schema.String })

const orderVersions = {
  OrderPlaced: EventVersions.from(OrderPlacedV1).next(
    OrderEvent.cases.OrderPlaced,
    (v1) => ({ ...v1, quantity: 1 })
  )
}
// #endregion

// #region order
export interface OrderState {
  readonly status: "none" | "placed" | "confirmed" | "rejected"
  readonly sku: string
  readonly quantity: number
}

export const Order = Aggregate.make<OrderState>()({
  name: "order",
  events: OrderEvent,
  initial: { status: "none", sku: "", quantity: 0 },
  projection: {
    OrderPlaced: (_, { quantity, sku }) => ({ status: "placed", sku, quantity }),
    OrderConfirmed: (state) => ({ ...state, status: "confirmed" }),
    OrderRejected: (state) => ({ ...state, status: "rejected" })
  },
  versions: orderVersions
})
  .command("place", {
    payload: Schema.Struct({ sku: Schema.String, quantity: Quantity }),
    handler: Effect.fnUntraced(function*(state, { quantity, sku }, { aggregateId }) {
      if (state.status !== "none") return yield* new OrderAlreadyPlaced({ orderId: aggregateId })
      return OrderEvent.cases.OrderPlaced.make({ sku, quantity })
    })
  })
  .command("confirm", {
    payload: Schema.Struct({}),
    handler: (state) => Effect.succeed(state.status === "placed" ? OrderEvent.cases.OrderConfirmed.make({}) : [])
  })
  .command("reject", {
    payload: Schema.Struct({ reason: Schema.String }),
    handler: (state, { reason }) =>
      Effect.succeed(state.status === "placed" ? OrderEvent.cases.OrderRejected.make({ reason }) : [])
  })
// #endregion

// #region services
export class Orders extends Context.Service<Orders, AggregateService.AggregateService<typeof Order>>()("shop/Orders") {
  static readonly layer = Layer.effect(Orders, AggregateService.make(Order))
}

export class Stocks extends Context.Service<Stocks, AggregateService.AggregateService<typeof Stock>>()("shop/Stocks") {
  static readonly layer = Layer.effect(Stocks, AggregateService.make(Stock))
}
// #endregion

// #region view-model
export class OrderList extends Context.Service<OrderList, Ref.Ref<ReadonlyMap<string, string>>>()("shop/OrderList") {
  static readonly layer = Layer.effect(OrderList, Ref.make<ReadonlyMap<string, string>>(new Map()))
}

const setStatus = (orderId: string, status: string) =>
  OrderList.use((list) => Ref.update(list, (orders) => new Map(orders).set(orderId, status)))

export const OrderListView = ViewModel.make({
  name: "order-list",
  aggregates: [Order],
  dispose: OrderList.use((list) => Ref.set(list, new Map())),
  handlers: {
    OrderPlaced: ({ quantity, sku }, { aggregateId }) => setStatus(aggregateId, `placed ${quantity} ${sku}`),
    OrderConfirmed: (_, { aggregateId }) => setStatus(aggregateId, "confirmed"),
    OrderRejected: ({ reason }, { aggregateId }) => setStatus(aggregateId, `rejected: ${reason}`)
  }
})
// #endregion

// #region saga
// One run per order: reserve the stock, then confirm the order or reject it.
export const Fulfil = Saga.workflow("Fulfil", {
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Int },
  idempotencyKey: ({ orderId }) => orderId
})

export const FulfilWorkflow = Saga.toLayer(Fulfil, ({ orderId, quantity, sku }) =>
  Effect.gen(function*() {
    const orders = yield* Orders
    const stocks = yield* Stocks
    yield* Saga.command("reserve", {
      error: OutOfStock,
      execute: stocks.commands.reserve(sku, { orderId, quantity })
    }).pipe(
      Effect.andThen(Saga.command("confirm", { execute: orders.commands.confirm(orderId, {}) })),
      Effect.catchTag("OutOfStock", ({ available }) =>
        Saga.command("reject", {
          execute: orders.commands.reject(orderId, { reason: `only ${available} left` })
        }))
    )
  }))

// The trigger: starts a run for every order placed.
export const Fulfilment = Saga.make({
  name: "fulfilment",
  aggregates: [Order],
  handlers: {
    OrderPlaced: ({ quantity, sku }, { aggregateId }) => Saga.start(Fulfil, { orderId: aggregateId, sku, quantity })
  }
})
// #endregion

// #region layers
const Storage = Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, OrderList.layer)

const Sagas = Saga.layer([Fulfilment]).pipe(
  Layer.provideMerge(FulfilWorkflow),
  Layer.provide(WorkflowEngine.layerMemory)
)

export const Shop = Layer.mergeAll(Sagas, ViewModel.layer([OrderListView])).pipe(
  Layer.provideMerge(Layer.merge(Orders.layer, Stocks.layer)),
  Layer.provideMerge(Storage),
  // Every command records the version of the software that ran it.
  Layer.provide(Metadata.layer({ appVersion: "1.0.0" }))
)
// #endregion

/** Waits until the saga has confirmed or rejected an order, and the view model has seen it. */
const settled = (orderId: string) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const viewModels = yield* ViewModel.ViewModels
    const done = yield* store.subscribe({
      aggregateIds: [orderId],
      eventTypes: ["OrderConfirmed", "OrderRejected"]
    }).pipe(Stream.runHead)
    if (Option.isSome(done)) yield* viewModels.caughtUp(done.value.position)
  })

export const program = Effect.gen(function*() {
  const orders = yield* Orders
  const stocks = yield* Stocks

  // #region legacy
  // An order the previous version of the shop stored, before quantities existed.
  yield* EventStore.EventStore.use((store) =>
    store.append({
      aggregateName: "order",
      aggregateId: "order-0",
      expectedVersion: 0,
      events: [{ type: "OrderPlaced", schemaVersion: 1, payload: { sku: "pear" } }]
    })
  )
  yield* settled("order-0")

  const legacy = yield* orders.readModel("order-0")
  yield* Effect.log("order-0 is read as", legacy)
  // #endregion

  // #region commands
  yield* stocks.commands.add("apple", { quantity: 5 })
  yield* orders.commands.place("order-1", { sku: "apple", quantity: 3 })
  yield* settled("order-1")

  const rejected = yield* Effect.flip(orders.commands.place("order-1", { sku: "apple", quantity: 1 }))
  yield* Effect.log("placing order-1 again fails with", rejected._tag)
  // #endregion

  // #region metadata
  yield* orders.commands.place("order-2", { sku: "apple", quantity: 3 }).pipe(
    Metadata.withMetadata({ userId: "bob", channel: "mobile" })
  )
  yield* settled("order-2")

  const history = yield* orders.history("order-2")
  yield* Effect.log("order-2 history", history.map(({ metadata, type }) => ({ type, metadata })))
  // #endregion

  const list = yield* OrderList.use(Ref.get)
  yield* Effect.log("order list", Object.fromEntries(list))
  yield* Effect.log("apple stock", yield* stocks.readModel("apple"))

  // #region metrics
  // How many `place` commands ran, and how long they took, by outcome.
  const timings = (yield* Metric.snapshot).flatMap((metric) =>
    metric.id === AggregateService.MetricNames.commandDuration &&
      metric.type === "Histogram" &&
      metric.attributes?.["aggregate"] === "order" &&
      metric.attributes["command"] === "place"
      ? [{ outcome: metric.attributes["outcome"], count: metric.state.count, totalMs: metric.state.sum }]
      : []
  )
  yield* Effect.log("order.place timings", timings)
  // #endregion

  return { orders: Object.fromEntries(list), legacy, rejected: rejected._tag, history, timings }
})
