/**
 * The tutorial's shop: a saga holds the stock for each order and gives the
 * customer 15 seconds to pay. It uses commands, events, a view model, a saga with
 * a timeout, metadata, metrics and an event version. Run it with `pnpm tutorial`.
 */
import { Context, Duration, Effect, Layer, Metric, Option, Ref, Schema, Stream } from "effect"
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
import { DurableClock, DurableDeferred, WorkflowEngine } from "effect/workflow"

// #region events
const Quantity = Schema.Int.check(Schema.isGreaterThan(0))

export const StockEvent = Schema.TaggedUnion({
  StockAdded: { quantity: Quantity },
  StockReserved: { orderId: Schema.String, quantity: Quantity },
  StockReleased: { orderId: Schema.String, quantity: Quantity }
})

export const OrderEvent = Schema.TaggedUnion({
  OrderPlaced: { sku: Schema.String, quantity: Quantity },
  // The saga asks for the payment. The token names the saga's run that waits for it.
  PaymentRequested: { token: DurableDeferred.Token },
  OrderPaid: { token: DurableDeferred.Token },
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

export class OrderNotPayable extends Schema.TaggedError<OrderNotPayable>()("OrderNotPayable", {
  orderId: Schema.String,
  status: Schema.String
}) {}

export class OrderAlreadyPaid extends Schema.TaggedError<OrderAlreadyPaid>()("OrderAlreadyPaid", {
  orderId: Schema.String
}) {}
// #endregion

// #region stock
export interface StockState {
  readonly available: number
  /** What each order holds, by order id. */
  readonly reserved: Readonly<Record<string, number>>
}

export const Stock = Aggregate.make<StockState>()({
  name: "stock",
  events: StockEvent,
  initial: { available: 0, reserved: {} },
  projection: {
    StockAdded: (state, { quantity }) => ({ ...state, available: state.available + quantity }),
    StockReserved: (state, { orderId, quantity }) => ({
      available: state.available - quantity,
      reserved: { ...state.reserved, [orderId]: quantity }
    }),
    StockReleased: (state, { orderId, quantity }) => {
      const { [orderId]: _, ...reserved } = state.reserved
      return { available: state.available + quantity, reserved }
    }
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
      if (state.reserved[orderId] !== undefined) return []
      if (state.available < quantity) return yield* new OutOfStock({ sku: aggregateId, available: state.available })
      return StockEvent.cases.StockReserved.make({ orderId, quantity })
    })
  })
  .command("release", {
    payload: Schema.Struct({ orderId: Schema.String }),
    handler: (state, { orderId }) => {
      const quantity = state.reserved[orderId]
      return Effect.succeed(quantity === undefined ? [] : StockEvent.cases.StockReleased.make({ orderId, quantity }))
    }
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
  readonly status: "none" | "placed" | "awaiting-payment" | "paid" | "confirmed" | "rejected"
  readonly sku: string
  readonly quantity: number
  readonly payment: DurableDeferred.Token | null
}

export const Order = Aggregate.make<OrderState>()({
  name: "order",
  events: OrderEvent,
  initial: { status: "none", sku: "", quantity: 0, payment: null },
  projection: {
    OrderPlaced: (_, { quantity, sku }) => ({ status: "placed", sku, quantity, payment: null }),
    PaymentRequested: (state, { token }) => ({ ...state, status: "awaiting-payment", payment: token }),
    OrderPaid: (state) => ({ ...state, status: "paid" }),
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
  .command("requestPayment", {
    payload: Schema.Struct({ token: DurableDeferred.Token }),
    handler: (state, { token }) =>
      Effect.succeed(state.status === "placed" ? OrderEvent.cases.PaymentRequested.make({ token }) : [])
  })
  // The customer pays, and only while the saga waits for it.
  .command("pay", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
      if (state.status === "paid" || state.status === "confirmed") return []
      if (state.status !== "awaiting-payment" || state.payment === null) {
        return yield* new OrderNotPayable({ orderId: aggregateId, status: state.status })
      }
      return OrderEvent.cases.OrderPaid.make({ token: state.payment })
    })
  })
  .command("confirm", {
    payload: Schema.Struct({}),
    handler: (state) => Effect.succeed(state.status === "paid" ? OrderEvent.cases.OrderConfirmed.make({}) : [])
  })
  .command("reject", {
    payload: Schema.Struct({ reason: Schema.String }),
    handler: Effect.fnUntraced(function*(state, { reason }, { aggregateId }) {
      if (state.status === "paid") return yield* new OrderAlreadyPaid({ orderId: aggregateId })
      if (state.status !== "placed" && state.status !== "awaiting-payment") return []
      return OrderEvent.cases.OrderRejected.make({ reason })
    })
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
    PaymentRequested: (_, { aggregateId }) => setStatus(aggregateId, "awaiting payment"),
    OrderPaid: (_, { aggregateId }) => setStatus(aggregateId, "paid"),
    OrderConfirmed: (_, { aggregateId }) => setStatus(aggregateId, "confirmed"),
    OrderRejected: ({ reason }, { aggregateId }) => setStatus(aggregateId, `rejected: ${reason}`)
  }
})
// #endregion

// #region saga
/** How long a customer has to pay once the stock is held. */
export const paymentTimeout = Duration.seconds(15)

// The customer's payment, which a run of the saga waits for.
export const Payment = DurableDeferred.make("Payment")

// One run per order: hold the stock, ask for the payment and wait for it, then
// confirm the order, or reject it and put the stock back.
export const Fulfil = Saga.workflow("Fulfil", {
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Int },
  idempotencyKey: ({ orderId }) => orderId
})

export const FulfilWorkflow = Saga.toLayer(Fulfil, ({ orderId, quantity, sku }) =>
  Effect.gen(function*() {
    const orders = yield* Orders
    const stocks = yield* Stocks

    // 1. Hold the stock, or turn the order down.
    const reserved = yield* Saga.command("reserve", {
      error: OutOfStock,
      execute: stocks.commands.reserve(sku, { orderId, quantity })
    }).pipe(
      Effect.as(true),
      Effect.catchTag("OutOfStock", ({ available }) =>
        Saga.command("reject", {
          execute: orders.commands.reject(orderId, { reason: `only ${available} left` })
        }).pipe(Effect.as(false)))
    )
    if (!reserved) return

    // 2. Ask for the payment, then wait for whichever comes first: the payment or the deadline.
    const token = yield* DurableDeferred.token(Payment)
    yield* Saga.command("request-payment", { execute: orders.commands.requestPayment(orderId, { token }) })
    const paid = yield* DurableDeferred.raceAll({
      name: "payment",
      success: Schema.Boolean,
      error: Schema.Never,
      effects: [
        DurableDeferred.await(Payment).pipe(Effect.as(true)),
        DurableClock.sleep({ name: "payment-timeout", duration: paymentTimeout }).pipe(Effect.as(false))
      ]
    })

    // 3. Too late: reject the order and give the stock back. The order refuses if the
    //    customer paid just as the time ran out, and the saga confirms it instead.
    if (!paid) {
      const expired = yield* Saga.command("expire", {
        error: OrderAlreadyPaid,
        execute: orders.commands.reject(orderId, { reason: "not paid in time" })
      }).pipe(
        Effect.as(true),
        Effect.catchTag("OrderAlreadyPaid", () => Effect.succeed(false))
      )
      if (expired) {
        yield* Saga.command("release", { execute: stocks.commands.release(sku, { orderId }) })
        return
      }
    }
    yield* Saga.command("confirm", { execute: orders.commands.confirm(orderId, {}) })
  }))

// The trigger: starts a run for every order placed, and wakes it when the order is paid.
export const Fulfilment = Saga.make({
  name: "fulfilment",
  aggregates: [Order],
  handlers: {
    OrderPlaced: ({ quantity, sku }, { aggregateId }) => Saga.start(Fulfil, { orderId: aggregateId, sku, quantity }),
    OrderPaid: ({ token }) => DurableDeferred.succeed(Payment, { token, value: undefined })
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

/** Waits until one of these events is stored for an order, and the view model has seen it. */
export const reached = (orderId: string, ...eventTypes: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const viewModels = yield* ViewModel.ViewModels
    const done = yield* store.subscribe({ aggregateIds: [orderId], eventTypes }).pipe(Stream.runHead)
    if (Option.isSome(done)) yield* viewModels.caughtUp(done.value.position)
  })

/** Waits until the saga has confirmed or rejected an order. */
export const settled = (orderId: string) => reached(orderId, "OrderConfirmed", "OrderRejected")

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
  // The saga holds the stock and asks for the payment: the customer has 15 seconds to pay.
  yield* reached("order-1", "PaymentRequested", "OrderRejected")
  yield* orders.commands.pay("order-1", {})
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
  const stock = yield* stocks.readModel("apple")
  yield* Effect.log("apple stock", stock)

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

  return { orders: Object.fromEntries(list), stock, legacy, rejected: rejected._tag, history, timings }
})
