/**
 * The shop: products with real stock, orders paid from the bank account, and a
 * queue per product for orders placed while it is out of stock.
 *
 * An order goes through a saga, because it touches three aggregates. The saga
 * takes the money, then takes the goods. If there are none and the customer
 * accepted to wait, the order joins the product's queue; otherwise the money goes
 * back. Restocking hands goods to the queue in order of arrival, inside the
 * product's own command, so two orders can never get the same piece.
 */
import { Context, DateTime, Effect, Layer, Ref, Schema } from "effect"
import * as Aggregate from "effect-cqrs/Aggregate"
import * as AggregateService from "effect-cqrs/AggregateService"
import * as Saga from "effect-cqrs/Saga"
import * as ViewModel from "effect-cqrs/ViewModel"
import { Activity, WorkflowEngine } from "effect/workflow"
import { AccountNotOpen, Amount, InsufficientFunds } from "./account.ts"
import { Accounts } from "./bank.ts"

/** The most pieces one order asks for. */
export const maxQuantity = 5

export const Quantity = Schema.Int.check(Schema.isGreaterThan(0))

export const OrderQuantity = Quantity.check(Schema.isLessThanOrEqualTo(maxQuantity))

// ---------------------------------------------------------------------------
// Product
// ---------------------------------------------------------------------------

export const ProductEvent = Schema.TaggedUnion({
  ProductListed: { name: Schema.String, emoji: Schema.String, price: Amount },
  ProductRestocked: { quantity: Quantity },
  StockTaken: { orderId: Schema.String, quantity: Quantity },
  BackorderQueued: { orderId: Schema.String, quantity: Quantity },
  /** Goods that arrived went to an order waiting in the queue. */
  BackorderAllocated: { orderId: Schema.String, quantity: Quantity },
  BackorderCancelled: { orderId: Schema.String, quantity: Quantity }
})
export type ProductEvent = typeof ProductEvent.Type

export class ProductNotFound extends Schema.TaggedError<ProductNotFound>()("ProductNotFound", {
  productId: Schema.String
}, { httpApiStatus: 404 }) {}

export class OutOfStock extends Schema.TaggedError<OutOfStock>()("OutOfStock", {
  productId: Schema.String,
  available: Schema.Int,
  /** Orders already waiting, which come first. */
  queued: Schema.Int
}, { httpApiStatus: 409 }) {}

export class NotInQueue extends Schema.TaggedError<NotInQueue>()("NotInQueue", {
  orderId: Schema.String
}, { httpApiStatus: 409 }) {}

interface Waiting {
  readonly orderId: string
  readonly quantity: number
}

export interface ProductState {
  readonly status: "none" | "listed"
  readonly name: string
  readonly emoji: string
  readonly price: number
  readonly available: number
  /** Orders waiting for goods, oldest first. */
  readonly queue: ReadonlyArray<Waiting>
  /** Orders that took goods or joined the queue, so a saga that asks again changes nothing. */
  readonly handled: ReadonlyArray<string>
}

const initialProduct: ProductState = {
  status: "none",
  name: "",
  emoji: "",
  price: 0,
  available: 0,
  queue: [],
  handled: []
}

// #region allocate
/**
 * Hands the available goods to the queue in order. The first order that doesn't
 * fit stops it: a small order behind it doesn't jump the queue.
 */
const allocate = (available: number, queue: ReadonlyArray<Waiting>): ReadonlyArray<ProductEvent> => {
  const events: Array<ProductEvent> = []
  for (const waiting of queue) {
    if (waiting.quantity > available) break
    available -= waiting.quantity
    events.push(ProductEvent.cases.BackorderAllocated.make(waiting))
  }
  return events
}

// #endregion

const leaveQueue = (queue: ReadonlyArray<Waiting>, orderId: string) =>
  queue.filter((waiting) => waiting.orderId !== orderId)

export const Product = Aggregate.make<ProductState>()({
  name: "product",
  events: ProductEvent,
  initial: initialProduct,
  projection: {
    ProductListed: (state, { emoji, name, price }) => ({ ...state, status: "listed", name, emoji, price }),
    ProductRestocked: (state, { quantity }) => ({ ...state, available: state.available + quantity }),
    StockTaken: (state, { orderId, quantity }) => ({
      ...state,
      available: state.available - quantity,
      handled: [...state.handled, orderId]
    }),
    BackorderQueued: (state, { orderId, quantity }) => ({
      ...state,
      queue: [...state.queue, { orderId, quantity }],
      handled: [...state.handled, orderId]
    }),
    BackorderAllocated: (state, { orderId, quantity }) => ({
      ...state,
      available: state.available - quantity,
      queue: leaveQueue(state.queue, orderId)
    }),
    BackorderCancelled: (state, { orderId }) => ({ ...state, queue: leaveQueue(state.queue, orderId) })
  }
})
  .command("list", {
    payload: Schema.Struct({ name: Schema.NonEmptyString, emoji: Schema.String, price: Amount, stock: Schema.Int }),
    handler: (state, { emoji, name, price, stock }) =>
      Effect.succeed(
        state.status === "listed" ? [] : [
          ProductEvent.cases.ProductListed.make({ name, emoji, price }),
          ...(stock > 0 ? [ProductEvent.cases.ProductRestocked.make({ quantity: stock })] : [])
        ]
      )
  })
  .command("restock", {
    payload: Schema.Struct({ quantity: Quantity }),
    handler: Effect.fnUntraced(function*(state, { quantity }, { aggregateId }) {
      if (state.status !== "listed") return yield* new ProductNotFound({ productId: aggregateId })
      return [
        ProductEvent.cases.ProductRestocked.make({ quantity }),
        ...allocate(state.available + quantity, state.queue)
      ]
    })
  })
  .command("take", {
    payload: Schema.Struct({ orderId: Schema.NonEmptyString, quantity: Quantity }),
    handler: Effect.fnUntraced(function*(state, { orderId, quantity }, { aggregateId }) {
      if (state.handled.includes(orderId)) return []
      if (state.status !== "listed") return yield* new ProductNotFound({ productId: aggregateId })
      // Whoever waits comes first: with a queue, nothing is available to newcomers.
      if (state.queue.length > 0 || state.available < quantity) {
        return yield* new OutOfStock({
          productId: aggregateId,
          available: state.queue.length > 0 ? 0 : state.available,
          queued: state.queue.length
        })
      }
      return ProductEvent.cases.StockTaken.make({ orderId, quantity })
    })
  })
  .command("enqueue", {
    payload: Schema.Struct({ orderId: Schema.NonEmptyString, quantity: Quantity }),
    handler: Effect.fnUntraced(function*(state, { orderId, quantity }, { aggregateId }) {
      if (state.handled.includes(orderId)) return []
      if (state.status !== "listed") return yield* new ProductNotFound({ productId: aggregateId })
      // Goods may have arrived since the order found none: hand them out at once.
      const queue = [...state.queue, { orderId, quantity }]
      return [ProductEvent.cases.BackorderQueued.make({ orderId, quantity }), ...allocate(state.available, queue)]
    })
  })
  .command("dequeue", {
    payload: Schema.Struct({ orderId: Schema.NonEmptyString }),
    handler: Effect.fnUntraced(function*(state, { orderId }) {
      const waiting = state.queue.find((entry) => entry.orderId === orderId)
      if (waiting === undefined) return yield* new NotInQueue({ orderId })
      return ProductEvent.cases.BackorderCancelled.make(waiting)
    })
  })

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

export const OrderStatus = Schema.Literals([
  "none",
  "placed",
  "paid",
  "backordered",
  "fulfilled",
  "cancelled",
  "rejected"
])
export type OrderStatus = typeof OrderStatus.Type

export const OrderEvent = Schema.TaggedUnion({
  OrderPlaced: {
    customer: Schema.String,
    productId: Schema.String,
    quantity: Quantity,
    total: Amount,
    /** Whether the customer waits when the product is out of stock. */
    backorder: Schema.Boolean,
    placedAt: Schema.DateTimeUtc
  },
  OrderPaid: {},
  OrderBackordered: {},
  OrderFulfilled: { fulfilledAt: Schema.DateTimeUtc },
  /** The customer gave up waiting. It carries what the refund needs. */
  OrderCancelled: { customer: Schema.String, total: Amount },
  OrderRejected: { reason: Schema.String }
})
export type OrderEvent = typeof OrderEvent.Type

export class OrderAlreadyPlaced extends Schema.TaggedError<OrderAlreadyPlaced>()("OrderAlreadyPlaced", {
  orderId: Schema.String
}, { httpApiStatus: 409 }) {}

export class OrderNotCancellable extends Schema.TaggedError<OrderNotCancellable>()("OrderNotCancellable", {
  orderId: Schema.String,
  status: Schema.String
}, { httpApiStatus: 409 }) {}

export interface OrderState {
  readonly status: OrderStatus
  readonly customer: string
  readonly productId: string
  readonly quantity: number
  readonly total: number
}

export const Order = Aggregate.make<OrderState>()({
  name: "order",
  events: OrderEvent,
  initial: { status: "none", customer: "", productId: "", quantity: 0, total: 0 },
  projection: {
    OrderPlaced: (_, { customer, productId, quantity, total }) => ({
      status: "placed",
      customer,
      productId,
      quantity,
      total
    }),
    OrderPaid: (state) => ({ ...state, status: "paid" }),
    OrderBackordered: (state) => ({ ...state, status: "backordered" }),
    OrderFulfilled: (state) => ({ ...state, status: "fulfilled" }),
    OrderCancelled: (state) => ({ ...state, status: "cancelled" }),
    OrderRejected: (state) => ({ ...state, status: "rejected" })
  }
})
  .command("place", {
    payload: Schema.Struct({
      customer: Schema.NonEmptyString,
      productId: Schema.NonEmptyString,
      quantity: Quantity,
      total: Amount,
      backorder: Schema.Boolean
    }),
    handler: Effect.fnUntraced(function*(state, payload, { aggregateId }) {
      if (state.status !== "none") return yield* new OrderAlreadyPlaced({ orderId: aggregateId })
      return OrderEvent.cases.OrderPlaced.make({ ...payload, placedAt: yield* DateTime.now })
    })
  })
  // The saga's steps: each does nothing when the order is past it, so it can run again.
  .command("markPaid", {
    payload: Schema.Struct({}),
    handler: (state) => Effect.succeed(state.status === "placed" ? OrderEvent.cases.OrderPaid.make({}) : [])
  })
  .command("markBackordered", {
    payload: Schema.Struct({}),
    handler: (state) => Effect.succeed(state.status === "paid" ? OrderEvent.cases.OrderBackordered.make({}) : [])
  })
  .command("fulfil", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state) {
      if (state.status !== "paid" && state.status !== "backordered") return []
      return OrderEvent.cases.OrderFulfilled.make({ fulfilledAt: yield* DateTime.now })
    })
  })
  .command("reject", {
    payload: Schema.Struct({ reason: Schema.String }),
    handler: (state, { reason }) =>
      Effect.succeed(
        state.status === "placed" || state.status === "paid" ? OrderEvent.cases.OrderRejected.make({ reason }) : []
      )
  })
  .command("cancel", {
    payload: Schema.Struct({}),
    handler: Effect.fnUntraced(function*(state, _, { aggregateId }) {
      if (state.status === "cancelled") return []
      if (state.status !== "backordered") {
        return yield* new OrderNotCancellable({ orderId: aggregateId, status: state.status })
      }
      return OrderEvent.cases.OrderCancelled.make({ customer: state.customer, total: state.total })
    })
  })

export class Products extends Context.Service<Products, AggregateService.AggregateService<typeof Product>>()(
  "workshop/Products"
) {}

export class Orders extends Context.Service<Orders, AggregateService.AggregateService<typeof Order>>()(
  "workshop/Orders"
) {}

// ---------------------------------------------------------------------------
// Sagas
// ---------------------------------------------------------------------------

export const reasons = {
  insufficientFunds: "saldo insufficiente",
  accountClosed: "conto chiuso",
  outOfStock: "esaurito",
  cancelled: "annullato dal cliente",
  warehouse: "guasto in magazzino (simulato)"
} as const

/**
 * The presenter's way to break things on purpose: the next orders fail after
 * they are paid, so the room sees the saga give the money back.
 */
export class Chaos extends Context.Service<Chaos, {
  /** Breaks one more order, and says how many will break. */
  readonly breakNext: Effect.Effect<number>
  /** Whether this order breaks, using up one if so. */
  readonly consume: Effect.Effect<boolean>
  readonly pending: Effect.Effect<number>
}>()("workshop/Chaos") {
  static readonly layer = Layer.effect(
    Chaos,
    Effect.map(Ref.make(0), (broken) =>
      Chaos.of({
        breakNext: Ref.updateAndGet(broken, (n) => n + 1),
        consume: Ref.modify(broken, (n) => n > 0 ? [true, n - 1] : [false, 0]),
        pending: Ref.get(broken)
      }))
  )
}

/** Takes an order from placed to fulfilled, backordered or rejected with its money back. */
export const Checkout = Saga.workflow("Checkout", {
  payload: {
    orderId: Schema.String,
    customer: Schema.String,
    productId: Schema.String,
    quantity: Schema.Int,
    total: Schema.Int,
    backorder: Schema.Boolean
  },
  idempotencyKey: ({ orderId }) => orderId
})

// #region checkout
const CheckoutWorkflow = Saga.toLayer(
  Checkout,
  ({ backorder, customer, orderId, productId, quantity, total }) =>
    Effect.gen(function*() {
      const accounts = yield* Accounts
      const products = yield* Products
      const orders = yield* Orders
      const chaos = yield* Chaos

      const reject = (reason: string) =>
        Saga.command("reject", { execute: orders.commands.reject(orderId, { reason }) })
      const refund = (reason: string) =>
        Saga.command("refund", { execute: accounts.commands.refundOrder(customer, { orderId, amount: total, reason }) })

      const paid = yield* Saga.command("pay", {
        error: Schema.Union([InsufficientFunds, AccountNotOpen]),
        execute: accounts.commands.payOrder(customer, { orderId, amount: total })
      }).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          reject(error._tag === "InsufficientFunds" ? reasons.insufficientFunds : reasons.accountClosed).pipe(
            Effect.as(false)
          )
        )
      )
      if (!paid) return
      yield* Saga.command("mark-paid", { execute: orders.commands.markPaid(orderId, {}) })

      // An activity, so a saga that runs again after a restart decides the same way.
      const broken = yield* Activity.make({ name: "warehouse", success: Schema.Boolean, execute: chaos.consume })
      if (broken) {
        yield* refund(reasons.warehouse)
        return yield* reject(reasons.warehouse)
      }

      const taken = yield* Saga.command("take", {
        error: OutOfStock,
        execute: products.commands.take(productId, { orderId, quantity })
      }).pipe(Effect.as(true), Effect.catchTag("OutOfStock", () => Effect.succeed(false)))
      if (taken) {
        yield* Saga.command("fulfil", { execute: orders.commands.fulfil(orderId, {}) })
      } else if (backorder) {
        yield* Saga.command("enqueue", { execute: products.commands.enqueue(productId, { orderId, quantity }) })
        // If goods were there already, the order is fulfilled by now and this does nothing.
        yield* Saga.command("mark-backordered", { execute: orders.commands.markBackordered(orderId, {}) })
      } else {
        yield* refund(reasons.outOfStock)
        yield* reject(reasons.outOfStock)
      }
    })
)

// #endregion

/** Delivers an order from the queue when goods arrive for it. */
export const Fulfil = Saga.workflow("Fulfil", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId
})

const FulfilWorkflow = Saga.toLayer(Fulfil, ({ orderId }) =>
  Effect.gen(function*() {
    const orders = yield* Orders
    yield* Saga.command("fulfil", { execute: orders.commands.fulfil(orderId, {}) })
  }))

/** Gives the money back for an order the customer cancelled. */
export const RefundCancelled = Saga.workflow("RefundCancelled", {
  payload: { orderId: Schema.String, customer: Schema.String, total: Schema.Int },
  idempotencyKey: ({ orderId }) => orderId
})

const RefundCancelledWorkflow = Saga.toLayer(RefundCancelled, ({ customer, orderId, total }) =>
  Effect.gen(function*() {
    const accounts = yield* Accounts
    yield* Saga.command("refund", {
      execute: accounts.commands.refundOrder(customer, { orderId, amount: total, reason: reasons.cancelled })
    })
  }))

export const OrderSagas = Saga.make({
  // Not "orders": a saga is a view model, and that name is the orders view model's checkpoint.
  name: "order-sagas",
  aggregates: [Order],
  handlers: {
    OrderPlaced: ({ backorder, customer, productId, quantity, total }, { aggregateId }) =>
      Saga.start(Checkout, { orderId: aggregateId, customer, productId, quantity, total, backorder }),
    OrderCancelled: ({ customer, total }, { aggregateId }) =>
      Saga.start(RefundCancelled, { orderId: aggregateId, customer, total })
  }
})

export const ProductSagas = Saga.make({
  name: "backorders",
  aggregates: [Product],
  handlers: {
    BackorderAllocated: ({ orderId }) => Saga.start(Fulfil, { orderId })
  }
})

export const ShopSagas = Saga.layer([OrderSagas, ProductSagas]).pipe(
  Layer.provideMerge(Layer.mergeAll(CheckoutWorkflow, FulfilWorkflow, RefundCancelledWorkflow)),
  Layer.provide(WorkflowEngine.layerMemory)
)

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

export const CatalogProduct = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  emoji: Schema.String,
  price: Schema.Number,
  available: Schema.Number,
  /** The orders waiting, oldest first. */
  queue: Schema.Array(Schema.Struct({ orderId: Schema.String, quantity: Schema.Number })),
  sold: Schema.Number
})
export type CatalogProduct = typeof CatalogProduct.Type

export class CatalogStore extends Context.Service<CatalogStore, Ref.Ref<ReadonlyMap<string, CatalogProduct>>>()(
  "workshop/CatalogStore"
) {
  static readonly layer = Layer.effect(CatalogStore, Ref.make<ReadonlyMap<string, CatalogProduct>>(new Map()))
}

const updateProduct = (id: string, f: (product: CatalogProduct) => CatalogProduct) =>
  CatalogStore.use((store) =>
    Ref.update(store, (products) => {
      const current = products.get(id)
      return current === undefined ? products : new Map(products).set(id, f(current))
    })
  )

export const CatalogView = ViewModel.make({
  name: "catalog",
  aggregates: [Product],
  dispose: CatalogStore.use((store) => Ref.set(store, new Map())),
  handlers: {
    ProductListed: ({ emoji, name, price }, { aggregateId }) =>
      CatalogStore.use((store) =>
        Ref.update(
          store,
          (products) =>
            new Map(products).set(aggregateId, {
              id: aggregateId,
              name,
              emoji,
              price,
              available: 0,
              queue: [],
              sold: 0
            })
        )
      ),
    ProductRestocked: ({ quantity }, { aggregateId }) =>
      updateProduct(aggregateId, (product) => ({ ...product, available: product.available + quantity })),
    StockTaken: ({ quantity }, { aggregateId }) =>
      updateProduct(aggregateId, (product) => ({
        ...product,
        available: product.available - quantity,
        sold: product.sold + quantity
      })),
    BackorderQueued: ({ orderId, quantity }, { aggregateId }) =>
      updateProduct(aggregateId, (product) => ({ ...product, queue: [...product.queue, { orderId, quantity }] })),
    BackorderAllocated: ({ orderId, quantity }, { aggregateId }) =>
      updateProduct(aggregateId, (product) => ({
        ...product,
        available: product.available - quantity,
        sold: product.sold + quantity,
        queue: product.queue.filter((waiting) => waiting.orderId !== orderId)
      })),
    BackorderCancelled: ({ orderId }, { aggregateId }) =>
      updateProduct(aggregateId, (product) => ({
        ...product,
        queue: product.queue.filter((waiting) => waiting.orderId !== orderId)
      }))
  }
})

export const listCatalog = CatalogStore.use((store) =>
  Effect.map(Ref.get(store), (products) => Array.from(products.values()).sort((a, b) => a.price - b.price))
)

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

export const ShopOrder = Schema.Struct({
  id: Schema.String,
  customer: Schema.String,
  productId: Schema.String,
  quantity: Schema.Number,
  total: Schema.Number,
  backorder: Schema.Boolean,
  status: OrderStatus,
  reason: Schema.optionalKey(Schema.String),
  placedAt: Schema.DateTimeUtcFromString,
  updatedAt: Schema.DateTimeUtcFromString
})
export type ShopOrder = typeof ShopOrder.Type

export class OrdersStore extends Context.Service<OrdersStore, Ref.Ref<ReadonlyMap<string, ShopOrder>>>()(
  "workshop/OrdersStore"
) {
  static readonly layer = Layer.effect(OrdersStore, Ref.make<ReadonlyMap<string, ShopOrder>>(new Map()))
}

const setStatus =
  (status: OrderStatus, reason?: string) => (_: unknown, { aggregateId, timestamp }: Aggregate.EventMeta) =>
    OrdersStore.use((store) =>
      Ref.update(store, (orders) => {
        const current = orders.get(aggregateId)
        if (current === undefined) return orders
        return new Map(orders).set(aggregateId, {
          ...current,
          status,
          updatedAt: timestamp,
          ...(reason === undefined ? {} : { reason })
        })
      })
    )

export const OrdersView = ViewModel.make({
  name: "orders",
  aggregates: [Order],
  dispose: OrdersStore.use((store) => Ref.set(store, new Map())),
  handlers: {
    OrderPlaced: ({ backorder, customer, placedAt, productId, quantity, total }, { aggregateId }) =>
      OrdersStore.use((store) =>
        Ref.update(store, (orders) =>
          new Map(orders).set(aggregateId, {
            id: aggregateId,
            customer,
            productId,
            quantity,
            total,
            backorder,
            status: "placed",
            placedAt,
            updatedAt: placedAt
          }))
      ),
    OrderPaid: setStatus("paid"),
    OrderBackordered: setStatus("backordered"),
    OrderFulfilled: setStatus("fulfilled"),
    OrderCancelled: setStatus("cancelled"),
    OrderRejected: ({ reason }, meta) => setStatus("rejected", reason)(undefined, meta)
  }
})

/** The orders of one customer, newest first. */
export const ordersOf = (customer: string) =>
  OrdersStore.use((store) =>
    Effect.map(Ref.get(store), (orders) =>
      Array.from(orders.values())
        .filter((order) => order.customer === customer)
        .sort((a, b) => DateTime.toEpochMillis(b.placedAt) - DateTime.toEpochMillis(a.placedAt)))
  )

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

/** What the shop sells when it opens. Listing is idempotent, so this runs on every start. */
export const initialCatalog = [
  { id: "caffe", name: "Caffè", emoji: "☕", price: 3, stock: 10 },
  { id: "cornetto", name: "Cornetto", emoji: "🥐", price: 2, stock: 8 },
  { id: "succo", name: "Succo", emoji: "🧃", price: 4, stock: 5 },
  { id: "cioccolato", name: "Cioccolato", emoji: "🍫", price: 5, stock: 3 },
  { id: "quaderno", name: "Quaderno", emoji: "📓", price: 12, stock: 2 },
  { id: "cappellino", name: "Cappellino", emoji: "🧢", price: 25, stock: 1 },
  { id: "cuffie", name: "Cuffie", emoji: "🎧", price: 60, stock: 0 }
] as const

export const seedCatalog = Products.use((products) =>
  Effect.forEach(
    initialCatalog,
    ({ id, ...product }) => products.commands.list(id, product),
    { discard: true }
  )
)

/** The shop's view models. `domain/workshop.ts` runs them with the bank's. */
export const shopViews = [CatalogView, OrdersView] as const

export const ShopStores = Layer.mergeAll(CatalogStore.layer, OrdersStore.layer)

export const ShopServices = Layer.mergeAll(
  Layer.effect(Products, AggregateService.make(Product)),
  Layer.effect(Orders, AggregateService.make(Order))
)
