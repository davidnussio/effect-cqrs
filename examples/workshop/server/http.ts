/**
 * The handlers of `api.ts`. Every command runs as the caller, on the caller's
 * account: the account id is the user id in the token, never one from the request.
 */
import { Clock, Config, Context, DateTime, Effect, Layer, Option, Predicate, Redacted } from "effect"
import { EventStore, Ids, Metadata, PersonalData, ViewModel } from "effect-cqrs"
import { HttpApiBuilder } from "effect/http-api"
import {
  Api,
  Authorization,
  CurrentUser,
  Forbidden,
  type Me,
  OrderNotFound,
  RecipientNotFound,
  Unauthorized,
  type User,
  WrongAdminCode
} from "../api.ts"
import { Bot, Simulator } from "../domain/automation.ts"
import { Accounts, ledgerOf, LedgerStore, listParticipants, ParticipantsStore } from "../domain/bank.ts"
import {
  CatalogStore,
  listCatalog,
  OrderNotCancellable,
  Orders,
  ordersOf,
  OrdersStore,
  ProductNotFound,
  Products,
  reasons
} from "../domain/shop.ts"
import { type InvalidToken, Tokens } from "./auth.ts"

export class StoreKind extends Context.Service<StoreKind, "postgres" | "sqlite">()("workshop/StoreKind") {}

/**
 * Keeps the errors whose tags are listed and turns every other failure into a
 * defect, answered with `500`: a store that can't be reached is the server's
 * problem, not the caller's.
 */
const expecting = <const Tags extends ReadonlyArray<string>>(...tags: Tags) =>
<A, E, R>(
  self: Effect.Effect<A, E, R>
): Effect.Effect<A, Extract<E, { readonly _tag: Tags[number] }>, R> =>
  Effect.catch(self, (error) =>
    Predicate.hasProperty(error, "_tag") && tags.includes(error._tag as string)
      ? Effect.fail(error as Extract<E, { readonly _tag: Tags[number] }>)
      : Effect.die(error))

/** Waits a little for the view models to reach `position`, so a client reads its own writes. */
const makeCatchUp = ViewModel.ViewModels.use((viewModels) =>
  Effect.succeed((position: number | undefined) =>
    position === undefined
      ? Effect.void
      : viewModels.caughtUp(position).pipe(Effect.timeout("5 seconds"), Effect.ignore)
  )
)

const tokenProblems: Record<InvalidToken["reason"], string> = {
  malformed: "The token is not one this server issued",
  signature: "The token's signature doesn't match",
  expired: "The token has expired: join again"
}

export const AuthorizationLayer = Layer.effect(
  Authorization,
  Effect.gen(function*() {
    const tokens = yield* Tokens
    const accounts = yield* Accounts
    return Authorization.of({
      bearer: (httpEffect, { credential }) =>
        Effect.gen(function*() {
          const user = yield* tokens.verify(Redacted.value(credential)).pipe(
            Effect.mapError(({ reason }) => new Unauthorized({ message: tokenProblems[reason] }))
          )
          const account = yield* Effect.orDie(accounts.readModel(user.id))
          if (account.status !== "open") return yield* new Unauthorized({ message: "The account is closed" })
          return yield* Effect.provideService(httpEffect, CurrentUser, user)
        })
    })
  })
)

const SystemHandlers = HttpApiBuilder.group(Api, "system", (handlers) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const kind = yield* StoreKind
    const commit = yield* Config.option(Config.String("RENDER_GIT_COMMIT"))
    const startedAt = yield* DateTime.now
    return handlers.handle("health", () =>
      Effect.gen(function*() {
        // Read from the store, so that a database that can't be reached shows up here.
        const position = yield* Effect.orDie(store.head)
        const now = yield* Clock.currentTimeMillis
        return {
          status: "ok" as const,
          store: kind,
          position,
          ...(Option.isSome(commit) ? { commit: commit.value } : {}),
          startedAt,
          uptimeSeconds: Math.round((now - DateTime.toEpochMillis(startedAt)) / 1000)
        }
      }))
  }))

const AuthHandlers = HttpApiBuilder.group(Api, "auth", (handlers) =>
  Effect.gen(function*() {
    const tokens = yield* Tokens
    const accounts = yield* Accounts
    return handlers.handle("join", ({ payload }) =>
      Effect.gen(function*() {
        if (payload.adminCode !== undefined && !tokens.isAdminCode(payload.adminCode)) {
          return yield* new WrongAdminCode()
        }
        const user: User = {
          id: yield* Ids.next,
          nickname: payload.nickname,
          role: payload.adminCode === undefined ? "participant" : "admin"
        }
        yield* Effect.orDie(accounts.commands.openAccount(user.id, { nickname: user.nickname }))
        return { token: yield* tokens.issue(user), user }
      }))
  }))

// Each group takes the services it uses when it is built, so its handlers need
// nothing from the request but the caller.
const MeHandlers = HttpApiBuilder.group(Api, "me", (handlers) =>
  Effect.gen(function*() {
    const accounts = yield* Accounts
    const products = yield* Products
    const orders = yield* Orders
    const catchUp = yield* makeCatchUp
    const ledgers = yield* LedgerStore
    const orderList = yield* OrdersStore
    const forgetting = yield* Effect.context<PersonalData.Keys | EventStore.EventStore>()

    const balanceOf = (id: string) => Effect.map(Effect.orDie(accounts.readModel(id)), (state) => state.balance)

    /**
     * Takes an order out of its product's queue and cancels it. Fails with
     * `OrderNotCancellable` when the goods reached it first.
     */
    const cancel = Effect.fnUntraced(function*(customer: string, orderId: string) {
      const order = yield* Effect.orDie(orders.readModel(orderId))
      if (order.status === "none" || order.customer !== customer) return yield* new OrderNotFound({ orderId })
      if (order.status !== "backordered") {
        return yield* new OrderNotCancellable({ orderId, status: order.status })
      }
      yield* products.commands.dequeue(order.productId, { orderId }).pipe(
        expecting("NotInQueue"),
        Effect.catchTag("NotInQueue", () => Effect.fail(new OrderNotCancellable({ orderId, status: "fulfilled" })))
      )
      const result = yield* orders.commands.cancel(orderId, {}).pipe(expecting("OrderNotCancellable"))
      return { result, total: order.total }
    })

    return handlers
      .handle("get", () =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          const state = yield* Effect.orDie(accounts.readModel(user.id))
          return {
            id: user.id,
            nickname: state.nickname,
            role: user.role,
            status: state.status,
            balance: state.balance
          } satisfies Me
        }))
      .handle("deposit", ({ payload }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          const result = yield* accounts.commands.deposit(user.id, payload).pipe(expecting("AccountNotOpen"))
          return { position: result.position, balance: yield* balanceOf(user.id) }
        }))
      .handle("transfer", ({ payload: { amount, to } }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          const recipient = yield* Effect.orDie(accounts.readModel(to))
          if (recipient.status !== "open" && to !== user.id) {
            return yield* new RecipientNotFound({ accountId: to })
          }
          const transferId = yield* Ids.next
          const result = yield* accounts.commands.sendTransfer(user.id, { transferId, to, amount }).pipe(
            expecting("AccountNotOpen", "InsufficientFunds", "SameAccountTransfer")
          )
          return { position: result.position, balance: yield* balanceOf(user.id), transferId }
        }))
      .handle("ledger", ({ query }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          yield* catchUp(query.position)
          return yield* ledgerOf(user.id).pipe(Effect.provideService(LedgerStore, ledgers))
        }))
      .handle("orders", ({ query }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          yield* catchUp(query.position)
          return yield* ordersOf(user.id).pipe(Effect.provideService(OrdersStore, orderList))
        }))
      .handle("cancelOrder", ({ params: { orderId } }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          const { result } = yield* cancel(user.id, orderId)
          // The refund follows in a saga, which `position` doesn't wait for.
          return { position: result.position }
        }))
      .handle("close", () =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          // Orders still waiting give their money back first, so the payout includes it.
          // The refund saga would pay it later, on a closed account: refunding here
          // makes that a no-op.
          const waiting = yield* ordersOf(user.id).pipe(
            Effect.provideService(OrdersStore, orderList),
            Effect.map((list) => list.filter((order) => order.status === "backordered"))
          )
          yield* Effect.forEach(waiting, (order) =>
            cancel(user.id, order.id).pipe(
              Effect.andThen(
                accounts.commands.refundOrder(user.id, {
                  orderId: order.id,
                  amount: order.total,
                  reason: reasons.cancelled
                })
              ),
              // Delivered in the meantime: nothing to give back.
              Effect.catchTag(["OrderNotCancellable", "OrderNotFound"], () => Effect.void),
              Effect.orDie
            ), { discard: true })
          const result = yield* accounts.commands.closeAccount(user.id, {}).pipe(expecting("AccountNotOpen"))
          // Destroys the key of the nickname: the events stay, and read `[forgotten]`.
          yield* PersonalData.forget(user.id).pipe(Effect.provideContext(forgetting), Effect.orDie)
          return { position: result.position, balance: 0 }
        }))
  }))

const BankHandlers = HttpApiBuilder.group(Api, "bank", (handlers) =>
  Effect.gen(function*() {
    const catchUp = yield* makeCatchUp
    const participants = yield* ParticipantsStore
    return handlers.handle("participants", ({ query }) =>
      Effect.andThen(
        catchUp(query.position),
        listParticipants.pipe(Effect.provideService(ParticipantsStore, participants))
      ))
  }))

const ShopHandlers = HttpApiBuilder.group(Api, "shop", (handlers) =>
  Effect.gen(function*() {
    const products = yield* Products
    const orders = yield* Orders
    const catchUp = yield* makeCatchUp
    const catalog = yield* CatalogStore
    const bot = yield* Bot
    return handlers
      .handle("bot", () => bot.status)
      .handle("products", ({ query }) =>
        Effect.andThen(catchUp(query.position), listCatalog.pipe(Effect.provideService(CatalogStore, catalog))))
      .handle("order", ({ payload: { backorder, productId, quantity } }) =>
        Effect.gen(function*() {
          const user = yield* CurrentUser
          const product = yield* Effect.orDie(products.readModel(productId))
          if (product.status !== "listed") {
            return yield* new ProductNotFound({ productId })
          }
          const orderId = yield* Ids.next
          // The order is only placed here: paying and taking the goods is the saga's job.
          const result = yield* Effect.orDie(orders.commands.place(orderId, {
            customer: user.id,
            productId,
            quantity,
            total: product.price * quantity,
            backorder
          }))
          return { position: result.position, orderId }
        }))
  }))

/** Lets only the presenter through. */
const asAdmin = <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E | Forbidden, R | CurrentUser> =>
  Effect.gen(function*() {
    const user = yield* CurrentUser
    if (user.role !== "admin") return yield* new Forbidden({ message: "Only the presenter can do this" })
    return yield* self
  })

const AdminHandlers = HttpApiBuilder.group(Api, "admin", (handlers) =>
  Effect.gen(function*() {
    const products = yield* Products
    const bot = yield* Bot
    const simulator = yield* Simulator
    return handlers
      .handle("restock", ({ params: { productId }, payload: { quantity } }) =>
        asAdmin(
          products.commands.restock(productId, { quantity }).pipe(
            Metadata.withMetadata({ actor: "presenter" }),
            expecting("ProductNotFound"),
            Effect.map((result) => ({ position: result.position }))
          )
        ))
      .handle("configureBot", ({ payload }) => asAdmin(bot.configure(payload)))
      .handle("runBot", () => asAdmin(bot.runNow))
      .handle("simulator", () => asAdmin(simulator.status))
      .handle("configureSimulator", ({ payload }) => asAdmin(simulator.configure(payload)))
  }))

/** Every group of the API, needing the bank's and the shop's services and the authorization middleware. */
export const Handlers = Layer.mergeAll(
  SystemHandlers,
  AuthHandlers,
  MeHandlers,
  BankHandlers,
  ShopHandlers,
  AdminHandlers
).pipe(
  Layer.provideMerge(AuthorizationLayer)
)
